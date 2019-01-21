//! Curl agent that executes multiple requests simultaneously.

use crate::error::Error;
use crate::internal::request::*;
use crossbeam_channel::{self, Sender, Receiver};
use curl::multi;
use log::*;
use mio::*;
use slab::Slab;
use std::sync::{Arc, Weak};
use std::sync::atomic::*;
use std::thread;
use std::time::{Duration, Instant};

const AGENT_THREAD_NAME: &'static str = "curl agent";
const DEFAULT_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_TIMEOUT: Duration = Duration::from_millis(1000);
const WAKER_TOKEN: Token = Token(usize::max_value() - 1);

/// Handle to an agent. Handles can be sent between threads, shared, and cloned.
#[derive(Clone, Debug)]
pub struct Handle {
    inner: Arc<HandleInner>,
}

/// Create an agent that executes multiple curl requests simultaneously.
///
/// The agent maintains a background thread that multiplexes all active requests using a single "multi" handle.
pub fn create() -> Result<Handle, Error> {
    let create_start = Instant::now();

    let (message_tx, message_rx) = crossbeam_channel::unbounded();
    let (notify_rx, waker) = mio::Registration::new2();

    let handle_inner = Arc::new(HandleInner {
        message_tx,
        waker,
        thread_terminated: AtomicBool::default(),
    });

    let handle_weak = Arc::downgrade(&handle_inner);

    thread::Builder::new().name(String::from(AGENT_THREAD_NAME)).spawn(move || {
        let agent = Agent::new(message_rx, notify_rx, handle_weak).unwrap();

        debug!("agent took {:?} to start up", create_start.elapsed());

        // Intentionally panic the thread if an error occurs.
        agent.run().unwrap();
    })?;

    Ok(Handle {
        inner: handle_inner,
    })
}

/// Actual handle to an agent. Only one of these exists per agent.
#[derive(Debug)]
struct HandleInner {
    /// Used to send messages to the agent.
    message_tx: Sender<Message>,

    /// Used to wake up the agent thread while it is polling.
    waker: SetReadiness,

    /// Indicates that the agent thread has exited.
    thread_terminated: AtomicBool,
}

impl Handle {
    /// Begin executing a request with this agent.
    pub fn begin_execute(&self, request: CurlRequest) -> Result<(), Error> {
        request.0.get_ref().set_agent(self.clone());

        self.inner.send_message(Message::BeginRequest(request))
    }

    /// Cancel a request by its token.
    pub fn cancel_request(&self, token: usize) -> Result<(), Error> {
        self.inner.send_message(Message::Cancel(token))
    }

    /// Unpause a request by its token.
    pub fn unpause_write(&self, token: usize) -> Result<(), Error> {
        self.inner.send_message(Message::UnpauseWrite(token))
    }
}

impl HandleInner {
    /// Send a message to the associated agent.
    ///
    /// If the agent is not connected, an error is returned.
    fn send_message(&self, message: Message) -> Result<(), Error> {
        if self.thread_terminated.load(Ordering::SeqCst) {
            error!("agent thread terminated prematurely");
            return Err(Error::Internal);
        }

        self.message_tx.send(message).map_err(|_| Error::Internal)?;
        self.waker.set_readiness(mio::Ready::readable())?;

        Ok(())
    }
}

impl Drop for HandleInner {
    fn drop(&mut self) {
        if self.send_message(Message::Close).is_err() {
            warn!("agent thread was already terminated");
        }
    }
}

/// A message sent from the main thread to the agent thread.
#[derive(Debug)]
enum Message {
    Cancel(usize),
    Close,
    BeginRequest(CurlRequest),
    UnpauseWrite(usize),
}

/// Internal state of the agent thread.
struct Agent {
    /// A curl multi handle, of course.
    multi: multi::Multi,

    /// A poll
    poll: Poll,

    /// Incoming message from the main thread.
    message_rx: Receiver<Message>,

    /// A queue of requests from curl to register or unregister sockets.
    socket_changes: Receiver<(multi::Socket, multi::SocketEvents, usize)>,

    /// The evented handle that the agent handle uses to wake up the agent loop.
    /// We don't really use it for much, we just carry it along so it doesn't
    /// get dropped until shutdown.
    waker: Registration,

    /// Contains all of the active requests.
    requests: Slab<multi::Easy2Handle<CurlHandler>>,

    /// Indicates if the thread has been requested to stop.
    close_requested: bool,

    /// Weak reference to a handle, used to communicate back to handles.
    handle: Weak<HandleInner>,
}

impl Agent {
    fn new(message_rx: Receiver<Message>, waker: Registration, handle: Weak<HandleInner>) -> Result<Self, Error> {
        let poll = Poll::new()?;
        poll.register(&waker, WAKER_TOKEN, Ready::all(), PollOpt::edge())?;

        let mut multi = multi::Multi::new();
        let (socket_tx, socket_rx) = crossbeam_channel::unbounded();

        multi.socket_function(move |socket, events, token| {
            debug_assert!(socket_tx.send((socket, events, token)).is_ok());
        })?;


        Ok(Self {
            multi,
            poll,
            message_rx,
            socket_changes: socket_rx,
            waker,
            requests: Slab::new(),
            close_requested: false,
            handle: handle,
        })
    }

    /// Run the agent in the current thread until requested to stop.
    fn run(mut self) -> Result<(), Error> {
        let mut events = Events::with_capacity(1024);
        let mut sockets = Slab::new();

        debug!("agent ready");

        // Agent main loop.
        loop {
            self.poll_messages()?;

            // Handle any socket changes from curl.
            for (socket, events, mut token) in self.socket_changes.try_iter() {
                // Brand new socket.
                if token == 0 {
                    token = sockets.insert(socket);
                    self.multi.assign(socket, token)?;

                    let readiness = match (events.input(), events.output()) {
                        (true, true) => Ready::all(),
                        (true, false) => Ready::readable(),
                        (false, true) => Ready::writable(),
                        (false, false) => Ready::empty(),
                    };

                    self.poll.register(&mio::unix::EventedFd(&socket), Token(token), readiness, PollOpt::level())?;
                }

                else if events.remove() {
                    self.poll.deregister(&mio::unix::EventedFd(&socket))?;
                    sockets.remove(token);
                }

                else {
                    let readiness = match (events.input(), events.output()) {
                        (true, true) => Ready::all(),
                        (true, false) => Ready::readable(),
                        (false, true) => Ready::writable(),
                        (false, false) => Ready::empty(),
                    };

                    self.poll.reregister(&mio::unix::EventedFd(&socket), Token(token), readiness, PollOpt::level())?;
                }
            }

            // Perform any pending reads or writes and handle any state changes.
            self.dispatch()?;

            if self.close_requested {
                break;
            }

            // Determine the blocking timeout value. If curl returns None, then it is unsure as to what timeout value is
            // appropriate. In this case we use a default value.
            let mut timeout = self.multi.get_timeout()?.unwrap_or(DEFAULT_TIMEOUT);

            // HACK: A mysterious bug in recent versions of curl causes it to return the value of
            // `CURLOPT_CONNECTTIMEOUT_MS` a few times during the DNS resolve phase. Work around this issue by
            // truncating this known value to 1ms to avoid blocking the agent loop for a long time.
            // See https://github.com/curl/curl/issues/2996 and https://github.com/alexcrichton/curl-rust/issues/227.
            if timeout == Duration::from_secs(300) {
                debug!("HACK: curl returned CONNECTTIMEOUT of {:?}, truncating to 1ms!", timeout);
                timeout = Duration::from_millis(1);
            }

            // Truncate the timeout to the max value.
            timeout = timeout.min(MAX_TIMEOUT);

            // Block until activity is detected or the timeout passes.
            trace!("polling with timeout of {:?}", timeout);

            if self.poll.poll(&mut events, Some(timeout))? == 0 {
                // Reached timeout, let curl know.
                self.multi.timeout()?;
            } else {
                // Handle any readiness events.
                for event in &events {
                    // Skip spurious events.
                    if event.readiness().is_empty() {
                        if event.token() == WAKER_TOKEN {
                            debug!("woke up by agent handle");
                        } else {
                            let socket = sockets[event.token().0];

                            self.multi.action(socket, multi::Events::new()
                                .input(event.readiness().is_readable())
                                .output(event.readiness().is_writable()))?;
                        }
                    }
                }
            }
        }

        debug!("agent shutting down");

        self.requests.clear();
        self.multi.close()?;

        Ok(())
    }

    /// Polls the message channel for new messages from any agent handles.
    ///
    /// If there are no active requests right now, this function will block until a message is received.
    fn poll_messages(&mut self) -> Result<(), Error> {
        loop {
            if !self.close_requested && self.requests.is_empty() {
                match self.message_rx.recv() {
                    Ok(message) => self.handle_message(message)?,
                    _ => {
                        warn!("agent handle disconnected without close message");
                        self.close_requested = true;
                        break;
                    },
                }
            } else {
                match self.message_rx.try_recv() {
                    Ok(message) => self.handle_message(message)?,
                    Err(crossbeam_channel::TryRecvError::Empty) => break,
                    Err(crossbeam_channel::TryRecvError::Disconnected) => {
                        warn!("agent handle disconnected without close message");
                        self.close_requested = true;
                        break;
                    },
                }
            }
        }

        Ok(())
    }

    fn handle_message(&mut self, message: Message) -> Result<(), Error> {
        trace!("received message from agent handle: {:?}", message);

        match message {
            Message::Close => {
                trace!("agent close requested");
                self.close_requested = true;
            },
            Message::BeginRequest(request) => {
                let mut handle = self.multi.add2(request.0)?;
                let entry = self.requests.vacant_entry();

                handle.get_ref().set_token(entry.key());
                handle.set_token(entry.key())?;

                entry.insert(handle);
            },
            Message::Cancel(token) => {
                if self.requests.contains(token) {
                    let request = self.requests.remove(token);
                    let request = self.multi.remove2(request)?;
                    drop(request);
                }
            },
            Message::UnpauseWrite(token) => {
                if let Some(request) = self.requests.get(token) {
                    request.unpause_write()?;
                } else {
                    warn!("received unpause request for unknown request token: {}", token);
                }
            },
        }

        Ok(())
    }

    fn dispatch(&mut self) -> Result<(), Error> {
        self.multi.perform()?;

        let mut messages = Vec::new();
        self.multi.messages(|message| {
            if let Some(result) = message.result() {
                if let Ok(token) = message.token() {
                    messages.push((token, result));
                }
            }
        });

        for (token, result) in messages {
            match result {
                Ok(()) => self.complete_request(token)?,
                Err(e) => {
                    debug!("curl error: {}", e);
                    self.fail_request(token, e.into())?;
                },
            };
        }

        Ok(())
    }

    fn complete_request(&mut self, token: usize) -> Result<(), Error> {
        debug!("request with token {} completed", token);
        let handle = self.requests.remove(token);
        let mut handle = self.multi.remove2(handle)?;
        handle.get_mut().complete();

        Ok(())
    }

    fn fail_request(&mut self, token: usize, error: curl::Error) -> Result<(), Error> {
        let handle = self.requests.remove(token);
        let mut handle = self.multi.remove2(handle)?;
        handle.get_mut().fail(error);

        Ok(())
    }
}

impl Drop for Agent {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.upgrade() {
            handle.thread_terminated.store(true, Ordering::SeqCst);
        }
    }
}
