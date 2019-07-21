//! Helpers to obtain traces suitable for PAG construction from timely / differential.
//!
//! To log a computation, use `register_logger` at its beginning. If
//! `SNAILTRAIL_ADDR=<IP>:<Port>` is set as env variable, the computation will be logged
//! online via TCP. Regardless of offline/online logging, `register_logger`'s contract has
//! to be upheld. See `log_pag`'s docstring for more information.
//!
//! To obtain the logged computation's events, use `make_replayers` and `replay_into` them
//! into a dataflow of your choice. If you pass `Some(sockets)` created with `open_sockets`
//! to `make_replayer`, an online connection will be used as the event source.

use std::{
    error::Error,
    fs::File,
    io::Write,
    net::TcpStream,
    path::Path,
    time::Duration,
};

use timely::{
    communication::allocator::Generic,
    dataflow::operators::capture::{event::EventPusher, Event, EventReader, EventWriter},
    logging::{TimelyEvent, WorkerIdentifier, StartStop},
    worker::Worker,
};

use differential_dataflow::lattice::Lattice;

use logformat::pair::Pair;
use std::fmt::Debug;

use abomonation::Abomonation;

use TimelyEvent::{Messages, Operates, Progress, Schedule, Text};

/// A prepared computation event: (epoch, seq_no, event)
/// The seq_no is a worker-unique identifier of the message and given
/// in the order the events are logged.
pub type CompEvent = (u64, u64, (Duration, WorkerIdentifier, TimelyEvent));

/// A replayer that reads data to be streamed into timely
pub type Replayer<T, R> = EventReader<T, CompEvent, R>;

/// A ReplayWriter that writes data to be streamed into timely
pub type ReplayWriter<T, R> = EventWriter<T, CompEvent, R>;

/// Logging of events to TCP or file.
/// For live analysis, provide `SNAILTRAIL_ADDR` as env variable.
/// Else, the computation will log to file for later replay.
pub fn register_logger<T: 'static + NextEpoch + Lattice + Ord + Debug + Default + Clone + Abomonation> (worker: &mut Worker<Generic>, load_balance_factor: usize, max_fuel: usize) {
    assert!(load_balance_factor > 0);

    if let Ok(addr) = ::std::env::var("SNAILTRAIL_ADDR") {
        info!("w{} registers logger @{:?}: lbf{}, fuel{}", worker.index(), &addr, load_balance_factor, max_fuel);
        let writers = (0 .. load_balance_factor)
            .map(|_| TcpStream::connect(&addr).expect("could not connect to logging stream"))
            .map(|stream| {
                // SnailTrail should be able to keep up with an online computation.
                // If batch sizes are too large, they should be buffered. Blocking the
                // TCP connection is not an option as it slows down the main computation.
                stream
                    .set_nonblocking(true)
                    .expect("set_nonblocking call failed");

                EventWriter::<T, _, _>::new(stream)
            })
            .collect::<Vec<_>>();

        info!("w{} registered {} streams", worker.index(), writers.len());

        let mut pag_logger = PAGLogger::new(worker.index(), writers, max_fuel);
        worker
            .log_register()
            .insert::<TimelyEvent, _>("timely", move |_time, data| {
                pag_logger.publish_batch(data);
            });
    } else {
        let writers = (0 .. load_balance_factor).map(|i| {
            let name = format!("../timely-snailtrail/{:?}.dump", (worker.index() + i * worker.peers()));
            info!("creating {}", name);
            let path = Path::new(&name);
            let file = match File::create(&path) {
                Err(why) => panic!("couldn't create {}: {}", path.display(), why.description()),
                Ok(file) => file,
            };
            EventWriter::<T, _, _>::new(file)
        }).collect::<Vec<_>>();

        let mut pag_logger = PAGLogger::new(worker.index(), writers, max_fuel);
        worker
            .log_register()
            .insert::<TimelyEvent, _>("timely", move |_time, data| {
                pag_logger.publish_batch(data);
            });
    }
}

/// Wrapper for timestamps that defines how they progress system and epoch time
pub trait NextEpoch {
    /// advance epoch
    fn tick_epoch(&mut self, tuple_time: &Duration);
    /// advance system time
    fn tick_sys(&mut self, tuple_time: &Duration);
    /// get epoch part of time
    fn get_epoch(&self) -> u64;
}

impl NextEpoch for Duration {
    fn tick_epoch(&mut self, tuple_time: &Duration) {
        *self = *tuple_time;
    }

    fn tick_sys(&mut self, tuple_time: &Duration) {
        *self = *tuple_time;
    }

    fn get_epoch(&self) -> u64 {
        panic!("get epoch not possible for Duration");
    }
}

impl NextEpoch for Pair<u64, Duration> {
    fn tick_epoch(&mut self, _tuple_time: &Duration) {
        self.first += 1;
    }

    fn tick_sys(&mut self, tuple_time: &Duration) {
        self.second = *tuple_time;
    }

    fn get_epoch(&self) -> u64 {
        self.first
    }
}

// @TODO: for triangles query with round size == 1, the computation is slowed down by TCP.
//        A reason for this might be the overhead in creating TCP packets, so it might be
//        worthwhile investigating the reintroduction of batching for very small computation
//        rounds.
/// Used by a `TimelyEvent` logger to output relevant log events for PAG construction.
/// Relevant means:
/// 1. Only relevant events are written to `writer`.
/// 2. Using `Text` events as markers, logged events are written out at one time per epoch.
/// 3. Dataflow setup is collapsed into t=(0, 0ns) so that peel_operators is more efficient.
/// 4. If the computation is bounded, capabilities will be dropped correctly at the end of
///    computation.
///
/// The PAGLogger relies on the following contract to function properly:
/// 1. After `advance_to(0)`, the beginning of computation should be logged:
///    `"[st] begin computation at epoch: {:?}"`
/// 2. After each epoch's `worker.step()`, the epoch progress should be logged:
///    `"[st] closed times before: {:?}"`
/// Failing to do so might have unexpected effects on the PAG creation.
struct PAGLogger<T, W> where T: 'static + NextEpoch + Lattice + Ord + Debug + Default + Clone + Abomonation, W: 'static + Write {
    /// Writers log messages can be written to.
    writers: Vec<ReplayWriter<T, W>>,
    /// Current writer used to log messages to. Used for load balancing
    /// with the `load_balance_factor`
    curr_writer: usize,
    /// Cap time at which current events are written.
    curr_cap: T,
    /// Next cap time
    next_cap: T,
    /// Worker-unique identifier for every message sent by computation
    seq_no: u64,
    /// Buffer of relevant events for a batch. As a batch only ever belongs
    /// to a single epoch (epoch markers only appear at the beginning of a batch),
    /// we don't have to keep track of times for batch elements.
    buffer: Vec<CompEvent>,
    /// Advances system time if it hasn't yet been advanced
    /// for the current progress batch.
    tick_sys: bool,
    /// max fuel
    max_fuel: usize,
    /// If a computation's epoch size exceeds `MAX_FUEL` events are batched
    /// into multiple processing times within that epoch.
    /// Received data messages don't use up fuel to avoid separating
    /// them from the schedules event that is used to reorder them.
    fuel: usize,
    /// For debugging (tracks this logger's worker index)
    worker_index: usize,
    /// For debugging (tracks per-epoch messages this pag logger received)
    overall_messages: u64,
    /// For debugging (tracks per-epoch messages this pag logger wrote)
    pag_messages: u64,
    /// For debugging (elapsed time)
    elapsed: std::time::Instant,
    /// For prepping
    epoch_count: u64,
}

impl<T, W> PAGLogger<T, W> where T: 'static + NextEpoch + Lattice + Ord + Debug + Default + Clone + Abomonation, W: 'static + Write {
    /// Creates a new PAG Logger.
    pub fn new(worker_index: usize, writers: Vec<ReplayWriter<T, W>>, max_fuel: usize) -> Self {
        if worker_index == 0 {
            info!("worker|epoch|elapsed [ms]|overall|pag");
        }

        PAGLogger {
            writers,
            curr_writer: 0,
            curr_cap: Default::default(),
            next_cap: Default::default(),
            seq_no: 0,
            buffer: Vec::new(),
            tick_sys: true,
            max_fuel,
            fuel: max_fuel,
            worker_index,
            overall_messages: 0,
            pag_messages: 0,
            elapsed: std::time::Instant::now(),
            epoch_count: 0,
        }
    }

    /// Publishes a batch of logged events and advances the capability.
    pub fn publish_batch(&mut self, data: &mut Vec<(Duration, WorkerIdentifier, TimelyEvent)>) {
        for (t, wid, x) in data.drain(..) {
            self.overall_messages += 1;

            match &x {
                // Text events advance epochs, start and end logging
                Text(e) => {
                    trace!("w{}@{:?} text event: {}", self.worker_index, self.curr_cap, e);
                    // info!("w{}@{:?}, overall: {}, pag: {}, elapsed: {:?}", self.worker_index, self.curr_cap, self.overall_messages, self.pag_messages, self.elapsed.elapsed());

                    if self.epoch_count % 10 == 0 {
                        if self.curr_cap.get_epoch() > 0 {
                            println!("{}|{}|{}|{}|{}", self.worker_index, self.curr_cap.get_epoch() - 1, self.elapsed.elapsed().as_nanos(), self.overall_messages, self.pag_messages);
                        }
                        self.elapsed = std::time::Instant::now();
                        self.overall_messages = 0;
                        self.pag_messages = 0;

                        self.next_cap.tick_epoch(&t);
                    }

                    if self.curr_cap == Default::default() {
                        // The dataflow structure is propagated to all writers.
                        self.flush_to_all();
                    } else {
                        if self.epoch_count % 10 == 0 {
                            self.flush_buffer();
                            self.curr_writer = (self.curr_writer + 1) % self.writers.len();
                        }
                    }

                    self.epoch_count += 1;
                }
                Operates(_) => {
                    self.pag_messages += 1;
                    self.fuel -= 1;
                    self.seq_no += 1;

                    // all operates events should happen in the initialization epoch,
                    // i.e., before any Text event epoch markers have been observed
                    assert!(self.next_cap == self.curr_cap && self.curr_cap == Default::default());

                    self.buffer.push((self.curr_cap.get_epoch(), self.seq_no, (Default::default(), wid, x)));
                }
                Schedule(e) => {
                    self.pag_messages += 1;
                    // extend buffer size by 1 to avoid breaking up repositioning of
                    // schedule start events and consequent data messages
                    if self.fuel > 1 || e.start_stop == StartStop::Stop {
                        self.fuel -= 1;
                    }
                    self.seq_no += 1;

                    if self.tick_sys {
                        self.advance_cap(&t);
                    }

                    self.buffer.push((self.curr_cap.get_epoch(), self.seq_no, (t, wid, x)));
                }
                // Remote progress events
                Progress(e) if e.is_send || e.source != wid => {
                    self.pag_messages += 1;
                    self.fuel -= 1;
                    self.seq_no += 1;

                    if self.tick_sys {
                        self.advance_cap(&t);
                    }

                    self.buffer.push((self.curr_cap.get_epoch(), self.seq_no, (t, wid, x)));
                }
                // Remote data receive events
                Messages(e) if e.source != e.target && e.target == wid => {
                    self.pag_messages += 1;
                    self.seq_no += 1;

                    let (last_epoch, last_seq, last_event) = self.buffer.pop()
                        .expect("non-empty buffer required");

                    assert!(if let Schedule(e) = &last_event.2 {e.start_stop == StartStop::Start}
                            else {false});

                    // Reposition received remote data message:
                    // 1. push the data message with previous' seq_no
                    self.buffer.push((self.curr_cap.get_epoch(), last_seq, (t, wid, x)));
                    // 2. push the schedule event
                    self.buffer.push((last_epoch, self.seq_no, last_event));
                }
                // remote data send events
                Messages(e) if e.source != e.target => {
                    self.pag_messages += 1;
                    self.fuel -= 1;
                    self.seq_no += 1;
                    if self.tick_sys {
                        self.advance_cap(&t);
                    }
                    self.buffer.push((self.curr_cap.get_epoch(), self.seq_no, (t, wid, x)));
                }
                _ => {}
            }

            if self.fuel == 0 {
                self.flush_buffer();
            }
        }
    }

    /// Flushes the buffer repeatedly, until all writers have received its content.
    fn flush_to_all(&mut self) {
        trace!("w{}: flush@{:?} to ALL - count: {}", self.worker_index, self.curr_cap, self.buffer.len());
        for writer in self.writers.iter_mut() {
            writer.push(Event::Messages(self.curr_cap.clone(), self.buffer.clone()));
        }
        self.buffer.drain(..);

        self.fuel = self.max_fuel;
        self.tick_sys = true;
    }

    /// Flushes the buffer. The buffer is written out at `curr_cap`.
    fn flush_buffer(&mut self) {
        trace!("w{} flush@{:?} to {} - count: {}", self.worker_index, self.curr_cap, self.curr_writer, self.buffer.len());
        if self.buffer.len() > 0 {
            if let Some(writer) = self.writers.get_mut(self.curr_writer) {
                writer.push(Event::Messages(self.curr_cap.clone(), std::mem::replace(&mut self.buffer, Vec::new())));
            } else {
                panic!("couldn't get writer");
            }
        }

        self.fuel = self.max_fuel;
        self.tick_sys = true;
    }

    /// Sends progress information for SnailTrail. The capability is
    /// downgraded to `next_cap`, allowing the frontier to advance.
    fn advance_cap(&mut self, t: &Duration) {
        self.next_cap.tick_sys(t);
        self.tick_sys = false;

        trace!("w{} progresses from {:?} to {:?}", self.worker_index, self.curr_cap, self.next_cap);

        for writer in self.writers.iter_mut() {
            writer.push(Event::Progress(vec![
                (self.next_cap.clone(), 1),
                (self.curr_cap.clone(), -1),
            ]));
        }

        self.curr_cap = self.next_cap.clone();
    }
}

impl<T, W> Drop for PAGLogger<T, W> where T: 'static + NextEpoch + Lattice + Ord + Debug + Default + Clone + Abomonation, W: 'static + Write {
    fn drop(&mut self) {
        info!("w{}@ep{:?}: timely logging wrapping up", self.worker_index, self.curr_cap);
        // assert!(self.buffer.len() == 0, "flush buffer before wrap up!");

        // free capabilities
        for writer in self.writers.iter_mut() {
            writer.push(Event::Progress(vec![(self.curr_cap.clone(), -1)]));
        }
    }
}
