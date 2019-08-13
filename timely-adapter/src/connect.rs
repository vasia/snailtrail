//! Helpers to log traces for PAG construction from timely & differential.
//!
//! To log a computation, see `Adapter`'s docstring. If `SNAILTRAIL_ADDR=<IP>:<Port>`
//! is set as env variable, the computation will be logged online via TCP.
//!
//! Replay a log trace with `replay_into` or `replay_throttled`.

use std::{
    error::Error,
    fs::File,
    io::Write,
    net::TcpStream,
    path::Path,
    time::Duration,
};
use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;

use timely::{
    communication::allocator::Generic,
    dataflow::operators::capture::{event::EventPusher, Event, EventReader, EventWriter},
    logging::{TimelyEvent, WorkerIdentifier, StartStop, Logger},
    worker::Worker,
};

use TimelyEvent::{Messages, Operates, Channels, Progress, Schedule, Text};

use differential_dataflow::logging::DifferentialEvent;
use DifferentialEvent::Merge;

use logformat::pair::Pair;


/// A prepared computation event: (epoch, seq_no, Option<event_length>, event)
/// The seq_no is a worker-unique identifier of the message and given
/// in the order the events are logged.
pub type CompEvent = (u64, u64, Option<usize>, (Duration, WorkerIdentifier, TimelyEvent));

/// A replayer that reads data to be streamed into timely
pub type Replayer<T, R> = EventReader<T, CompEvent, R>;

/// A ReplayWriter that writes data to be streamed into timely
pub type ReplayWriter<T, R> = EventWriter<T, CompEvent, R>;

/// Wrapper around a `Vec` of `(Duration, usize, DifferentialEvent|TimelyEvent)`
pub enum DataflowEvents <'a> {
    /// A `TimelyEvent` batch
    Timely(&'a mut Vec<(Duration, usize, TimelyEvent)>),
    /// A `DifferentialEvent` batch
    Differential(&'a mut Vec<(Duration, usize, DifferentialEvent)>)
}

/// Types of Write a PAGLogger can attach to
pub enum TcpStreamOrFile {
    /// a TCP-backed online reader
    Tcp(TcpStream),
    /// a file-backed offline reader
    File(File),
}

impl Write for TcpStreamOrFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            TcpStreamOrFile::Tcp(stream) => stream.write(buf),
            TcpStreamOrFile::File(file) => file.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            TcpStreamOrFile::Tcp(stream) => stream.flush(),
            TcpStreamOrFile::File(file) => file.flush(),
        }
    }
}

/// Timely Adapter API
/// 1. Create an instance with `attach`.
///    *IMPORTANT:* This instance should be created at the very beginning
///    of the timely scope, otherwise some event messages might not be
///    correctly picked up
/// 2. Call `tick_epoch()` every time a source computation epoch closes.
///
/// For live analysis, provide `SNAILTRAIL_ADDR` as env variable.
/// Else, the computation will log to file for later replay.
pub struct Adapter {
    /// This adapter's logger, used to communicate epoch ticks.
    logger: Logger<TimelyEvent>
}

impl Adapter {
    /// Creates a `PAGLogger` instance and attaches it to the computation.
    pub fn attach(worker: &Worker<Generic>) -> Self {
        Self::attach_configured(worker, None, None)
    }

    /// Creates a customized `PAGLogger` instance and attaches it to the computation.
    pub fn attach_configured(worker: &Worker<Generic>, load_balance_factor: Option<usize>, max_fuel: Option<usize>) -> Self {
        PAGLogger::create_and_attach(worker, load_balance_factor, max_fuel);
        let logger = worker.log_register().get::<TimelyEvent>("timely").expect("timely logger not found");
        Adapter { logger }
    }

    /// Communicates epoch completion to the underlying `PAGLogger`.
    pub fn tick_epoch(&self) {
        self.logger.log(TimelyEvent::Text(Default::default()));
    }
}


/// Listens for `TimelyEvent`s that are relevant to the PAG construction and writes them to `writers`.
/// If the computation is bounded, capabilities will be dropped correctly at the end of computation.
pub struct PAGLogger {
    /// Writers log messages can be written to.
    writers: Vec<ReplayWriter<Pair<u64, Duration>, TcpStreamOrFile>>,
    /// Current writer used to log messages to. Used for load balancing
    /// with the `load_balance_factor`
    curr_writer: usize,
    /// Cap time at which current events are written.
    curr_cap: Pair<u64, Duration>,
    /// Next cap time
    next_cap: Pair<u64, Duration>,
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
    /// Stores mapping `operator address (target of channel) -> vec![channel ids]`
    op_addr_to_ch_target: HashMap<usize, Vec<usize>>,
    /// Stores mapping `operator address (source of channel) -> channel id`
    op_addr_to_ch_source: HashMap<usize, usize>,
    /// Stores mapping `operator id -> operator addr`
    op_id_to_op_addr: HashMap<usize, usize>,
    /// Stores current record count for a given channel id
    channel_records: HashMap<usize, usize>,
    /// For debugging (tracks this logger's worker index)
    worker_index: usize,
    /// For debugging (tracks per-epoch messages this pag logger received)
    overall_messages: u64,
    /// For debugging (tracks per-epoch messages this pag logger wrote)
    pag_messages: u64,
    /// For debugging (elapsed time)
    elapsed: std::time::Instant,
    /// For benchmarking (blow up PAG arbitrarily)
    epoch_count: u64,
}

impl PAGLogger {
    /// Convenience method to create and directly attach a `PAGLogger`
    pub fn create_and_attach(worker: &Worker<Generic>, load_balance_factor: Option<usize>, max_fuel: Option<usize>) {
        let pag_logger = Self::new(worker, load_balance_factor, max_fuel);
        pag_logger.attach(worker);
    }

    /// Creates a new PAGLogger. Events are logged to TCP or file.
    /// Commonly called indirectly from `create_and_attach`
    pub fn new(worker: &Worker<Generic>, load_balance_factor: Option<usize>, max_fuel: Option<usize>) -> Self {
        let load_balance_factor = if let Some(load_balance_factor) = load_balance_factor {
            load_balance_factor
        } else {
            1
        };

        let max_fuel = if let Some(max_fuel) = max_fuel {
            max_fuel
        } else {
            4096
        };

        let writers = if let Ok(addr) = ::std::env::var("SNAILTRAIL_ADDR") {
            info!("w{} registers logger @{:?}: lbf{}, fuel{}", worker.index(), &addr, load_balance_factor, max_fuel);
            (0 .. load_balance_factor)
                .map(|_| TcpStream::connect(&addr).expect("could not connect to logging stream"))
                .map(|stream| {
                    // SnailTrail should be able to keep up with an online computation.
                    // If batch sizes are too large, they should be buffered. Blocking the
                    // TCP connection is not an option as it slows down the main computation.
                    // stream
                    //    .set_nonblocking(true)
                    //    .expect("set_nonblocking call failed");

                    EventWriter::<Pair<u64, Duration>, _, _>::new(TcpStreamOrFile::Tcp(stream))
                })
                .collect::<Vec<_>>()
        } else {
            (0 .. load_balance_factor).map(|i| {
                let name = format!("../timely-snailtrail/{:?}.dump", (worker.index() + i * worker.peers()));
                info!("creating {}", name);
                let path = Path::new(&name);
                let file = match File::create(&path) {
                    Err(why) => panic!("couldn't create {}: {}", path.display(), why.description()),
                    Ok(file) => file,
                };
                EventWriter::<Pair<u64, Duration>, _, _>::new(TcpStreamOrFile::File(file))
            }).collect::<Vec<_>>()
        };

        PAGLogger {
            writers,
            curr_writer: 0,
            curr_cap: Default::default(),
            next_cap: Pair::new(1, Default::default()),
            seq_no: 0,
            buffer: Vec::new(),
            tick_sys: true,
            max_fuel,
            fuel: max_fuel,
            op_addr_to_ch_target: HashMap::new(),
            op_addr_to_ch_source: HashMap::new(),
            op_id_to_op_addr: HashMap::new(),
            channel_records: HashMap::new(),
            worker_index: worker.index(),
            overall_messages: 0,
            pag_messages: 0,
            elapsed: std::time::Instant::now(),
            epoch_count: 0,
        }
    }

    /// Redirects all events from the `TimelyEvent` logger to self.
    pub fn attach(self, worker: &Worker<Generic>) {
        // if there's already a logger attached, we won't override it
        if let Err(_) = ::std::env::var("TIMELY_WORKER_LOG_ADDR") {
            let timely_logger = Rc::new(RefCell::new(self));

            // TODO: Merge events aren't guaranteed to be picked up early enough
            // so that they're assigned to the correct SchedulesEvent.
            // They are generated at the "right" time, but there exists a race
            // condition due to the way we read out timely & differential events in `attach`.
            // For this reason, we currently don't add arrangement information.
            // let differential_logger = timely_logger.clone();
            // worker
            //     .log_register()
            //     .insert::<DifferentialEvent, _>("differential/arrange", move |_time, data| {
            //         differential_logger.borrow_mut().publish_batch(DataflowEvents::Differential(data));
            //     });

            worker
                .log_register()
                .insert::<TimelyEvent, _>("timely", move |_time, data| {
                    timely_logger.borrow_mut().publish_batch(DataflowEvents::Timely(data));
                });
        }
    }


    /// Publishes a batch of logged events and advances the capability.
    pub fn publish_batch(&mut self, data: DataflowEvents) {
        match data {
            DataflowEvents::Differential(data) => {
                for (_t, _wid, x) in data.drain(..) {
                    match &x {
                        Merge(e) if e.complete.is_some() => {
                            let op_addr = self.op_id_to_op_addr.get(&e.operator).expect("op id in addr not found");
                            // we need `ch_source` here, since we're not interested in the operator id
                            // provided by the `Batch` event, but where the batch event's records flow to
                            // (e.g., a join operator).
                            let ch_id = self.op_addr_to_ch_source.get(&op_addr).expect("op addr in chs not found");

                            let counter = self.channel_records.entry(*ch_id).or_insert(0);
                            *counter += e.complete.unwrap();
                        }
                        _ => {}
                    }
                }
            }
            DataflowEvents::Timely(data) => {
                for (t, wid, x) in data.drain(..) {
                    self.overall_messages += 1;

                    match &x {
                        TimelyEvent::Operates(_) | TimelyEvent::Channels(_) |
                        TimelyEvent::Messages(_) | TimelyEvent::Schedule(_) |
                        TimelyEvent::Text(_) => {
                            // println!("{:?}, {:?}: {:?}", std::time::SystemTime::now(), t, x);
                        },
                        _ => {}
                    }

                    match &x {
                        Text(_) => self.tick_epoch(),
                        Operates(e) => {
                            self.pag_messages += 1;
                            self.fuel -= 1;
                            self.seq_no += 1;

                            // all operates events should happen in the initialization epoch,
                            // i.e., before any Text event epoch markers have been observed
                            assert!(self.next_cap.first == 1 && self.curr_cap == Default::default());

                            self.op_id_to_op_addr.insert(e.id, *e.addr.last().expect("addr empty"));

                            self.buffer.push((self.curr_cap.first, self.seq_no, None, (Default::default(), wid, x)));
                        }
                        Channels(e) => {
                            let ids = self.op_addr_to_ch_target.entry(e.target.0).or_insert(Vec::new());
                            ids.push(e.id);
                            self.op_addr_to_ch_source.insert(e.source.0, e.id);
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

                            // fetch length
                            let length = if e.start_stop == StartStop::Stop {
                                let op_addr = self.op_id_to_op_addr.get(&e.id).expect("op id not found");

                                // TODO: refactor
                                if let Some(ch_ids) = self.op_addr_to_ch_target.get(&op_addr) {
                                    let mut len = 0;
                                    for ch_id in ch_ids.iter() {
                                        if let Some(l) = self.channel_records.remove(ch_id) {
                                            len += l;
                                        }
                                    }
                                    if len > 0 {
                                        Some(len)
                                    } else {
                                        None
                                    }
                                } else {
                                    // For Inputs, we won't find a corresponding channel
                                    // (by definition, no channels end at an input)
                                    None
                                }
                            } else {
                                None
                            };

                            self.buffer.push((self.curr_cap.first, self.seq_no, length, (t, wid, x)));
                        }
                        // Remote progress events
                        Progress(e) if e.is_send || e.source != wid => {
                            self.pag_messages += 1;
                            self.fuel -= 1;
                            self.seq_no += 1;

                            if self.tick_sys {
                                self.advance_cap(&t);
                            }

                            self.buffer.push((self.curr_cap.first, self.seq_no, None, (t, wid, x)));
                        }
                        // Data receive events
                        Messages(e) if e.is_send == false => {
                            assert!(e.target == wid);
                            // A. update record counter
                            let counter = self.channel_records.entry(e.channel).or_insert(0);
                            *counter += e.length;

                            // B. if remote message: add to pag events
                            if e.source != e.target {
                                self.pag_messages += 1;
                                self.seq_no += 1;

                                self.buffer.push((self.curr_cap.first, self.seq_no, Some(e.length), (t, wid, x)));

                                // let (last_epoch, last_seq, last_length, (last_t, last_wid, last_x)) = self.buffer.pop()
                                    // .expect("non-empty buffer required");

                                // assert!(if let Schedule(e) = &last_x {e.start_stop == StartStop::Start}
                                        // else {false});

                                // Reposition received remote data message:
                                // 1. push the data message with previous' seq_no
                                // self.buffer.push((self.curr_cap.first, last_seq, Some(e.length), (t, wid, x)));
                                // 2. push the schedule event
                                // self.buffer.push((last_epoch, self.seq_no, last_length, (last_t, last_wid, last_x)));
                            }
                        }
                        // remote data send events
                        Messages(e) if e.source != e.target => {
                            assert!(e.source == wid);

                            self.pag_messages += 1;
                            self.fuel -= 1;
                            self.seq_no += 1;
                            if self.tick_sys {
                                self.advance_cap(&t);
                            }
                            self.buffer.push((self.curr_cap.first, self.seq_no, Some(e.length), (t, wid, x)));
                        }
                        _ => {}
                    }

                    if self.fuel == 0 {
                        self.flush_buffer();
                    }
                }
            }
        }
    }

    /// Advances the PAGLogger's epoch.
    pub fn tick_epoch(&mut self) {
        trace!("w{}@{:?} tick epoch", self.worker_index, self.curr_cap);

        if self.epoch_count % 1 == 0 {
            if self.curr_cap.first > 0 {
                println!("{}|{}|{}|{}|{}", self.worker_index, self.curr_cap.first - 1, self.elapsed.elapsed().as_nanos(), self.overall_messages, self.pag_messages);
            }
            self.elapsed = std::time::Instant::now();
            self.overall_messages = 0;
            self.pag_messages = 0;

            self.next_cap.first += 1;
        }

        if self.curr_cap == Default::default() {
            // The dataflow structure is propagated to all writers.
            self.flush_to_all();
        } else {
            if self.epoch_count % 1 == 0 {
                self.flush_buffer();
                self.curr_writer = (self.curr_writer + 1) % self.writers.len();
            }
        }

        self.epoch_count += 1;
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
        self.next_cap.second = *t;
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

impl Drop for PAGLogger {
    fn drop(&mut self) {
        info!("w{}@ep{:?}: timely logging wrapping up", self.worker_index, self.curr_cap);
        // assert!(self.buffer.len() == 0, "flush buffer before wrap up!");

        // free capabilities
        for writer in self.writers.iter_mut() {
            writer.push(Event::Progress(vec![(self.curr_cap.clone(), -1)]));
        }
    }
}
