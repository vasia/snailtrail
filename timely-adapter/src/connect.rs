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
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::{Arc, Mutex},
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

use TimelyEvent::{Channels, Messages, Operates, Progress, Schedule, Text};

/// A prepared computation event: (epoch, seq_no, event)
/// The seq_no is a worker-unique identifier of the message and given
/// in the order the events are logged.
pub type CompEvent = (u64, u64, (Duration, WorkerIdentifier, TimelyEvent));

/// A replayer that reads data to be streamed into timely
pub type Replayer<T, R> = EventReader<T, CompEvent, R>;

/// A ReplayWriter that writes data to be streamed into timely
pub type ReplayWriter<T, R> = EventWriter<T, CompEvent, R>;

/// Types of replayer to be created from `make_replayers`
pub enum ReplayerType {
    /// a TCP-backed online replayer
    Tcp(TcpStream),
    /// a file-backed offline replayer
    File(File),
}

impl Read for ReplayerType {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            ReplayerType::Tcp(x) => x.read(buf),
            ReplayerType::File(x) => x.read(buf),
        }
    }
}

/// Listens on 127.0.0.1:8000 and opens `source_peers` sockets from the
/// computations we're examining (one socket for every worker on the
/// examined computation).
/// Adapted from TimelyDataflow examples / https://github.com/utaal/timely-viz
pub fn open_sockets(count: usize) -> Arc<Mutex<Vec<Option<TcpStream>>>> {
    assert!(count > 0);
    let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    Arc::new(Mutex::new(
        (0..count)
            .map(|_| Some(listener.incoming().next().unwrap().unwrap()))
            .collect::<Vec<_>>(),
    ))
}

// @TODO Currently, the computation runs best with worker_peers == source_peers.
// It might be worth investigating how replaying could benefit from worker_peers > source_peers.
// @TODO TCP stream optimization might be necessary (e.g. smarter consumption of batches)
/// Construct replayers that read data from sockets or file and can stream it into
/// timely dataflow. If `Some(sockets)` is passed, the replayers assume an online setting.
/// Adapted from TimelyDataflow examples / https://github.com/utaal/timely-viz
pub fn make_replayers <T>(
    worker_index: usize,
    worker_peers: usize,
    source_peers: usize,
    sockets: Option<Arc<Mutex<Vec<Option<TcpStream>>>>>,
) -> Vec<Replayer<T, ReplayerType>>
where T: Lattice + Ord {
    info!(
        "Creating replayers\tworker index: {}, worker peers: {}, source peers: {}, online: {}",
        worker_index,
        worker_peers,
        source_peers,
        sockets.is_some()
    );

    if let Some(sockets) = sockets {
        // online
        sockets
            .lock()
            .unwrap()
            .iter_mut()
            .enumerate()
            .filter(|(i, _)| *i % worker_peers == worker_index)
            .map(move |(_, s)| s.take().unwrap())
            .map(|r| EventReader::new(ReplayerType::Tcp(r)))
            .collect::<Vec<_>>()
    } else {
        // from file
        (0..source_peers)
            .filter(|i| i % worker_peers == worker_index)
            .map(|i| {
                let name = format!("{:?}.dump", i);
                let path = Path::new(&name);

                match File::open(&path) {
                    Err(why) => panic!("couldn't open. {}", why.description()),
                    Ok(file) => file,
                }
            })
            .map(|f| EventReader::new(ReplayerType::File(f)))
            .collect::<Vec<_>>()
    }
}

/// Logging of events to TCP or file.
/// For live analysis, provide `SNAILTRAIL_ADDR` as env variable.
/// Else, the computation will log to file for later replay.
pub fn register_logger<T: 'static + NextEpoch + Lattice + Ord + Debug + Default + Clone + Abomonation> (worker: &mut Worker<Generic>, load_balance_factor: usize) {
    assert!(load_balance_factor > 0);

    if let Ok(addr) = ::std::env::var("SNAILTRAIL_ADDR") {
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

        unsafe { log_pag(worker, writers); }
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

        unsafe { log_pag(worker, writers); }
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
    fn tick_epoch(&mut self, tuple_time: &Duration) {
        self.first += 1;
    }

    fn tick_sys(&mut self, tuple_time: &Duration) {
        self.second = *tuple_time;
    }

    fn get_epoch(&self) -> u64 {
        self.first
    }
}

/// Status of logged computation. Changed by log message
/// `[st] computation done`.
#[derive(Eq, PartialEq)]
enum ComputationStatus {
    /// Computation is ongoing
    Ongoing,
    /// marker that computation has ended
    WrappingUp,
    /// apabilities have been dropped; no further messages
    /// should get sent.
    WrappedUp
}

// @TODO: further describe contract between log_pag and SnailTrail; mark as unsafe
// @TODO: for triangles query with round size == 1, the computation is slowed down by TCP.
//        A reason for this might be the overhead in creating TCP packets, so it might be
//        worthwhile investigating the reintroduction of batching for very small computation
//        rounds.
/// Registers a `TimelyEvent` logger which outputs relevant log events for PAG construction.
/// 1. Only relevant events are written to `writer`.
/// 2. Using `Text` events as markers, logged events are written out at one time per epoch.
/// 3. Dataflow setup is collapsed into t=(0, 0ns) so that peel_operators is more efficient.
/// 4. If the computation is bounded, capabilities will be dropped correctly at the end of
///    computation.
///
/// This function is marked `unsafe` as it relies on an implicit contract with the
/// logged computation:
/// 1. After `advance_to(0)`, the beginning of computation should be logged:
///    `"[st] begin computation at epoch: {:?}"`
/// 2. After each epoch's `worker.step()`, the epoch progress should be logged:
///    `"[st] closed times before: {:?}"`
/// 3. (optional) After the last round, the end of the computation should be logged:
///    `"[st] computation done"`
/// Failing to do so might have unexpected effects on the PAG creation.
unsafe fn log_pag<W: 'static + Write, T: 'static + NextEpoch + Lattice + Ord + Debug + Default + Clone + Abomonation>(
    worker: &mut Worker<Generic>,
    mut writers: Vec<ReplayWriter<T, W>>,
) {
    // initialized to default, dropped as soon as the
    // computation starts running.
    let mut curr_cap: T = Default::default();

    // first real frontier, used for setting up the computation
    // (`Operates` et al.).
    let mut next_cap: T = Default::default();

    // worker-unique identifier for every message sent by computation
    let mut seq_no = 0;

    // buffer of relevant events for a batch. As a batch only ever belongs
    // to a single epoch (epoch markers only appear at the beginning of a batch),
    // we don't have to keep track of times for batch elements.
    let mut buffer = Vec::new();

    let mut status = ComputationStatus::Ongoing;

    // Advances system time if it hasn't yet been advanced
    // for the current progress batch.
    let mut tick_sys = true;

    // If a computation's epoch size exceeds `MAX_FUEL` events are batched
    // into multiple processing times within that epoch.
    // Received data messages don't use up fuel to avoid separating
    // them from the schedules event that is used to reorder them.
    const MAX_FUEL: usize = 512;
    let mut fuel = MAX_FUEL;

    let mut curr_writer = 0;

    // for debugging
    let worker_index = worker.index();

    worker
        .log_register()
        .insert::<TimelyEvent, _>("timely", move |time, data| {
            match status {
                ComputationStatus::Ongoing => {
                    for (t, wid, x) in data.drain(..) {
                        match &x {
                            // Text events advance epochs, start and end logging
                            Text(e) => {
                                info!("w{}@{:?} text event: {}", worker_index, curr_cap, e);

                                // Text events mark ends of epochs in the computation.
                                next_cap.tick_epoch(&t);

                                info!("flushing to {}", curr_writer);
                                flush_buffer(std::mem::replace(&mut buffer, Vec::new()),
                                             &mut writers[curr_writer],
                                             &mut curr_cap,
                                             worker_index);
                                curr_writer = (curr_writer + 1) % writers.len();
                                fuel = MAX_FUEL;
                                tick_sys = true;

                                if e.starts_with("[st] computation done") {
                                    status = ComputationStatus::WrappingUp;
                                    break;
                                }
                            }
                            Operates(_) | Channels(_) => {
                                fuel -= 1;
                                seq_no += 1;

                                // all operates events should happen in the initialization epoch,
                                // i.e., before any Text event epoch markers have been observed
                                assert!(next_cap == curr_cap && curr_cap == Default::default());

                                buffer.push((curr_cap.get_epoch(), seq_no, (Default::default(), wid, x)));
                            }
                            other => {
                                // we're only interested in Schedule, Progress and Messages events.
                                // Progress and Messages event shouldn't be local only.
                                let push_new_event = match other {
                                    Schedule(_) => true,
                                    Progress(e) if e.is_send || e.source != tuple.1 => true,
                                    Messages(e) if e.source != e.target => true,
                                    _ => false
                                };

                                if push_new_event {
                                    fuel -= 1;
                                    seq_no += 1;

                                    if tick_sys {
                                        next_cap.tick_sys(&tuple.0);
                                        tick_sys = false;
                                        advance_cap(&mut writers, &mut next_cap, &mut curr_cap, worker_index);
                                    }

                                    buffer.push((curr_cap.get_epoch(), seq_no, tuple));
                                }
                            }
                        }

                        if fuel == 0 {
                            info!("flushing to {}", curr_writer);
                            flush_buffer(std::mem::replace(&mut buffer, Vec::new()),
                                         &mut writers[curr_writer],
                                         &mut curr_cap,
                                         worker_index);
                            curr_writer = (curr_writer + 1) % writers.len();
                            fuel = MAX_FUEL;
                            tick_sys = true;
                        }
                    }
                }
                ComputationStatus::WrappingUp => {
                    info!("w{}@ep{:?}: timely logging wrapping up", worker_index, curr_cap);
                    assert!(buffer.len() == 0, "flush buffer before wrap up!");

                    // free capabilities
                    for writer in writers.iter_mut() {
                        writer.push(Event::Progress(vec![(curr_cap.clone(), -1)]));
                    }

                    status = ComputationStatus::WrappedUp;
                }
                ComputationStatus::WrappedUp => {}
            };
        });
}

/// Flushes the buffer The buffer is written out at `curr_cap`.
fn flush_buffer<W, T>(buffer: Vec<CompEvent>,
                      writer: &mut ReplayWriter<T, W>,
                      curr_cap: &mut T,
                      index: usize)
where
    W: 'static + Write,
    T: Abomonation + Clone + Debug {

    if buffer.len() > 0 {
        info!("w{} flush@{:?}: count: {}", index, curr_cap, buffer.len());
        writer.push(Event::Messages(curr_cap.clone(), buffer));
    }
}

/// Sends progress information for SnailTrail. The capability is
/// downgraded to `next_cap`, allowing the frontier to advance.
fn advance_cap<W, T>(writers: &mut Vec<ReplayWriter<T, W>>,
                     next_cap: &mut T,
                     curr_cap: &mut T,
                     index: usize)
where
    W: 'static + Write,
    T: Abomonation + Clone + Debug {
    info!("w{} progresses from {:?} to {:?}", index, curr_cap, next_cap);

    for writer in writers.iter_mut() {
        writer.push(Event::Progress(vec![
            (next_cap.clone(), 1),
            (curr_cap.clone(), -1),
        ]));
    }

    *curr_cap = next_cap.clone();
}
