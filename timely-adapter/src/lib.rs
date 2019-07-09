//! Connects to a TimelyDataflow / DifferentialDataflow instance that is run with
//! `TIMELY_WORKER_LOG_ADDR` env variable set and constructs a single epoch PAG
//! from the received log trace.

#![deny(missing_docs)]

#[macro_use]
extern crate log;

pub mod connect;
use crate::connect::{Replayer, CompEvent};
pub mod replay_throttled;
use crate::replay_throttled::ReplayThrottled;

use logformat::{ActivityType, EventType, LogRecord};
use logformat::pair::Pair;

use std::io::Read;
use std::time::Duration;

use timely::{
    dataflow::{
        channels::pact::Pipeline,
        operators::generic::operator::Operator,
        operators::map::Map,
        Scope, Stream,
    },
    logging::{
        StartStop, TimelyEvent,
        TimelyEvent::{Messages, Operates, Progress, Schedule},
    },
};
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::channels::pact;
use timely::dataflow::operators::exchange::Exchange;
use timely::logging::TimelyEvent::Text;

use differential_dataflow::{
    collection::{AsCollection, Collection},
    operators::{consolidate::Consolidate, join::Join, reduce::Threshold},
};
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::lattice::Lattice;

/// Returns a `Collection` of `LogRecord`s that can be used for PAG construction.
pub fn create_lrs<S, R>(
    scope: &mut S,
    replayers: Vec<Replayer<S::Timestamp, R>>,
    index: usize,
    throttle: u64,
) -> Stream<S, LogRecord>
where
    S: Scope<Timestamp = Pair<u64, Duration>>,
    S::Timestamp: Lattice + Ord,
    R: Read + 'static,
{
    replayers
        .replay_throttled_into(index, scope, None, throttle)
        .construct_lrs(index)
}

/// Operator that converts a Stream of TimelyEvents to their LogRecord representation
trait ConstructLRs<S: Scope<Timestamp = Pair<u64, Duration>>> where S::Timestamp: Lattice + Ord {
    /// Constructs a stream of log records to be used in PAG construction from an event stream.
    fn construct_lrs(&self, index: usize) -> Stream<S, LogRecord>;
    /// Strips an event `Stream` of encompassing operators
    /// (e.g. the dataflow operator for every direct child,
    /// the surrounding iterate operators for loops).
    fn peel_ops(&self, index: usize) -> Stream<S, CompEvent>;
    /// Makes a stream of log records from an event stream.
    fn make_lrs(&self, index: usize) -> Stream<S, LogRecord>;
    /// Builds a log record at differential time `time` from the supplied computation event.
    fn build_lr(comp_event: CompEvent) -> Option<LogRecord>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> ConstructLRs<S>
    for Stream<S, CompEvent>
    where S::Timestamp: Lattice + Ord
{
    fn construct_lrs(&self, index: usize) -> Stream<S, LogRecord> {
        self.peel_ops(index)
            .make_lrs(index)
    }

    fn peel_ops(&self, index: usize) -> Stream<S, CompEvent> {
        let mut vector = Vec::new();
        let mut outer_operates = std::collections::BTreeSet::new();
        let mut ids_to_addrs = std::collections::HashMap::new();
        let mut total = 0;

        self.unary(Pipeline, "Peel", move |_, _| { move |input, output| {
            let timer = std::time::Instant::now();

            input.for_each(|cap, data| {
                data.swap(&mut vector);
                for (epoch, seq_no, (t, wid, x)) in vector.drain(..) {
                    match x {
                        Operates(e) => {
                            let mut addr = e.addr.clone();
                            addr.pop();
                            outer_operates.insert(addr);

                            ids_to_addrs.insert(e.id, e.addr);
                        }
                        Schedule(ref e) => {
                            assert!(cap.time() > &Pair::new(0, Default::default()));

                            let addr = ids_to_addrs.get(&e.id).expect("operates went wrong");
                            if !outer_operates.contains(addr) {
                                output.session(&cap).give((epoch, seq_no, (t, wid, x)));
                            }
                        }
                        _ => {
                            assert!(cap.time() > &Pair::new(0, Default::default()));

                            output.session(&cap).give((epoch, seq_no, (t, wid, x)));
                        }
                    }
                }

                if cap.time().first > 29996 {
                    total += timer.elapsed().as_nanos();
                    // println!("w{} peel_ops time: {}ms", index, total / 1_000_000);
                }
            });
            total += timer.elapsed().as_nanos();
        }})
    }

    fn make_lrs(&self, index: usize) -> Stream<S, LogRecord> {
        let mut vector = Vec::new();
        let mut total = 0;

        self.unary(Pipeline, "LogRecordConstruct", move |_, _| { move |input, output| {
            let timer = std::time::Instant::now();
            input.for_each(|cap, data| {
                data.swap(&mut vector);
                output.session(&cap).give_iterator(vector.drain(..).flat_map(|x| Self::build_lr(x).into_iter()));
                // @TODO: handle retractions (record.1.first += 1; record.2 = -1)

                if cap.time().first > 29996 {
                    total += timer.elapsed().as_nanos();
                    // println!("w{} make_lrs time: {}ms", index, total / 1_000_000);
                }
            });
            total += timer.elapsed().as_nanos();
        }})
    }

    fn build_lr(comp_event: CompEvent) -> Option<LogRecord> {
        let (epoch, seq_no, (t, wid, x)) = comp_event;
        match x {
            // Scheduling & Processing
            Schedule(event) => {
                let event_type = if event.start_stop == StartStop::Start {
                    EventType::Start
                } else {
                    EventType::End
                };

                Some(LogRecord {
                        seq_no,
                        epoch,
                        timestamp: t,
                        local_worker: wid as u64,
                        activity_type: ActivityType::Scheduling,
                        event_type,
                        remote_worker: None,
                        operator_id: Some(event.id as u64),
                        channel_id: None,
                        correlator_id: None,
                    })
            }
            // remote data messages
            Messages(event) => {
                let remote_worker = if event.is_send {
                    Some(event.target as u64)
                } else {
                    Some(event.source as u64)
                };

                let event_type = if event.is_send {
                    EventType::Sent
                } else {
                    EventType::Received
                };

                Some(LogRecord {
                        seq_no,
                        epoch,
                        timestamp: t,
                        local_worker: wid as u64,
                        activity_type: ActivityType::DataMessage,
                        event_type,
                        remote_worker,
                        operator_id: None,
                        channel_id: Some(event.channel as u64),
                        correlator_id: Some(event.seq_no as u64),
                    })
            }
            // Control Messages
            Progress(event) => {
                let event_type = if event.is_send {
                    EventType::Sent
                } else {
                    EventType::Received
                };

                let remote_worker = if event.is_send {
                    // Outgoing progress messages are broadcasts, so we don't know
                    // where they'll end up.
                    None
                } else {
                    Some(event.source as u64)
                };

                Some(LogRecord {
                        seq_no,
                        epoch,
                        timestamp: t,
                        local_worker: wid as u64,
                        activity_type: ActivityType::ControlMessage,
                        event_type,
                        remote_worker,
                        operator_id: None,
                        channel_id: Some(event.channel as u64),
                        correlator_id: Some(event.seq_no as u64),
                    })
            }
            // Channels / Operates events
            _ => None
        }
    }
}

// let mut vector = Vec::new();
// .inner
// .unary_frontier(Pipeline, "Logger", move |_, _| { move |input, output| {
//     input.for_each(|cap, data| {
//         data.swap(&mut vector);
//         output.session(&cap).give_iterator(vector.drain(..));
//     });
//     // println!("ggg: {} --- {:?}", index, input.frontier().frontier().to_vec());
// }})
// .as_collection()
