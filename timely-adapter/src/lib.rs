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
) -> Collection<S, LogRecord, isize>
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
    /// Constructs a collection of log records to be used in PAG construction from an event stream.
    fn construct_lrs(&self, index: usize) -> Collection<S, LogRecord, isize>;
    /// Makes a stream of log records from an event stream.
    fn make_lrs(&self, index: usize) -> Stream<S, (LogRecord, S::Timestamp, isize)>;
    /// Builds a log record at differential time `time` from the supplied computation event.
    fn build_lr<T: Lattice + Ord>(comp_event: CompEvent, time: T) -> Option<(LogRecord, T, isize)>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> ConstructLRs<S>
    for Stream<S, CompEvent>
    where S::Timestamp: Lattice + Ord
{
    fn construct_lrs(&self, index: usize) -> Collection<S, LogRecord, isize> {
        self.make_lrs(index)
            .as_collection()
            .peel_operators(&self)

        // arrange_core + as_collection is the same as consolidate,
        // but allows to specify the exchange contract used. We
        // don't want records to be shuffled around -> Pipeline.
        // .arrange_core::<_, OrdValSpine<_, _, _, _>>(Pipeline, "PipelinedConsolidate")
        // .as_collection(|d, _| d.clone())
        // .inspect(|x| println!("{:?}", x))
    }

    fn make_lrs(&self, index: usize) -> Stream<S, (LogRecord, S::Timestamp, isize)> {
        self.unary_frontier(Pipeline, "LogRecordConstruct", |_capability, _info| {
            let mut vector = Vec::new();

            move |input, output| {
                input.for_each(|cap, data| {
                    data.swap(&mut vector);

                    let mut session = output.session(&cap);

                    for comp_event in vector.drain(..) {
                        let record = Self::build_lr(comp_event, cap.time().clone());

                        if let Some(record) = record {
                            // retract every log record at next epoch
                            let mut retract = record.clone();
                            retract.1.first += 1;
                            retract.2 = -1;

                            session.give(record);
                            // session.give(retract);
                        }
                    }
                });
            }
        })
    }

    fn build_lr<T: Lattice + Ord>(comp_event: CompEvent, time: T) -> Option<(LogRecord, T, isize)> {
        let (epoch, seq_no, (t, wid, x)) = comp_event;
        match x {
            // Scheduling & Processing
            Schedule(event) => {
                let event_type = if event.start_stop == StartStop::Start {
                    EventType::Start
                } else {
                    EventType::End
                };

                Some((
                    LogRecord {
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
                    },
                    time,
                    1,
                ))
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

                Some((
                    LogRecord {
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
                    },
                    time,
                    1,
                ))
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

                Some((
                    LogRecord {
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
                    },
                    time,
                    1,
                ))
            }
            _ => None
        }
    }
}


/// Strips a `Collection` of `LogRecord`s from encompassing operators.
trait PeelOperators<S: Scope> where S::Timestamp: Lattice + Ord {
    /// Returns a stream of LogRecords where records that describe
    /// encompassing operators have been stripped off
    /// (e.g. the dataflow operator for every direct child,
    /// the surrounding iterate operators for loops)
    fn peel_operators(
        &self,
        stream: &Stream<S, CompEvent>,
    ) -> Collection<S, LogRecord, isize>;
}

impl<S: Scope> PeelOperators<S> for Collection<S, LogRecord, isize>
where S::Timestamp: Lattice + Ord {
    fn peel_operators(
        &self,
        stream: &Stream<S, CompEvent>,
    ) -> Collection<S, LogRecord, isize> {
        // only operates events, keyed by addr
        let operates = stream
            .flat_map(|(_, _, (_t, wid, x))| {
                if let Operates(x) = x {
                    Some(((x.addr, (wid as u64, Some(x.id as u64))), Default::default(), 1))
                } else {
                    None
                }
            })
            .as_collection();

        let peel_addrs = operates.map(|(mut addr, _)| {
            addr.pop();
            addr
        });

        let peel_ids = operates
            .semijoin(&peel_addrs)
            .map(|(_, (wid, id))| (wid, id))
            .distinct();

        self.map(|x| ((x.local_worker, x.operator_id), x))
            .antijoin(&peel_ids)
            .map(|(_, x)| x)
    }
}
