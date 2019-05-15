//! Connects to a TimelyDataflow / DifferentialDataflow instance that is run with
//! `TIMELY_WORKER_LOG_ADDR` env variable set and constructs a single epoch PAG
//! from the received log trace.

#![deny(missing_docs)]

#[macro_use]
extern crate log;

pub mod connect;
use crate::connect::Replayer;

use logformat::pair::Pair;
use logformat::{ActivityType, EventType, LogRecord};

use std::io::Read;
use std::time::Duration;

use timely::{
    dataflow::{
        channels::pact::Pipeline,
        operators::{capture::replay::Replay, generic::operator::Operator, map::Map},
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

use differential_dataflow::{
    collection::{AsCollection, Collection},
    operators::{consolidate::Consolidate, join::Join, reduce::Threshold},
};

/// Returns a `Collection` of `LogRecord`s that can be used for PAG construction.
/// The `LogRecord`s are sorted by timestamp and exchanged so that
/// SnailTrail peer == computation peer.
/// Should be called from within a dataflow.
pub fn make_log_records<S, R>(
    scope: &mut S,
    replayers: Vec<Replayer<R>>,
    index: usize,
) -> Collection<S, LogRecord, isize>
where
    S: Scope<Timestamp = Pair<u64, Duration>>,
    R: Read + 'static,
{
    let stream = replayers.replay_into(scope);
    stream
        .events_to_log_records()
        .as_collection()
        .peel_operators(&stream)
        .consolidate() // @TODO: quite a performance hit, perhaps we can avoid this?
}

/// Operator that converts a Stream of TimelyEvents to their LogRecord representation
trait EventsToLogRecords<S: Scope<Timestamp = Pair<u64, Duration>>> {
    /// Converts a Stream of TimelyEvents to their LogRecord representation
    fn events_to_log_records(&self) -> Stream<S, (LogRecord, Pair<u64, Duration>, isize)>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> EventsToLogRecords<S>
    for Stream<S, (Duration, usize, TimelyEvent)>
{
    fn events_to_log_records(&self) -> Stream<S, (LogRecord, Pair<u64, Duration>, isize)> {
        self.unary_frontier(Pipeline, "EpochalFlatMap", |_capability, _info| {
            // This only works since we're sure that each worker replays a consistent
            // worker log. In other cases, we'd need to implement a smarter stateful operator.
            let mut vector = Vec::new();

            move |input, output| {
                input.for_each(|cap, data| {
                    // drop the current capability
                    let retained = cap.retain();

                    data.swap(&mut vector);
                    for (t, wid, x) in vector.drain(..) {
                        let record = match x {
                            // Scheduling & Processing
                            Schedule(event) => {
                                let event_type = if event.start_stop == StartStop::Start {
                                    EventType::Start
                                } else {
                                    EventType::End
                                };

                                Some((
                                    LogRecord {
                                        timestamp: t,
                                        local_worker: wid as u64,
                                        activity_type: ActivityType::Scheduling,
                                        event_type,
                                        correlator_id: None,
                                        remote_worker: None,
                                        operator_id: Some(event.id as u64),
                                        channel_id: None,
                                    },
                                    Pair::new(retained.time().first, t),
                                    1,
                                ))
                            }
                            // data messages
                            Messages(event) => {
                                // @TODO: push the filtering of local data messages into log_pag
                                // discard local data messages for now
                                if event.source == event.target {
                                    None
                                } else {
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
                                            timestamp: t,
                                            local_worker: wid as u64,
                                            activity_type: ActivityType::DataMessage,
                                            event_type,
                                            correlator_id: Some(event.seq_no as u64),
                                            remote_worker,
                                            operator_id: None,
                                            channel_id: Some(event.channel as u64),
                                        },
                                        Pair::new(retained.time().first, t),
                                        1,
                                    ))
                                }
                            }
                            // Control Messages
                            Progress(event) => {
                                // discard local progress updates for now
                                // @TODO: push the filtering of local control messages into log_pag
                                if !event.is_send && event.source == wid {
                                    None
                                } else {
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
                                            timestamp: t,
                                            local_worker: wid as u64,
                                            activity_type: ActivityType::ControlMessage,
                                            event_type,
                                            correlator_id: Some(event.seq_no as u64),
                                            remote_worker,
                                            operator_id: None,
                                            channel_id: Some(event.channel as u64),
                                        },
                                        Pair::new(retained.time().first, t),
                                        1,
                                    ))
                                }
                            }
                            _ => None,
                        };

                        if let Some(record) = record {
                            let mut session = output.session(&retained);
                            session.give(record);
                        }
                    }
                });
            }
        })
    }
}

/// Strips a `Collection` of `LogRecord`s from encompassing operators.
trait PeelOperators<S: Scope<Timestamp = Pair<u64, Duration>>> {
    /// Returns a stream of LogRecords where records that describe
    /// encompassing operators have been stripped off
    /// (e.g. the dataflow operator for every direct child,
    /// the surrounding iterate operators for loops)
    fn peel_operators(
        &self,
        stream: &Stream<S, (Duration, usize, TimelyEvent)>,
    ) -> Collection<S, LogRecord, isize>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> PeelOperators<S> for Collection<S, LogRecord, isize> {
    fn peel_operators(
        &self,
        stream: &Stream<S, (Duration, usize, TimelyEvent)>,
    ) -> Collection<S, LogRecord, isize> {
        // only operates events, keyed by addr
        let operates = stream
            .flat_map(|(t, wid, x)| {
                // according to contract defined in connect.rs, dataflow setup
                // is collapsed into data-t=0ns and handed at t=(0, 0ns)
                if t.as_nanos() == 0 {
                    if let Operates(event) = x {
                        Some((
                            (event.addr, (wid as u64, Some(event.id as u64))),
                            Pair::new(0, Default::default()),
                            1,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .as_collection();

        let peel_addrs = operates.map(|(mut addr, _)| {
            addr.pop();
            addr
        });

        let peel_ids = operates.semijoin(&peel_addrs).map(|(_, id)| id).distinct();

        self.map(|x| (x.operator_id, x))
            .antijoin(&peel_ids)
            .map(|(_, x)| x)
    }
}
