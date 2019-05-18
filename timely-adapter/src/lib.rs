//! Connects to a TimelyDataflow / DifferentialDataflow instance that is run with
//! `TIMELY_WORKER_LOG_ADDR` env variable set and constructs a single epoch PAG
//! from the received log trace.

#![deny(missing_docs)]

#[macro_use]
extern crate log;

pub mod connect;
use crate::connect::Replayer;

use logformat::{ActivityType, EventType, LogRecord};
use logformat::pair::Pair;

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
use timely::logging::TimelyEvent::Text;

use differential_dataflow::{
    collection::{AsCollection, Collection},
    operators::{consolidate::Consolidate, join::Join, reduce::Threshold},
};
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::lattice::Lattice;

/// Returns a `Collection` of `LogRecord`s that can be used for PAG construction.
/// The `LogRecord`s are sorted by timestamp and exchanged so that
/// SnailTrail peer == computation peer.
/// Should be called from within a dataflow.
pub fn make_log_records<S, R>(
    scope: &mut S,
    replayers: Vec<Replayer<S::Timestamp, R>>,
    index: usize,
) -> Collection<S, LogRecord, isize>
where
    S: Scope<Timestamp = Pair<u64, Duration>>,
    S::Timestamp: Lattice + Ord,
    R: Read + 'static,
{
    let stream = replayers.replay_into(scope);

    let log_records = stream
        .events_to_log_records(index)
        .as_collection()
        .peel_operators(&stream);

    // arrange_core + as_collection is the same as consolidate,
    // but allows to specify the exchange contract used. We
    // don't want records to be shuffled around -> Pipeline.
    log_records

        // .consolidate()
        // .inner
        // .exchange(|x| (x.0).local_worker)
        // .inspect_time(move |t, x| if index == 0 { println!("{:?}", x);})
        // .as_collection()

        // .arrange_core::<_, OrdValSpine<_, _, _, _>>(Pipeline, "PipelinedConsolidate")
        // .as_collection(|d, _| d.clone())
        // .inspect(|x| println!("{:?}", x))
}

/// Operator that converts a Stream of TimelyEvents to their LogRecord representation
trait EventsToLogRecords<S: Scope<Timestamp = Pair<u64, Duration>>> where S::Timestamp: Lattice + Ord {
    /// Converts a Stream of TimelyEvents to their LogRecord representation
    fn events_to_log_records(&self, index: usize) -> Stream<S, (LogRecord, S::Timestamp, isize)>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> EventsToLogRecords<S>
    for Stream<S, (Duration, usize, TimelyEvent)>
    where S::Timestamp: Lattice + Ord
{
    fn events_to_log_records(&self, index: usize) -> Stream<S, (LogRecord, S::Timestamp, isize)> {
        self.unary_frontier(Pipeline, "EpochalFlatMap", |_capability, _info| {
            // This only works since we're sure that each worker replays a consistent
            // worker log. In other cases, we'd need to implement a smarter stateful operator.
            let mut vector: Vec<(Duration, _, _)> = Vec::new();

            // holds an epoch for every source_peer this worker maintains
            let mut epoch = Vec::new();

            // holds a continually increasing seq_no for every source_peer this worker maintains
            let mut seq_no: Vec<u64> = Vec::new();

            move |input, output| {
                input.for_each(|cap, data| {
                    // drop the current capability
                    let retained = cap.retain();

                    data.swap(&mut vector);
                    for (t, wid, x) in vector.drain(..) {
                        let record = match x {
                            // epoch advance
                            Text(event) => {
                                get_or_fill(&mut epoch, wid);
                                epoch[wid] += 1;
                                None
                            },
                            // Scheduling & Processing
                            Schedule(event) => {
                                let event_type = if event.start_stop == StartStop::Start {
                                    EventType::Start
                                } else {
                                    EventType::End
                                };

                                get_or_fill(&mut seq_no, wid);
                                seq_no[wid] += 1;

                                Some((
                                    LogRecord {
                                        seq_no: seq_no[wid] - 1,
                                        epoch: get_or_fill(&mut epoch, wid),
                                        timestamp: t,
                                        local_worker: wid as u64,
                                        activity_type: ActivityType::Scheduling,
                                        event_type,
                                        correlator_id: None,
                                        remote_worker: None,
                                        operator_id: Some(event.id as u64),
                                        channel_id: None,
                                    },
                                    retained.time().clone(),
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

                                    get_or_fill(&mut seq_no, wid);
                                    seq_no[wid] += 1;

                                    Some((
                                        LogRecord {
                                            seq_no: seq_no[wid] - 1,
                                            epoch: get_or_fill(&mut epoch, wid),
                                            timestamp: t,
                                            local_worker: wid as u64,
                                            activity_type: ActivityType::DataMessage,
                                            event_type,
                                            correlator_id: Some(event.seq_no as u64),
                                            remote_worker,
                                            operator_id: None,
                                            channel_id: Some(event.channel as u64),
                                        },
                                        retained.time().clone(),
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

                                    get_or_fill(&mut seq_no, wid);
                                    seq_no[wid] += 1;

                                    Some((
                                        LogRecord {
                                            seq_no: seq_no[wid] - 1,
                                            epoch: get_or_fill(&mut epoch, wid),
                                            timestamp: t,
                                            local_worker: wid as u64,
                                            activity_type: ActivityType::ControlMessage,
                                            event_type,
                                            correlator_id: Some(event.seq_no as u64),
                                            remote_worker,
                                            operator_id: None,
                                            channel_id: Some(event.channel as u64),
                                        },
                                        retained.time().clone(),
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

fn get_or_fill<T: Default + Copy + Clone>(v: &mut Vec<T>, index: usize) -> T {
    if let Some(x) = v.get(index) {
        *x
    } else {
        for _ in v.len() .. index + 1  {
            v.push(Default::default());
        }

        v[index]
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
        stream: &Stream<S, (Duration, usize, TimelyEvent)>,
    ) -> Collection<S, LogRecord, isize>;
}

impl<S: Scope> PeelOperators<S> for Collection<S, LogRecord, isize>
where S::Timestamp: Lattice + Ord {
    fn peel_operators(
        &self,
        stream: &Stream<S, (Duration, usize, TimelyEvent)>,
    ) -> Collection<S, LogRecord, isize> {
        // only operates events, keyed by addr
        let operates = stream
            .unary_frontier(Pipeline, "operates_filter", |_capability, _info| {
                let mut buffer = Vec::new();

                move |input, output| {
                    input.for_each(|cap, data| {
                        let mut session = output.session(&cap);

                        data.swap(&mut buffer);
                        for (t, wid, x) in buffer.drain(..) {
                            // according to contract defined in connect.rs, dataflow setup
                            // is collapsed into data-t=1ns
                            if t.as_nanos() == 0 {
                                if let Operates(event) = x {
                                    session.give(((event.addr, (wid as u64, Some(event.id as u64))), cap.time().clone(), 1));
                                }
                            }
                        }
                    })
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
