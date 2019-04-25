#![deny(missing_docs)]

// Copyright 2019 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Connects to a TimelyDataflow / DifferentialDataflow instance that is run with
//! `TIMELY_WORKER_LOG_ADDR` env variable set and constructs a single epoch PAG
//! from the received log trace.

#![deny(missing_docs)]

pub mod connect;
use crate::connect::{make_file_replayers, make_replayers, open_sockets, Replayer};

use logformat::{ LogRecord, EventType, ActivityType };

use std::time::Duration;
use std::io::Read;

use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::capture::replay::Replay;
use timely::dataflow::operators::delay::Delay;
use timely::dataflow::operators::filter::Filter;
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::Input;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::logging::{StartStop, TimelyEvent};
use timely::logging::TimelyEvent::{Channels, Operates, Messages, Progress, Schedule, Text};
use timely::logging::WorkerIdentifier;
use timely::Data;
use timely::progress::Timestamp;
use timely::worker::Worker;
use timely::communication::Allocate;
use timely::order::Product;
use timely::dataflow::operators::enterleave::Enter;
use timely::dataflow::operators::enterleave::Leave;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::CapabilityRef;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::to_stream::ToStream;
use timely::logging::OperatesEvent;
use timely::dataflow::operators::exchange::Exchange;
use timely::logging::ScheduleEvent;
use timely::dataflow::InputHandle;

use differential_dataflow::collection::{ Collection, AsCollection };
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::{ Threshold, Reduce };
use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::trace::TraceReader;

/// Takes a stream of `TimelyEvent`s and returns a crude
/// reconstructed String representation of its dataflow.
fn reconstruct_dataflow<S: Scope<Timestamp = Duration>>(
    stream: &Stream<S, (Duration, usize, TimelyEvent)>,
) {
    let operates = stream
        .filter(|(_, worker, _)| *worker == 0)
        .flat_map(|(t, _worker, x)| if let Operates(event) = x {Some(((event.addr.clone(), event), t, 1))} else {None})
        .as_collection();
        // .inspect(|x| println!("{:?}", x));

    let channels_source = stream
        .filter(|(_, worker, _)| *worker == 0)
        .flat_map(|(t, _worker, x)| if let Channels(event) = x {
            // key by source
            let mut absolute_addr = event.scope_addr.clone();
            absolute_addr.push(event.source.0);
            Some(((absolute_addr, event), t, 1))
        } else {None})
        .as_collection();
        // .inspect(|x| println!("{:?}", x));

    channels_source
        // join source
        .join(&operates)
        .map(|(_key, (ch, op_src))| {
            // key by target
            let mut absolute_addr = ch.scope_addr.clone();
            absolute_addr.push(ch.target.0);
            (absolute_addr, (ch, op_src))
        })
        // join target
        .join(&operates)
        .map(|(_key, ((ch, op_src), op_target))|
             format!("{:?} Ch{}: ({}, {}) -> ({}, {})", ch.scope_addr, ch.id, op_src.id, op_src.name, op_target.id, op_target.name))
        .inspect(|x| println!("{:?}", x.0));
}


/// Operator that converts a Stream of TimelyEvents to their LogRecord representation
pub trait EventsToLogRecords<S: Scope<Timestamp = Duration>> {
    /// Converts a Stream of TimelyEvents to their LogRecord representation
    fn events_to_log_records(&self) -> Stream<S, (LogRecord, Duration, isize)>;
}

impl<S: Scope<Timestamp = Duration>> EventsToLogRecords<S> for Stream<S, (Duration, usize, TimelyEvent)> {
    fn events_to_log_records(&self) -> Stream<S, (LogRecord, Duration, isize)> {
        self.unary_frontier(Pipeline, "EpochalFlatMap", |_capability, _info| {
            // This only works since we're sure that each worker replays a consistent
            // worker log. In other cases, we'd need to implement a smarter stateful operator.
            let mut vector = Vec::new();

            let mut epoch = 0;

            move |input, output| {
                input.for_each(|cap, data| {
                    // drop the current capability
                    let mut retained = cap.retain();

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

                                Some((LogRecord {
                                    timestamp: t,
                                    epoch,
                                    local_worker: wid as u64,
                                    activity_type: ActivityType::Scheduling,
                                    event_type,
                                    correlator_id: None,
                                    remote_worker: None,
                                    operator_id: Some(event.id as u64),
                                    channel_id: None,
                                }, t, 1))
                            },
                            // data messages
                            Messages(event) => {
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

                                    Some((LogRecord {
                                        timestamp: t,
                                        epoch,
                                        local_worker: wid as u64,
                                        activity_type: ActivityType::DataMessage,
                                        event_type,
                                        correlator_id: Some(event.seq_no as u64),
                                        remote_worker,
                                        operator_id: None,
                                        channel_id: Some(event.channel as u64)
                                    }, t, 1))
                                }
                            },
                            // Control Messages
                            Progress(event) => {
                                // discard local progress updates for now
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

                                    Some((LogRecord {
                                        timestamp: t,
                                        epoch,
                                        local_worker: wid as u64,
                                        activity_type: ActivityType::ControlMessage,
                                        event_type,
                                        correlator_id: Some(event.seq_no as u64),
                                        remote_worker,
                                        operator_id: None,
                                        channel_id: Some(event.channel as u64),
                                    }, t, 1))
                                }
                            },
                            // epoch updates
                            Text(event) => {
                                epoch += 1;
                                None
                            },
                            _ => None
                        };

                        if let Some(record) = record {
                            let mut session = output.session(&retained);
                            session.give(record);
                        }
                    }
                });
            }
        },
        )
    }
}


/// Strips a `Collection` of `LogRecord`s from encompassing operators.
pub trait PeelOperators<S: Scope<Timestamp = Duration>> {
    /// Returns a stream of LogRecords where records that describe
    /// encompassing operators have been stripped off
    /// (e.g. the dataflow operator for every direct child,
    /// the surrounding iterate operators for loops)
    fn peel_operators(&self, stream: &Stream<S, (Duration, usize, TimelyEvent)> /*&Collection<S, (Vec<usize>, OperatesEvent), isize>*/) -> Collection<S, LogRecord, isize>;
}

impl<S: Scope<Timestamp = Duration>> PeelOperators<S> for Collection<S, LogRecord, isize> {
    fn peel_operators(&self, stream: &Stream<S, (Duration, usize, TimelyEvent)> /*&Collection<S, (Vec<usize>, OperatesEvent), isize>*/) -> Collection<S, LogRecord, isize> {

        let operates = stream
            .flat_map(|(t, _wid, x)| {
                if let Operates(event) = x {
                    Some(((event.addr.clone(), event), t, 1))
                } else {
                    None
                }
            })
            .as_collection();

        // all `Operates` addresses with their inner-most level removed
        let operates_anti = operates
            .map(|(mut addr, event)| {
                addr.pop();
                addr
            });

        // all `Operates` operator ids that are at the lowest nesting level
        let peeled = operates
            .antijoin(&operates_anti.distinct())
            .map(|(_, x)| x.id as u64);

        // all `LogRecord`s that have an `operator_id` that's not part of the lowest level
        let to_remove = self
            .flat_map(|x| if let Some(id) = x.operator_id { Some((id, x.timestamp)) } else { None })
            .antijoin(&peeled)
            .map(|(id, ts)| ts);

        // // `LogRecord`s without records with `operator_id`s that aren't part of the lowest level
        self.map(|x| (x.timestamp, x))
            .antijoin(&to_remove)
            .consolidate()
            .map(|(_, x)| x)
    }
}


/// log epoch at beginning
trait LogEpoch<S: Scope<Timestamp = Duration>> {
    /// log epoch at beginning
    fn log_epoch(&self) -> Collection<S, LogRecord, isize>;
}

impl<S: Scope<Timestamp = Duration>> LogEpoch<S> for Collection<S, LogRecord, isize> {
    fn log_epoch(&self) -> Collection<S, LogRecord, isize> {
        self.inner
            .unary(Pipeline, "epoch logger", |_default, _info| {
                let mut vector = Vec::new();
                let mut epoch = 0;
                move |input, output| {
                    input.for_each(|cap, data| {
                        data.swap(&mut vector);
                        let retained = cap.retain();
                        let mut session = output.session(&retained);
                        for (x, t, diff) in vector.drain(..) {
                            if x.epoch > epoch {
                                epoch = x.epoch;
                                println!("epoch: {}", epoch);
                            }
                            session.give((x, t, diff));
                        }
                    })
                }
            })
            .as_collection()
    }
}


/// Returns a `Collection` of `LogRecord`s that can be used for PAG construction.
/// Should be called from within a dataflow.
pub fn record_collection<S, R>(scope: &mut S, replayers: Vec<Replayer<R>>) -> Collection<S, LogRecord, isize>
where
    S: Scope<Timestamp = Duration>,
    R: Read + 'static,
{
    let stream = replayers.replay_into(scope);
    reconstruct_dataflow(&stream);

    stream
        .events_to_log_records()
        .as_collection()
        .peel_operators(&stream)
        // .inspect(|x| println!("RESULT: {:?} --- {}", x.2, x.0))
        .log_epoch()
}
