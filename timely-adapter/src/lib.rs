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
use crate::connect::{make_file_replayers, make_replayers, open_sockets, FileReplayer};

use logformat::{ LogRecord, EventType, ActivityType };

use std::time::Duration;

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

use differential_dataflow::collection::{ Collection, AsCollection };
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::{ Threshold, Reduce };
use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;

/// Takes a stream of `TimelyEvent`s and returns a crude
/// reconstructed String representation of its dataflow.
fn reconstruct_dataflow<S: Scope<Timestamp = Duration>>(
    stream: &mut Stream<S, (Duration, usize, TimelyEvent)>,
) {
    let operates = stream
        .filter(|(_, worker, _)| *worker == 0)
        .flat_map(|(t, _worker, x)| {
            if let Operates(event) = x {
                if event.addr.len() > 1 {
                    Some(((event.addr[1], event), t, 1))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .as_collection();

    stream
        .filter(|(_, worker, _)| *worker == 0)
        .flat_map(|(t, _worker, x)| {
            if let Channels(event) = x {
                Some(((event.source.0, event), t, 1))
            } else {
                None
            }
        })
        .as_collection()
        .join(&operates) // join sources
        .map(|(_key, (a, b))| (a.target.0, (a, b)))
        .join(&operates) // join targets
        .map(|(_key, ((a, b), c))| (0, (a, (b, c))))
        .reduce(|_key, input, output| {
            for ((channel, (from, to)), _t) in input {
                output.push((
                    format!("Channel {}: ({}, {}) -> ({}, {})", channel.id, from.id, from.name, to.id, to.name),
                    1,
                ))
            }
        })
        .map(|(_key, x)| x)
        .inspect(|(x, _t, _diff)| println!("Dataflow: {}", x));
}


/// Operator that converts a Stream of TimelyEvents to their LogRecord representation
pub trait EventsToLogRecords<S: Scope<Timestamp = Duration>> {
    /// Converts a Stream of TimelyEvents to their LogRecord representation
    fn events_to_log_records(&self) -> Stream<S, (LogRecord, Duration, isize)>;
}

impl<S: Scope<Timestamp = Duration>> EventsToLogRecords<S> for Stream<S, (Duration, usize, TimelyEvent)> {
    fn events_to_log_records(&self) -> Stream<S, (LogRecord, Duration, isize)> {

        self.unary_frontier(Pipeline, "EpochalFlatMap", |capability, _info| {
            // This only works since we're sure that each worker replays a consistent
            // worker log. In other cases, we'd need to implement a smarter stateful operator.
            let mut epoch = Duration::new(0, 0);
            let mut vector = Vec::new();

            let mut retained = Some(capability);

            move |input, output| {
                input.for_each(|cap, data| {
                    drop(cap);

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
                                    local_worker: wid as u64,
                                    activity_type: ActivityType::Scheduling,
                                    event_type,
                                    correlator_id: None,
                                    remote_worker: None,
                                    operator_id: Some(event.id as u64),
                                    channel_id: None,
                                }, epoch, 1))
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
                                        local_worker: wid as u64,
                                        activity_type: ActivityType::DataMessage,
                                        event_type,
                                        correlator_id: Some(event.seq_no as u64),
                                        remote_worker,
                                        operator_id: None,
                                        channel_id: Some(event.channel as u64)
                                    }, epoch, 1))
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
                                        local_worker: wid as u64,
                                        activity_type: ActivityType::ControlMessage,
                                        event_type,
                                        correlator_id: Some(event.seq_no as u64),
                                        remote_worker,
                                        operator_id: None,
                                        channel_id: Some(event.channel as u64)
                                    }, epoch, 1))
                                }
                            },
                            // epoch updates
                            Text(event) => {
                                epoch += Duration::new(1, 0);
                                // caps[0].downgrade(&epoch);
                                if let Some(r) = retained.as_mut() {
                                    r.downgrade(&epoch);
                                }
                                None
                            },
                            _ => None
                        };

                        if let Some(record) = record {
                            if let Some(r) = retained.as_mut() {
                                let mut session = output.session(&r);
                                session.give(record);
                            }
                        }
                    }
                });

                if input.frontier.is_empty() {
                    retained = None;
                }
            }
        },
        )
    }
}

/// Extracts Operates activities from a Stream of events
pub trait EventsToOperates<S: Scope<Timestamp = Duration>> {
    /// Extracts Operates activities from a Stream of events
    fn events_to_operates(&self) -> Stream<S, ((Vec<usize>, OperatesEvent), Duration, isize)>;
}

impl<S: Scope<Timestamp = Duration>> EventsToOperates<S> for Stream<S, (Duration, usize, TimelyEvent)> {
    fn events_to_operates(&self) -> Stream<S, ((Vec<usize>, OperatesEvent), Duration, isize)> {

        self.unary_frontier(Pipeline, "OperatesExtractor", |capability, _info| {
            // all Operates are collected at (0,0)
            let mut epoch = Duration::new(0, 0);

            let mut retained = Some(capability);

            let mut vector = Vec::new();

            move |input, output| {
                input.for_each(|cap, data| {
                    drop(cap);

                    data.swap(&mut vector);
                    for (t, wid, x) in vector.drain(..) {
                        let operates = match x {
                            // Operates
                            Operates(event) => {
                                Some(((event.addr.clone(), event), epoch, 1))
                            },
                            // Text event marks the end of Operates activities
                            Text(event) => {
                                if epoch < Duration::new(1, 0) {
                                    epoch = Duration::new(1, 0);
                                    if let Some(r) = retained.as_mut() {
                                        r.downgrade(&epoch);
                                    }
                                }
                                None
                            },
                            _ => None,
                        };

                        if let Some(operates) = operates {
                            if let Some(r) = retained.as_mut() {
                                let mut session = output.session(&r);
                                session.give(operates);
                            }
                        }
                    }
                });

                if input.frontier.is_empty() {
                    retained = None;
                }
            }
        })
    }
}



/// Strips a `Collection` of `LogRecord`s from encompassing operators.
pub trait PeelOperators<S: Scope<Timestamp = Duration>> {
    /// Returns a stream of LogRecords where records that describe
    /// encompassing operators have been stripped off
    /// (e.g. the dataflow operator for every direct child,
    /// the surrounding iterate operators for loops)
    fn peel_operators(&self, ops: &Stream<S, (Duration, usize, TimelyEvent)>) -> Collection<S, LogRecord, isize>;
}

impl<S: Scope<Timestamp = Duration>> PeelOperators<S> for Collection<S, LogRecord, isize> {
    fn peel_operators(&self, ops: &Stream<S, (Duration, usize, TimelyEvent)>) -> Collection<S, LogRecord, isize> {
        // all `Operates` events
        let operates = ops.events_to_operates().as_collection();

        // all `Operates` addresses with their inner-most level removed
        let operates_anti = operates
            .map(|(mut addr, _)| {
                addr.pop();
                addr
            })
            .distinct();

        // all `Operates` operator ids that are at the lowest nesting level
        let peeled = operates
            .antijoin(&operates_anti)
            .consolidate()
            .distinct()
            .map(|(_, x)| x.id as u64);

        // all `LogRecord`s that have an `operator_id` that's not part of the lowest level
        let to_remove = self
            .flat_map(|x| if let Some(id) = x.operator_id { Some((id, x.timestamp)) } else { None })
            .antijoin(&peeled)
            .consolidate()
            .map(|(id, ts)| ts)
            .distinct();

        // `LogRecord`s without records with `operator_id`s that aren't part of the lowest level
        self
            .map(|x| (x.timestamp, x))
            .antijoin(&to_remove)
            .map(|(_, x)| x)
            .distinct()
    }
}



/// Returns a `Collection` of `LogRecord`s that can be used for PAG construction.
/// Should be called from within a dataflow.
pub fn record_collection<S>(scope: &mut S, replayers: Vec<FileReplayer>) -> Collection<S, LogRecord, isize>
where S: Scope<Timestamp = Duration> {
    let stream = replayers.replay_into(scope);
    // stream.inspect(|x| println!("{:?}", x));

    // reconstruct_dataflow(&mut stream);

    // print_messages(&mut stream);

    stream
        // .inspect(|x| println!("0 {:?}", x))
        .events_to_log_records()
        // .inspect(|x| println!("3 {:?}: {}", x.1, x.0))
        .as_collection()
        .peel_operators(&stream)
        .inspect(|x| println!("peeled: {:?}: {}", x.1, x.0))
}
