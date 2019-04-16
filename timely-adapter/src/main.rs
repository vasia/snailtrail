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

mod connect;
use crate::connect::{make_file_replayers, make_replayers, open_sockets};

use logformat::{ LogRecord, EventType, ActivityType };

use std::time::Duration;

use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::capture::replay::Replay;
use timely::dataflow::operators::delay::Delay;
use timely::dataflow::operators::filter::Filter;
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::operators::map::Map;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::logging::{StartStop, TimelyEvent};
use timely::logging::TimelyEvent::{Channels, Operates, Messages, Progress, Schedule};
use timely::Data;
use timely::progress::Timestamp;

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



trait EventsToLogRecords<S: Scope> where S::Timestamp: Lattice + Ord {
    /// Converts a Stream of TimelyEvents to their LogRecord representation
    fn events_to_log_records(&self) -> Stream<S, (LogRecord, Duration, isize)>;
}

impl<S: Scope> EventsToLogRecords<S> for Stream<S, (Duration, usize, TimelyEvent)> where S::Timestamp: Lattice + Ord {
    fn events_to_log_records(&self) -> Stream<S, (LogRecord, Duration, isize)> {
        self
            .flat_map(|(t, wid, x)| {
                match x {
                    // data messages
                    Messages(event) => {
                        let remote_worker = if event.target != event.source {
                            if event.is_send {
                                Some(event.target as u64)
                            } else {
                                Some(event.source as u64)
                            }
                        } else { None };

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
                            correlator_id: None,
                            remote_worker,
                            operator_id: None,
                            channel_id: Some(event.channel as u64)
                        }, t, 1))
                    },
                    Schedule(event) => {
                        let event_type = if event.start_stop == StartStop::Start {
                            EventType::Start
                        } else {
                            EventType::End
                        };

                        // @TODO: this should be a Processing event if some kind
                        // of message is created in-between scheduling start & finish.

                        Some((LogRecord {
                            timestamp: t,
                            local_worker: wid as u64,
                            activity_type: ActivityType::Scheduling,
                            event_type,
                            correlator_id: None,
                            remote_worker: None,
                            operator_id: Some(event.id as u64),
                            channel_id: None,
                        }, t, 1))
                    },
                    Progress(event) => {
                        let event_type = if event.is_send {
                            EventType::Sent
                        } else {
                            EventType::Received
                        };

                        let remote_worker = if event.is_send {
                            // Outgoing progress messages are similar to broadcasts.
                            // We don't know where they'll end up, but they might be remote.
                            // For lack of better semantics, we'll still put `None` here.
                            None
                        } else {
                            if event.source == wid {
                                None
                            } else {
                                Some(event.source as u64)
                            }
                        };

                        Some((LogRecord {
                            timestamp: t,
                            local_worker: wid as u64,
                            activity_type: ActivityType::ControlMessage,
                            event_type,
                            correlator_id: None,
                            remote_worker,
                            operator_id: None,
                            channel_id: Some(event.channel as u64)
                        }, t, 1))
                    },
                    _ => None
                }
            })
    }
}

/// Strips a `Collection` of `LogRecord`s from encompassing operators.
trait PeelOperators<S: Scope> where S::Timestamp: Lattice + Ord {
    /// Returns a stream of LogRecords where records that describe
    /// encompassing operators have been stripped off
    /// (e.g. the dataflow operator for every direct child,
    /// the surrounding iterate operators for loops)
    fn peel_operators(&self, ops: &Stream<S, (S::Timestamp, usize, TimelyEvent)>) -> Collection<S, LogRecord, isize>;
}

impl<S: Scope> PeelOperators<S> for Collection<S, LogRecord, isize> where S::Timestamp: Lattice + Ord {
    fn peel_operators(&self, ops: &Stream<S, (S::Timestamp, usize, TimelyEvent)>) -> Collection<S, LogRecord, isize> {
        // all `Operates` events
        let operates = ops
            .flat_map(|(t, _worker, x)| if let Operates(event) = x { Some(((event.addr.clone(), event), t, 1)) } else { None })
            .as_collection();

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
            .consolidate()
            .map(|(_, x)| x)
            .distinct()
    }
}


fn main() {
    // the number of workers in the computation we're examining
    let source_peers = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();

    // one socket per worker in the computation we're examining
    // let sockets = open_sockets(source_peers);

    timely::execute_from_args(std::env::args(), move |worker| {
        // (un)comment lines to switch between TCP and offline filedump reader
        // let sockets = sockets.clone();
        // let replayers = make_replayers(sockets, worker.index(), worker.peers());
        let replayers = make_file_replayers(worker.index(), source_peers, worker.peers());

        // define a new computation.
        worker.dataflow(|scope| {
            let mut stream = replayers.replay_into(scope);
            // stream.inspect(|x| println!("{:?}", x));

            // reconstruct_dataflow(&mut stream);

            // print_messages(&mut stream);
            stream
                .events_to_log_records()
                .as_collection()
                .peel_operators(&stream)
                .inspect(|x| println!("{}", x.0));
        });

        // stall application
        // use std::io::stdin;
        // stdin().read_line(&mut String::new()).unwrap();
    })
    .expect("Something went wrong.");
}
