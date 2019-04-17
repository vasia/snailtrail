// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[macro_use]
extern crate abomonation_derive;

use logformat::{LogRecord, ActivityType, EventType, Worker, OperatorId};

use std::collections::HashMap;
use std::hash::Hash;
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::Collection;

use timely::ExchangeData;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::{Concat, Filter, Map, Partition};
use timely::dataflow::{Scope, Stream};

pub mod dataflow;

/// A node in the activity graph
#[derive(Abomonation, Clone, Debug, PartialEq, Hash, Eq, Copy, Ord, PartialOrd)]
pub struct PagNode {
    pub timestamp: logformat::Timestamp,
    pub worker_id: Worker,
}

// impl Partitioning for PagNode {
//     fn partition(&self) -> u64 {
//         u64::from(self.worker_id)
//     }
// }

impl<'a> From<&'a LogRecord> for PagNode {
    fn from(record: &'a LogRecord) -> Self {
        PagNode {
            timestamp: record.timestamp,
            worker_id: record.local_worker,
        }
    }
}

/// Elements of a complete activity graph, including ingress/egress points
#[derive(Abomonation, Clone, Debug, Eq, Hash, PartialEq)]
pub enum PagOutput {
    // Entry point into the graph
    StartNode(PagNode),
    // Exit point from the graph
    EndNode(PagNode),
    // Graph edges
    Edge(PagEdge),
}

// impl SrcDst<PagNode> for PagOutput {
//     fn src(&self) -> Option<PagNode> {
//         match *self {
//             PagOutput::StartNode(_) => None,
//             PagOutput::EndNode(ref n) => Some(*n),
//             PagOutput::Edge(ref e) => Some(e.source),
//         }
//     }

//     fn dst(&self) -> Option<PagNode> {
//         match *self {
//             PagOutput::StartNode(ref n) => Some(*n),
//             PagOutput::EndNode(_) => None,
//             PagOutput::Edge(ref e) => Some(e.destination),
//         }
//     }
// }

/// Information on how traverse an edge
#[derive(Abomonation, Hash, Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub enum TraversalType {
    Undefined,
    Block,
    Unbounded,
}

/// An edge in the activity graph
#[derive(Abomonation, Clone, Debug, PartialEq, Hash, Eq, PartialOrd, Ord)]
pub struct PagEdge {
    /// The source node
    pub source: PagNode,
    /// The destination node
    pub destination: PagNode,
    /// The activity type
    pub edge_type: ActivityType,
    /// An optional operator ID
    pub operator_id: Option<OperatorId>,
    /// Edge dependency information
    pub traverse: TraversalType,
}

impl PagEdge {
    pub fn weight(&self) -> u64 {
        if self.destination.timestamp > self.source.timestamp {
            (self.destination.timestamp - self.source.timestamp).as_nanos() as u64
        } else {
            (self.source.timestamp - self.destination.timestamp).as_nanos() as u64
        }
    }

    pub fn is_message(&self) -> bool {
        self.source.worker_id != self.destination.worker_id
    }
}

impl PagOutput {
    pub fn weight(&self) -> u64 {
        match *self {
            PagOutput::Edge(ref e) => e.weight(),
            _ => 0,
        }
    }

    pub fn destination(&self) -> &PagNode {
        match *self {
            PagOutput::Edge(ref e) => &e.destination,
            PagOutput::StartNode(ref e) |
            PagOutput::EndNode(ref e) => e,
        }
    }

    pub fn destination_worker(&self) -> Worker {
        match *self {
            PagOutput::Edge(ref e) => e.destination.worker_id,
            PagOutput::StartNode(ref e) |
            PagOutput::EndNode(ref e) => e.worker_id,
        }
    }

    pub fn source_timestamp(&self) -> logformat::Timestamp {
        match *self {
            PagOutput::Edge(ref e) => e.source.timestamp,
            PagOutput::StartNode(ref e) |
            PagOutput::EndNode(ref e) => e.timestamp,
        }
    }

    pub fn destination_timestamp(&self) -> logformat::Timestamp {
        match *self {
            PagOutput::Edge(ref e) => e.destination.timestamp,
            PagOutput::StartNode(ref e) |
            PagOutput::EndNode(ref e) => e.timestamp,
        }
    }

    pub fn is_message(&self) -> bool {
        match *self {
            PagOutput::Edge(ref e) => e.is_message(),
            PagOutput::StartNode(_) |
            PagOutput::EndNode(_) => false,
        }
    }
}

// Used internal to this module during PAG construction.  We need a single stream containing all
// a worker's activity and an indication of whether it was entirely local or involved a remote
// worker.
#[derive(Abomonation, Clone, Debug)]
enum Timeline {
    Local(PagEdge), // full-edge: computation
    Remote(LogRecord), // half-edge: communication
}

impl Timeline {
    fn get_start_timestamp(&self) -> Duration {
        match *self {
            Timeline::Local(ref edge) => edge.source.timestamp,
            Timeline::Remote(ref rec) => rec.timestamp,
        }
    }

    fn get_end_timestamp(&self) -> Duration {
        match *self {
            Timeline::Local(ref edge) => edge.destination.timestamp,
            Timeline::Remote(ref rec) => rec.timestamp,
        }
    }

    fn get_worker_id(&self) -> Worker {
        match *self {
            Timeline::Local(ref edge) => edge.source.worker_id,
            Timeline::Remote(ref rec) => rec.local_worker,
        }
    }

    fn get_sort_key(&self) -> (logformat::Timestamp, logformat::Timestamp) {
        match *self {
            Timeline::Local(ref edge) => (edge.source.timestamp, edge.destination.timestamp),
            Timeline::Remote(ref rec) => (rec.timestamp, Duration::new(0, 0)),
        }
    }
}

/// Collects all data within a single epoch and applies user-defined logic.
/// (A fusion of the `Accumulate` and `Map` operators but the logic is
/// triggered on notification rather than as each data element is delivered.)
trait MapEpoch<S: Scope, D: ExchangeData> {
    fn map_epoch<F: Fn(&mut Vec<D>) + 'static>(&self, logic: F) -> Stream<S, D>;
}

impl<S: Scope, D: ExchangeData> MapEpoch<S, D> for Stream<S, D>
    where S::Timestamp: Hash
{
    fn map_epoch<F: Fn(&mut Vec<D>) + 'static>(&self, logic: F) -> Stream<S, D> {
        let mut accums = HashMap::new();
        self.unary_notify(Pipeline,
                          "MapEpoch",
                          vec![],
                          move |input, output, notificator| {
            input.for_each(|time, data| {
                               accums
                                   .entry(time.time().clone())
                                   .or_insert_with(Vec::new)
                                   .extend_from_slice(&data);
                               notificator.notify_at(time.retain());
                           });

            notificator.for_each(|time, _count, _notify| if let Some(mut accum) =
                accums.remove(time.time()) {
                                     logic(&mut accum);
                                     output.session(&time).give_iterator(accum.drain(..));
                                 });
        })
    }
}


/// Main entry point which converts a stream of events produced from instrumentation into a Program
/// Activity Graph (PAG).  This method expects log records which are batched into disjoint windows
/// of event time (epoch) and the output will contain a time-ordered stream of edges which include
/// both ends of an activity (e.g. start/end or send/receive pairs).

pub trait BuildProgramActivityGraph<S: Scope> where S::Timestamp: Lattice + Ord {
    fn build_program_activity_graph(&self, index: usize) -> Collection<S, PagOutput, isize>;
    // fn build_program_activity_graph(&self,
    //                                 threshold: Duration,
    //                                 delayed_message_threshold: u64,
    //                                 window_size_ns: u32,
    //                                 insert_waitig_edges: bool)
                                    // -> Stream<S, PagOutput>;
}

impl<S: Scope> BuildProgramActivityGraph<S> for Collection<S, LogRecord, isize> where S::Timestamp: Lattice + Ord {
    fn build_program_activity_graph(&self, index: usize) -> Collection<S, PagOutput, isize> {
        // Check worker timelines for completeness

        // Step 1: Group events by worker
        // let grouped = self.map(|x| (x.local_worker, x));

        // // Step 2: pair up control and remote data events and add edges between worker timelines

        // @TODO 2: follow matching algo

        // self
        //     .filter(|x| (x.activity_type == ActivityType::ControlMessage || x.activity_type == ActivityType::DataMessage))
        //     .map(|x| {
        //         let sender_id = match x.event_type {
        //             EventType::Sent => x.local_worker,
        //             EventType::Received => x.remote_worker.expect("remote worker missing"),
        //             et => panic!("unexpected event type for communication record ({:?})", et),
        //         };
        //         // Assign key to group related message events
        //         ((sender_id, x.correlator_id), x)

        //     })

        self
            // .inspect(move |x| println!("1 -- {} ; {}@{:?}", index, x.0, x.1))
            .map(|x| {
                PagOutput::StartNode(PagNode {
                    timestamp: x.timestamp,
                    worker_id: 0
                })
            })
            // .inspect(move |(x, t, _diff)| {
            //     if let PagOutput::StartNode(x) = x {
            //         println!("2 -- {} ; {:?}@{:?}", index, x.timestamp, t)
            //     }
            // })

        // let communication_edges = input
        //     .filter(|record| (record.activity_type == ActivityType::ControlMessage ||
        //                     record.activity_type == ActivityType::DataMessage))
        //     .map(|record| {
        //         let sender_id = match record.event_type {
        //             EventType::Sent => record.local_worker,
        //             EventType::Received => record.remote_worker.expect("remote worker missing"),
        //             et =>  panic!("unexpected event type for communication record ({:?})", et),
        //         };
        //         // Assign key to group related message events
        //         ((sender_id, record.correlator_id), record)
        //     })
        // // This matching logic largely duplicates the aggregation operator above but is
        // // parameterized slightly differently -> make this into a reusable operator.
        // .pair_up_events_and_check(EventType::Sent, EventType::Received, window_size_ns, |sent, recv| {
        //     assert!((sent.local_worker == recv.remote_worker.unwrap()) &&
        //             (sent.remote_worker.is_none() || sent.remote_worker.unwrap() == recv.local_worker));
        // });


    //     let partitions = communication_edges.partition(2, |e| match e {
    //         Timeline::Local(_) => (0, e),
    //         Timeline::Remote(_) => (1, e),
    //     });

    //     // partitions[0]: communication edges
    //     // partitions[1]: records

    //     let communication_edges =
    //         partitions[0].map(|rec| match rec {
    //                               Timeline::Local(edge) => PagOutput::Edge(edge),
    //                               _ => panic!("Incorrect data!"),
    //                           });
    //     let communication_records =
    //         partitions[1].map(|rec| match rec {
    //                               Timeline::Remote(log_record) => log_record,
    //                               _ => panic!("Incorrect data!"),
    //                           });

    //     let worker_timeline_input = input.concat(&communication_records);

    //     // construct worker time line and filter zero time events
    //     let worker_timelines = worker_timeline_input
    //         .build_worker_timelines(threshold,
    //                                 window_size_ns,
    //                                 insert_waitig_edges)
    //         .filter(|pag| if let PagOutput::Edge(ref e) = *pag {
    //                     e.source.worker_id != e.destination.worker_id ||
    //                     e.source.timestamp < e.destination.timestamp
    //                 } else {
    //                     true
    //                 });

    //     // Step 3: merge the contents of both streams and sort according to event time
    //     let result = worker_timelines.concat(&communication_edges);

    //     // let exchange = Exchange::new(|e: &PagOutput| u64::from(e.destination_worker()));
    //     // let mut timelines_per_epoch = HashMap::new();
    //     // let mut vector = Vec::new();
    //     result.map(|x| {
    //         PagOutput::StartNode(PagNode {
    //             timestamp: Duration::new(0, 10),
    //             worker_id: 0
    //         })
    //     })
    // //     result.unary_notify(exchange, "add traversal info", vec![], move |input, output, notificator| {
    //         // Organize all data by time and then according to worker ID
    //         input.for_each(|time, data| {
    //             println!("setting up up notify");
    //             let epoch_slot = timelines_per_epoch.entry(*time.time())
    //                 .or_insert_with(HashMap::new);
    //             data.swap(&mut vector);
    //             for record in vector.drain(..) {
    //                 epoch_slot.entry(record.destination_worker())
    //                     .or_insert_with(Vec::new)
    //                     .push(record);
    //             }
    //             println!("set up notify");
    //             notificator.notify_at(time.retain());
    //         });
    //         println!("maybe just nothing new");
    //         // Sequentially assemble the edges for each worker timeline by pairing up log records
    //         notificator.for_each(|time, _count, _notify| {
    //             println!("new notification");
    //             if let Some(mut timelines) = timelines_per_epoch.remove(time.time()) {
    //                 let mut session = output.session(&time);
    //                 for (_worker_id, mut raw_timeline) in timelines.drain() {
    //                     raw_timeline.sort_by_key(PagOutput::destination_timestamp);
    //                     let mut last_local_was_waiting = false;
    //                     for mut record in raw_timeline {
    //                         if let PagOutput::Edge(ref mut edge) = record {
    //                             if edge.is_message() // is message?
    //                                 && delayed_message_threshold > 0 // delayed_message functionality enabled?
    //                                 && edge.weight() as u64 > delayed_message_threshold // is delayed?
    //                                 && !last_local_was_waiting // preceding was not a local waiting activity
    //                             {
    //                                 // message is delayed and last is not waiting -> do not traverse
    //                                 edge.traverse = TraversalType::Block;
    //                             } else if edge.edge_type == ActivityType::Waiting {
    //                                 // edge is waiting -> do not traverse
    //                                 edge.traverse = TraversalType::Block;
    //                                 last_local_was_waiting = true;
    //                             } else {
    //                                 // edge is local not waiting -> traverse
    //                                 last_local_was_waiting = false;
    //                                 edge.traverse = TraversalType::Unbounded;
    //                             }
    //                             debug_assert!(edge.traverse != TraversalType::Undefined,
    //                                 "Edge must not have undefined traversal type after adding traversal info, edge: {:?}", edge);
    //                         }
    //                         session.give(record);
    //                     }
    //                 }
    //             }
    //         });
    //     })
    }
}
