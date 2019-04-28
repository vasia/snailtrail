// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[macro_use]
extern crate abomonation_derive;

use logformat::{ActivityType, EventType, LogRecord, OperatorId, Worker};

use std::collections::HashMap;
use std::hash::Hash;
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::Collection;

use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::{Concat, Filter, Map, Partition};
use timely::dataflow::{Scope, Stream};
use timely::ExchangeData;

pub mod dataflow;

/// A node in the activity graph
#[derive(Abomonation, Clone, Debug, PartialEq, Hash, Eq, Copy, Ord, PartialOrd)]
pub struct PagNode {
    pub timestamp: logformat::Timestamp,
    pub worker_id: Worker,
}

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

// impl PagEdge {
//     pub fn weight(&self) -> u64 {
//         if self.destination.timestamp > self.source.timestamp {
//             (self.destination.timestamp - self.source.timestamp).as_nanos() as u64
//         } else {
//             (self.source.timestamp - self.destination.timestamp).as_nanos() as u64
//         }
//     }

//     pub fn is_message(&self) -> bool {
//         self.source.worker_id != self.destination.worker_id
//     }
// }

// impl PagOutput {
//     pub fn weight(&self) -> u64 {
//         match *self {
//             PagOutput::Edge(ref e) => e.weight(),
//             _ => 0,
//         }
//     }

//     pub fn destination(&self) -> &PagNode {
//         match *self {
//             PagOutput::Edge(ref e) => &e.destination,
//             PagOutput::StartNode(ref e) |
//             PagOutput::EndNode(ref e) => e,
//         }
//     }

//     pub fn destination_worker(&self) -> Worker {
//         match *self {
//             PagOutput::Edge(ref e) => e.destination.worker_id,
//             PagOutput::StartNode(ref e) |
//             PagOutput::EndNode(ref e) => e.worker_id,
//         }
//     }

//     pub fn source_timestamp(&self) -> logformat::Timestamp {
//         match *self {
//             PagOutput::Edge(ref e) => e.source.timestamp,
//             PagOutput::StartNode(ref e) |
//             PagOutput::EndNode(ref e) => e.timestamp,
//         }
//     }

//     pub fn destination_timestamp(&self) -> logformat::Timestamp {
//         match *self {
//             PagOutput::Edge(ref e) => e.destination.timestamp,
//             PagOutput::StartNode(ref e) |
//             PagOutput::EndNode(ref e) => e.timestamp,
//         }
//     }

//     pub fn is_message(&self) -> bool {
//         match *self {
//             PagOutput::Edge(ref e) => e.is_message(),
//             PagOutput::StartNode(_) |
//             PagOutput::EndNode(_) => false,
//         }
//     }
// }

// Used internal to this module during PAG construction.  We need a single stream containing all
// a worker's activity and an indication of whether it was entirely local or involved a remote
// worker.
// #[derive(Abomonation, Clone, Debug)]
// enum Timeline {
//     Local(PagEdge), // full-edge: computation
//     Remote(LogRecord), // half-edge: communication
// }

// impl Timeline {
//     fn get_start_timestamp(&self) -> Duration {
//         match *self {
//             Timeline::Local(ref edge) => edge.source.timestamp,
//             Timeline::Remote(ref rec) => rec.timestamp,
//         }
//     }

//     fn get_end_timestamp(&self) -> Duration {
//         match *self {
//             Timeline::Local(ref edge) => edge.destination.timestamp,
//             Timeline::Remote(ref rec) => rec.timestamp,
//         }
//     }

//     fn get_worker_id(&self) -> Worker {
//         match *self {
//             Timeline::Local(ref edge) => edge.source.worker_id,
//             Timeline::Remote(ref rec) => rec.local_worker,
//         }
//     }

//     fn get_sort_key(&self) -> (logformat::Timestamp, logformat::Timestamp) {
//         match *self {
//             Timeline::Local(ref edge) => (edge.source.timestamp, edge.destination.timestamp),
//             Timeline::Remote(ref rec) => (rec.timestamp, Duration::new(0, 0)),
//         }
//     }
// }

// /// Collects all data within a single epoch and applies user-defined logic.
// /// (A fusion of the `Accumulate` and `Map` operators but the logic is
// /// triggered on notification rather than as each data element is delivered.)
// trait MapEpoch<S: Scope, D: ExchangeData> {
//     fn map_epoch<F: Fn(&mut Vec<D>) + 'static>(&self, logic: F) -> Stream<S, D>;
// }

// impl<S: Scope, D: ExchangeData> MapEpoch<S, D> for Stream<S, D>
//     where S::Timestamp: Hash
// {
//     fn map_epoch<F: Fn(&mut Vec<D>) + 'static>(&self, logic: F) -> Stream<S, D> {
//         let mut accums = HashMap::new();
//         self.unary_notify(Pipeline,
//                           "MapEpoch",
//                           vec![],
//                           move |input, output, notificator| {
//             input.for_each(|time, data| {
//                                accums
//                                    .entry(time.time().clone())
//                                    .or_insert_with(Vec::new)
//                                    .extend_from_slice(&data);
//                                notificator.notify_at(time.retain());
//                            });

//             notificator.for_each(|time, _count, _notify| if let Some(mut accum) =
//                 accums.remove(time.time()) {
//                                      logic(&mut accum);
//                                      output.session(&time).give_iterator(accum.drain(..));
//                                  });
//         })
//     }
// }
