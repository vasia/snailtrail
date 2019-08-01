//! Performs PAG construction using the provided log trace,
//! either by connecting to an online dataflow, or by reading from a
//! serialized log trace.
//! 1. Reads from trace and constructs a replayed dataflow
//! 2. Uses `timely-adapter` to generate the intermediate `LogRecord`
//!     representation from the supplied trace
//! 3. Creates a PAG from the `LogRecord` representation

#![deny(missing_docs)]

#[macro_use]
extern crate abomonation_derive;

#[macro_use]
extern crate log;

// Contains algorithms to be run on the PAG
pub mod algo;

/// Contains the PAG construction
pub mod pag;

// /// Elements of a complete activity graph, including ingress/egress points
// #[derive(Abomonation, Clone, Debug, Eq, Hash, PartialEq)]
// pub enum PagOutput {
//     /// Entry point into the graph
//     StartNode(PagNode),
//     /// Exit point from the graph
//     EndNode(PagNode),
//     /// Graph edges
//     Edge(PagEdge),
// }

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
