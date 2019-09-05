//! Performs PAG construction using the provided log trace,
//! either by connecting to an online dataflow, or by reading from a
//! serialized log trace.
//! 1. Reads from trace and constructs a replayed dataflow
//! 2. Uses `timely-adapter` to generate the intermediate `LogRecord`
//!     representation from the supplied trace
//! 3. Creates a PAG from the `LogRecord` representation

#![deny(missing_docs)]

use crate::pag::PagEdge;
use crate::pag::PagNode;
use st2_logformat::ActivityType;
use serde::Serialize;

#[macro_use]
extern crate abomonation_derive;

#[macro_use]
extern crate log;

// Contains algorithms to be run on the PAG
pub mod algo;

/// Contains the PAG construction
pub mod pag;

/// Contains commands to execute ST2
pub mod commands;

/// A generic ST2 error
pub struct STError(pub String);

impl From<std::io::Error> for STError {
    fn from(error: std::io::Error) -> Self {
        STError(format!("io error: {}", error.to_string()))
    }
}

impl From<tdiag_connect::ConnectError> for STError {
    fn from(error: tdiag_connect::ConnectError) -> Self {
        match error {
            tdiag_connect::ConnectError::IoError(e) => STError(format!("io error: {}", e)),
            tdiag_connect::ConnectError::Other(e) => STError(e),
        }
    }
}


#[derive(Serialize, Debug)]
/// Serialization type for socket
pub enum PagData {
    /// Pag edges
    Pag(PagEdge),
    /// all events (for highlighting)
    All((u64, u64)),
    /// aggregates (for analysis)
    Agg(KHopSummaryData),
    /// metrics
    Met(MetricsData),
    /// invariants
    Inv(InvariantData),
}

#[derive(Serialize, Debug)]
/// Serialization type for khop summaries
/// edge_type, worker_id, activity_count, weighted activity_count
pub struct KHopSummaryData {
    a: ActivityType,
    wf: u64,
    ac: u64,
    wac: u64,
}

#[derive(Serialize, Debug)]
/// Serialization type for metrics
/// from_worker,to_worker,activity_type,#(activities),t(activities),#(records)
pub struct MetricsData {
    wf: u64,
    wt: u64,
    a: ActivityType,
    ac: u64,
    at: u64,
    rc: u64,
}

#[derive(Serialize, Debug)]
/// Types of invariants that are checked
pub enum InvariantData {
    // /// Max Progress pause invariant
    // Progress,
    /// Max epoch duration invariant
    Epoch(EpochData),
    /// Max operator duration invariant
    Operator(OperatorData),
    /// Max message duration invariant
    Message(MessageData),
}

#[derive(Serialize, Debug)]
/// Serialization type for max epoch
pub struct EpochData {
    max: u64,
    from: PagNode,
    to: PagNode,
}

#[derive(Serialize, Debug)]
/// Serialization type for max operator
pub struct OperatorData {
    max: u64,
    from: PagEdge,
    to: PagEdge,
}

#[derive(Serialize, Debug)]
/// Serialization type for max message
pub struct MessageData {
    max: u64,
    msg: PagEdge,
}


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
