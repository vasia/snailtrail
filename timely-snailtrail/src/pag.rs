//! Pag Construction
//! Uses LogRecord representation to create a PAG that contains local and remote edges

use std::collections::HashMap;
use std::{io::Read, time::Duration};
use std::cmp::Ordering;

use itertools::Itertools;

use differential_dataflow::{collection::AsCollection, operators::join::Join, Collection};
use differential_dataflow::lattice::Lattice;

use timely::dataflow::{channels::pact::Exchange, operators::generic::operator::Operator, Scope};
use timely::dataflow::channels::pact::Pipeline;

use logformat::{ActivityType, EventType, LogRecord, OperatorId};
use logformat::pair::Pair;
use timely_adapter::{connect::Replayer, create_lrs};

/// A node in the PAG
#[derive(Abomonation, Clone, Debug, PartialEq, Hash, Eq, Copy)]
pub struct PagNode {
    /// Timestamp of the event (also a unique identifier!)
    pub timestamp: logformat::Timestamp,
    /// Unique ID of the worker the event belongs to
    pub worker_id: logformat::Worker,
}

impl Ord for PagNode {
    fn cmp(&self, other: &PagNode) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl PartialOrd for PagNode {
    fn partial_cmp(&self, other: &PagNode) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> From<&'a LogRecord> for PagNode {
    fn from(record: &'a LogRecord) -> Self {
        PagNode {
            timestamp: record.timestamp,
            worker_id: record.local_worker,
        }
    }
}

/// Information on how to traverse an edge. This is used e.g. in critical
/// participation to decide whether an edge should be included in the critical
/// path calculation. A `Block`ed edge can't be traversed (e.g. waiting activities)
#[derive(Abomonation, Hash, Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub enum TraversalType {
    /// Unclear traversal
    Undefined,
    /// No traversal possible
    Block,
    /// Traversal possible
    Unbounded,
}

/// An edge in the activity graph
#[derive(Abomonation, Clone, Debug, PartialEq, Hash, Eq)]
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

impl Ord for PagEdge {
    fn cmp(&self, other: &PagEdge) -> Ordering {
        self.source.cmp(&other.source)
    }
}

impl PartialOrd for PagEdge {
    fn partial_cmp(&self, other: &PagEdge) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// @TODO currently, this creates a new pag per epoch, but never removes the old one.
//       so state will continually grow and multiple pags exist side by side.
// @TODO: add an optional checking operator that tests individual logrecord timelines for sanity
// e.g. sched start -> sched end, no interleave, start & end always belong to scheduling,
// sent/received always to remote messages, we don't see message types that we can't handle yet,
// t1 is always > t0, same count of sent and received remote messages for every epoch
// same results regardless of worker count, received events always have a remote worker
// matched remote events are (remote-count / 2), remote event count is always even
/// Creates a PAG (a Collection of `PagEdge`s, grouped by epoch) from the provided `Replayer`s.
/// To be called from within a timely computation.
pub fn create_pag<S: Scope<Timestamp = Pair<u64, Duration>>, R: 'static + Read> (
    scope: &mut S,
    replayers: Vec<Replayer<S::Timestamp, R>>,
    index: usize,
) -> Collection<S, PagEdge, isize>
where S::Timestamp: Lattice + Ord {
    create_lrs(scope, replayers, index)
        .construct_pag(index)
}


/// Operator that converts a Stream of LogRecords to a PAG
pub trait ConstructPAG<S: Scope<Timestamp = Pair<u64, Duration>>>
where S::Timestamp: Lattice + Ord {
    /// Builds a PAG from `LogRecord` by concatenating local edges, control edges
    /// and data edges.
    fn construct_pag(&self, index: usize) -> Collection<S, PagEdge, isize>;
    /// Takes `LogRecord`s and connects local edges (per epoch, per worker)
    fn make_local_edges(&self, index: usize) -> Collection<S, PagEdge, isize>;
    /// Helper to create a `PagEdge` from two `LogRecord`s
    fn build_local_edge(prev: &LogRecord, record: &LogRecord) -> PagEdge;
    // /// Takes `LogRecord`s and connects control edges (per epoch, across workers)
    // fn make_control_edges(&self) -> Collection<S, PagEdge, isize>;
    // /// Takes `LogRecord`s and connects data edges (per epoch, across workers)
    // fn make_data_edges(&self) -> Collection<S, PagEdge, isize>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> ConstructPAG<S> for Collection<S, LogRecord, isize>
where S::Timestamp: Lattice + Ord {
    fn construct_pag(&self, index: usize) -> Collection<S, PagEdge, isize> {
use timely::dataflow::operators::inspect::Inspect;
        self.make_local_edges(index)
        // .concat(&self.make_data_edges())
    }

    // this should scale with epoch size, as it can update incrementally
    // however, it takes longer than simple timely computations
    fn make_local_edges(&self, index: usize) -> Collection<S, PagEdge, isize> {
        let edge_start = self.map(|x| ((x.local_worker, x.epoch, x.seq_no), x));
        let edge_end = self.map(|x| ((x.local_worker, x.epoch, x.seq_no + 1), x));

        edge_start
            .join(&edge_end)
            .map(|(_, (curr, prev))| Self::build_local_edge(&prev, &curr))
    }

    // this doesn't scale per epoch, but across epochs
    // fn make_local_edges(&self) -> Collection<S, PagEdge, isize> {
    //     use differential_dataflow::operators::reduce::Reduce;
    //     use differential_dataflow::operators::reduce::Count;
    //     use timely::dataflow::operators::inspect::Inspect;

    //     self
    //         .map(|x| ((x.local_worker, x.epoch), x))
    //         .reduce(|key, input, output| {
    //             let mut prev_record: Option<&LogRecord> = None;

    //             for (record, diff) in input {
    //                 assert!(*diff == 1);

    //                 if let Some(prev) = prev_record.clone() {
    //                     assert!(prev.local_worker == record.local_worker);
    //                     output.push((Self::build_local_edge(prev, *record), *diff));
    //                     prev_record = Some(*record);
    //                 } else {
    //                     prev_record = Some(*record);
    //                 }
    //             }
    //         })
    //         .map(|(_, x)| x)
    // }


    // this abuses differential and enforces ordering, which seems wrong
    // fn make_local_edges(&self) -> Collection<S, PagEdge, isize> {
    //     self.inner
    //         .unary_frontier(Pipeline, "local_edges", |_capability, _info| {
    //                 let mut buffer = Vec::new();

    //                 // stores the last matched record for every epoch & worker_id
    //                 let mut prev_record = HashMap::new();

    //                 move |input, output| {
    //                     input.for_each(|cap, data| {
    //                         data.swap(&mut buffer);

    //                         // group by local_worker: we're building out separate paths
    //                         for (_wid, group) in &buffer
    //                             .drain(..)
    //                             .group_by(|(record, _, _)| record.local_worker)
    //                         {
    //                             for (record, t, diff) in group {
    //                                 // assert!(diff == 1, format!("{:?}\t{:?}\t{:?}", record, t, diff));

    //                                 if let Some(prev) = prev_record.get(&(t.first, record.local_worker)) {
    //                                     // delay to differential epochs: timely capability times and differential times
    //                                     // don't necessarily match up (e.g., timely might batch more aggressively)
    //                                     let delayed = cap.delayed(&t);
    //                                     let mut session = output.session(&delayed);
    //                                     session.give((
    //                                         Self::build_local_edge(prev, &record),
    //                                         t.clone(),
    //                                         diff,
    //                                     ));
    //                                     prev_record.insert((t.first, record.local_worker), record);
    //                                 } else {
    //                                     // the first node of an epoch
    //                                     trace!(
    //                                         "w{}'s first node of epoch {:?}: {:?}",
    //                                         record.local_worker,
    //                                         t.clone(),
    //                                         record
    //                                     );
    //                                     prev_record.insert((t.first, record.local_worker), record);
    //                                 }
    //                             }
    //                         }
    //                     });

    //                     // (optional cleanup since we don't give capabilities to state)
    //                     // prev_record.retain(|(t, _), _| input.frontier().less_equal(t));
    //                 }
    //             },
    //         )
    //         .as_collection()
    // }

    fn build_local_edge(prev: &LogRecord, record: &LogRecord) -> PagEdge {
        let edge_type = match (prev.event_type, record.event_type) {
            // SchedStart ----> SchedEnd
            (EventType::Start, EventType::End) => ActivityType::Scheduling,
            // SchedEnd ----> SchedStart
            (EventType::End, EventType::Start) => ActivityType::BusyWaiting,
            // something ---> msgreceive
            (_, EventType::Received) => ActivityType::Waiting,
            // schedend -> remotesend, remote -> schedstart, remote -> remotesend
            (_, _) => ActivityType::Unknown, // @TODO
        };

        let operator_id = if prev.operator_id == record.operator_id {
            prev.operator_id
        } else {
            None
        };

        let traverse = if edge_type == ActivityType::Waiting {
            TraversalType::Block
        } else {
            TraversalType::Unbounded
        };

        PagEdge {
            source: PagNode::from(prev),
            destination: PagNode::from(record),
            edge_type,
            operator_id,
            traverse,
        }
    }

    // fn make_control_edges(&self) -> Collection<S, PagEdge, isize> {
    //     let sent = self
    //         .filter(|x| {
    //             x.activity_type == ActivityType::ControlMessage && x.event_type == EventType::Sent
    //         })
    //         .map(|x| ((x.local_worker, x.correlator_id, x.channel_id), x));

    //     let received = self
    //         .filter(|x| {
    //             x.activity_type == ActivityType::ControlMessage
    //                 && x.event_type == EventType::Received
    //         })
    //         .map(|x| ((x.remote_worker.unwrap(), x.correlator_id, x.channel_id), x));

    //     sent.join(&received)
    //         .map(|(_key, (from, to))| PagEdge {
    //             source: PagNode::from(&from),
    //             destination: PagNode::from(&to),
    //             edge_type: ActivityType::ControlMessage,
    //             operator_id: None,
    //             traverse: TraversalType::Unbounded,
    //         })
    // }

    // @TODO: use channel information to reposition data message in front of the operator it was intended for
    // fn make_data_edges(&self) -> Collection<S, PagEdge, isize> {
    //     let sent = self
    //         .filter(|x| x.activity_type == ActivityType::DataMessage && x.event_type == EventType::Sent)
    //         .map(|x| ((x.local_worker, x.remote_worker.unwrap(), x.correlator_id, x.channel_id), x));

    //     let received = self
    //         .filter(|x| x.activity_type == ActivityType::DataMessage && x.event_type == EventType::Received)
    //         .map(|x| ((x.remote_worker.unwrap(), x.local_worker, x.correlator_id, x.channel_id), x));

    //     sent.join(&received)
    //         .map(|(_key, (from, to))| PagEdge {
    //             source: PagNode::from(&from),
    //             destination: PagNode::from(&to),
    //             edge_type: ActivityType::DataMessage,
    //             operator_id: None, // could be used to store op info
    //             traverse: TraversalType::Unbounded
    //         })
    // }
}
