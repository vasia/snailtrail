//! Pag Construction
//! Uses LogRecord representation to create a PAG that contains local and remote edges

use std::collections::HashMap;
use std::{io::Read, time::Duration};

use itertools::Itertools;

use differential_dataflow::{collection::AsCollection, operators::join::Join, Collection};

use timely::dataflow::{channels::pact::Exchange, operators::generic::operator::Operator, Scope};

use logformat::{ActivityType, EventType, LogRecord, OperatorId};
use timely_adapter::{connect::Replayer, make_log_records};

/// A node in the PAG
#[derive(Abomonation, Clone, Debug, PartialEq, Hash, Eq, Copy, Ord, PartialOrd)]
pub struct PagNode {
    /// Timestamp of the event (also a unique identifier!)
    pub timestamp: logformat::Timestamp,
    /// Unique ID of the worker the event belongs to
    pub worker_id: logformat::Worker,
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

// @TODO currently, this creates a new pag per epoch, but never removes the old one.
//       so state will continually grow and multiple pags exist side by side.
/// Creates a PAG (a Collection of `PagEdge`s, grouped by epoch) from the provided `Replayer`s.
/// To be called from within a timely computation.
pub fn create_pag<S: Scope<Timestamp = Duration>, R: 'static + Read>(
    scope: &mut S,
    replayers: Vec<Replayer<R>>,
) -> Collection<S, PagEdge, isize> {
    let records: Collection<_, LogRecord, isize> = make_log_records(scope, replayers);

    // @TODO: add an optional checking operator that tests individual logrecord timelines for sanity
    // e.g. sched start -> sched end, no interleave, start & end always belong to scheduling,
    // sent/received always to remote messages, we don't see message types that we can't handle yet,
    // t1 is always > t0, same count of sent and received remote messages for every epoch
    // same results regardless of worker count, received events always have a remote worker
    // matched remote events are (remote-count / 2), remote event count is always even

    records.construct_pag()
}

/// Operator that converts a Stream of LogRecords to a PAG
pub trait ConstructPAG<S: Scope<Timestamp = Duration>> {
    /// Builds a PAG from `LogRecord` by concatenating local edges, control edges
    /// and data edges.
    fn construct_pag(&self) -> Collection<S, PagEdge, isize>;
    /// Takes `LogRecord`s and connects local edges (per epoch, per worker)
    fn make_local_edges(&self) -> Collection<S, PagEdge, isize>;
    /// Helper to create a `PagEdge` from two `LogRecord`s
    fn build_local_edge(prev: &LogRecord, record: &LogRecord) -> PagEdge;
    /// Takes `LogRecord`s and connects control edges (per epoch, across workers)
    fn make_control_edges(&self) -> Collection<S, PagEdge, isize>;
    /// Takes `LogRecord`s and connects data edges (per epoch, across workers)
    fn make_data_edges(&self) -> Collection<S, PagEdge, isize>;
}

impl<S: Scope<Timestamp = Duration>> ConstructPAG<S> for Collection<S, LogRecord, isize> {
    fn construct_pag(&self) -> Collection<S, PagEdge, isize> {
        // self.make_data_edges()
        self.make_local_edges()
            // .concat(&self.make_control_edges())
            // .concat(&self.make_data_edges())
    }

    fn make_local_edges(&self) -> Collection<S, PagEdge, isize> {
        self.inner
            .unary_frontier(
                Exchange::new(|(record, _time, _diff): &(LogRecord, Duration, isize)| {
                    record.local_worker
                }),
                "local_edges",
                |_capability, _info| {
                    let mut buffer = Vec::new();

                    // stores the last matched record for every epoch & worker_id
                    let mut prev_record = HashMap::new();

                    move |input, output| {
                        input.for_each(|cap, data| {
                            data.swap(&mut buffer);

                            // group by local_worker: we're building out separate paths
                            for (_wid, group) in &buffer
                                .drain(..)
                                .group_by(|(record, _, _)| record.local_worker)
                            {
                                for (record, t, diff) in group {
                                    // @TODO: handle unconsolidated inputs gracefully
                                    assert!(diff == 1);

                                    if let Some(prev) = prev_record.get(&(t, record.local_worker)) {
                                        // delay to differential epochs: timely capability times and differential times
                                        // don't necessarily match up (e.g., timely might batch more aggressively)
                                        let delayed = cap.delayed(&t);
                                        let mut session = output.session(&delayed);
                                        session.give((
                                            Self::build_local_edge(prev, &record),
                                            t,
                                            diff,
                                        ));
                                        prev_record.insert((t, record.local_worker), record);
                                    } else {
                                        // the first node of an epoch
                                        trace!(
                                            "w{}'s first node of epoch {:?}: {:?}",
                                            record.local_worker,
                                            t,
                                            record
                                        );
                                        prev_record.insert((t, record.local_worker), record);
                                    }
                                }
                            }
                        });

                        // (optional cleanup since we don't give capabilities to state)
                        prev_record.retain(|(t, _), _| input.frontier().less_equal(t));
                    }
                },
            )
            .as_collection()
    }

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

    fn make_control_edges(&self) -> Collection<S, PagEdge, isize> {
        let sent = self
            .filter(|x| {
                x.activity_type == ActivityType::ControlMessage && x.event_type == EventType::Sent
            })
            .map(|x| ((x.local_worker, x.correlator_id, x.channel_id), x));

        let received = self
            .filter(|x| {
                x.activity_type == ActivityType::ControlMessage
                    && x.event_type == EventType::Received
            })
            .map(|x| ((x.remote_worker.unwrap(), x.correlator_id, x.channel_id), x));

        sent.join(&received)
            .map(|(_key, (from, to))| PagEdge {
                source: PagNode::from(&from),
                destination: PagNode::from(&to),
                edge_type: ActivityType::ControlMessage,
                operator_id: None,
                traverse: TraversalType::Unbounded,
            })
    }

    // @TODO: use channel information to reposition data message in front of the operator it was intended for
    fn make_data_edges(&self) -> Collection<S, PagEdge, isize> {
        let sent = self
            .filter(|x| x.activity_type == ActivityType::DataMessage && x.event_type == EventType::Sent)
            .map(|x| ((x.local_worker, x.remote_worker.unwrap(), x.correlator_id, x.channel_id), x));

        let received = self
            .filter(|x| x.activity_type == ActivityType::DataMessage && x.event_type == EventType::Received)
            .map(|x| ((x.remote_worker.unwrap(), x.local_worker, x.correlator_id, x.channel_id), x));

        sent.join(&received)
            .map(|(_key, (from, to))| PagEdge {
                source: PagNode::from(&from),
                destination: PagNode::from(&to),
                edge_type: ActivityType::DataMessage,
                operator_id: None, // could be used to store op info
                traverse: TraversalType::Unbounded
            })
    }
}
