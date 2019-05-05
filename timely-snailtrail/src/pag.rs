//! Pag Construction
//! Uses LogRecord representation to create a PAG that contains local and remote edges

use std::{io::Read, time::Duration};

use differential_dataflow::{
    operators::{join::Join, reduce::Reduce},
    Collection,
};

use timely::dataflow::Scope;

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

    let local_edges = make_local_edges(&records);
    let control_edges = make_control_edges(&records);
    // @TODO: DataMessages
    // let data_edges = make_data_edges(&records);

    local_edges.concat(&control_edges)
    // .concat(&data_edges)
}

fn make_local_edges_reduce<S: Scope<Timestamp = Duration>>(
    records: &Collection<S, LogRecord, isize>,
) -> Collection<S, PagEdge, isize> {
    records
        .map(|x| (x.local_worker, x))
        .reduce(|_key, input, output| {
            let mut prev_record: Option<&LogRecord> = None;

            for (record, diff) in input {
                assert!(*diff == 1);

                if let Some(prev) = prev_record.clone() {
                    assert!(prev.local_worker == record.local_worker);
                    output.push((build_local_edge(prev, *record), *diff));
                    prev_record = Some(*record);
                } else {
                    prev_record = Some(*record);
                }
            }
        })
        .map(|(_key, x)| x)
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
fn make_control_edges<S: Scope<Timestamp = Duration>>(
    records: &Collection<S, LogRecord, isize>,
) -> Collection<S, PagEdge, isize> {
    let control_messages_send = records
        .filter(|x| {
            x.activity_type == ActivityType::ControlMessage && x.event_type == EventType::Sent
        })
        .map(|x| ((x.local_worker, x.correlator_id, x.channel_id), x));

    let control_messages_received = records
        .filter(|x| {
            x.activity_type == ActivityType::ControlMessage && x.event_type == EventType::Received
        })
        .map(|x| ((x.remote_worker.unwrap(), x.correlator_id, x.channel_id), x));

    control_messages_send
        .join(&control_messages_received)
        .map(|(_key, (from, to))| PagEdge {
            source: PagNode::from(&from),
            destination: PagNode::from(&to),
            edge_type: ActivityType::ControlMessage,
            operator_id: None,
            traverse: TraversalType::Unbounded,
        })
}
