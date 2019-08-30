//! Pag Construction
//! Uses LogRecord representation to create a PAG that contains local and remote edges

use std::collections::HashMap;
use std::{io::Read, time::Duration};
use std::cmp::Ordering;
use std::hash::Hash;

use timely::dataflow::{channels::pact::Exchange, operators::generic::operator::Operator, Scope};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::Stream;
use timely::dataflow::operators::filter::Filter;
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::operators::concat::Concat;
use timely::Data;

use st2_logformat::{ActivityType, EventType, LogRecord, OperatorId};
use ActivityType::{Busy, Waiting, Scheduling, Processing, Spinning, ControlMessage, DataMessage};
use EventType::{Sent, Received, Start, End};
use st2_logformat::pair::Pair;
use st2_timely::{connect::Replayer, create_lrs};

use abomonation::Abomonation;

use serde::Serialize;


/// A node in the PAG
#[derive(Abomonation, Clone, PartialEq, Hash, Eq, Copy, Serialize)]
pub struct PagNode {
    /// Timestamp of the event (also a unique identifier!)
    pub timestamp: st2_logformat::Timestamp,
    /// Unique ID of the worker the event belongs to
    pub worker_id: st2_logformat::Worker,
    /// Epoch of PagNode
    pub epoch: u64,
    /// seq_no of PagNode
    pub seq_no: u64,
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
            epoch: record.epoch,
            seq_no: record.seq_no,
        }
    }
}

impl std::fmt::Debug for PagNode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // write!(f, "{}|{:?}@w{} (s{})", self.epoch, self.timestamp, self.worker_id, self.seq_no)
        write!(f, "{},{},{},{}", self.epoch, self.timestamp.as_nanos(), self.worker_id, self.seq_no)
    }
}

/// Information on how to traverse an edge. This is used e.g. in critical
/// participation to decide whether an edge should be included in the critical
/// path calculation. A `Block`ed edge can't be traversed (e.g. waiting activities)
#[derive(Abomonation, Hash, Clone, Eq, Ord, PartialEq, PartialOrd, Debug, Serialize)]
pub enum TraversalType {
    /// Unclear traversal
    Undefined,
    /// No traversal possible
    Block,
    /// Traversal possible
    Unbounded,
}

/// An edge in the activity graph
#[derive(Abomonation, Clone, PartialEq, Hash, Eq, Serialize)]
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
    /// record count
    pub length: Option<usize>,
}

impl PagEdge {
    /// PagEdge's duration in ns.
    /// Due to clock skew, we can't give guarantees that `to.timestamp > from.timestamp`.
    /// We report a duration of 0 in the case that `to.timestamp < from.timestamp`.
    pub fn duration(&self) -> u128 {
        // @TODO: Due to clock skew, this is sometimes < 0
        let dst_ts = self.destination.timestamp.as_nanos();
        let src_ts = self.source.timestamp.as_nanos();

        if src_ts > dst_ts {
            0
        } else {
            dst_ts - src_ts
        }
    }
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

impl std::fmt::Debug for PagEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // write!(f, "{:?} -> {:?} | {:?} {:?}\t| oid: {:?}\t",
        //        self.source, self.destination,
        //        self.traverse, self.edge_type,
        //        self.operator_id)

        write!(f, "{:?},{:?},{:?},{:?},{:?}",
               self.source, self.destination,
               self.edge_type, self.operator_id, self.length)
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
    throttle: u64,
) -> Stream<S, (PagEdge, S::Timestamp, isize)> {
    create_lrs(scope, replayers, index, throttle)
        .construct_pag(index)
}

/// Dump PAG to file
pub trait DumpPAG<S: Scope<Timestamp = Pair<u64, Duration>>> {
    /// Dump PAG to file
    fn dump_pag(&self, index: usize) -> Stream<S, (PagEdge, S::Timestamp, isize)>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> DumpPAG<S> for Stream<S, (PagEdge, S::Timestamp, isize)> {
    fn dump_pag(&self, index: usize) -> Stream<S, (PagEdge, S::Timestamp, isize)> {
        if index == 0 {
            println!("from_epoch,from_timestamp,from_workerid,from_seqno,to_epoch,to_timestamp,to_workerid,to_seqno,edge_type,edge_operatorid");
        }

        self.inspect(|(x, _, _)| println!("{:?}", x))
    }
}

/// Operator that converts a Stream of LogRecords to a PAG
pub trait ConstructPAG<S: Scope<Timestamp = Pair<u64, Duration>>> {
    /// Builds a PAG from `LogRecord` by concatenating local edges, control edges
    /// and data edges.
    fn construct_pag(&self, index: usize) -> Stream<S, (PagEdge, S::Timestamp, isize)>;
    /// Takes `LogRecord`s and connects local edges (per epoch, per worker)
    fn make_local_edges(&self, index: usize) -> Stream<S, (PagEdge, S::Timestamp, isize)>;
    /// Helper to create a `PagEdge` from two `LogRecord`s
    fn build_local_edge(prev: &LogRecord, record: &LogRecord) -> PagEdge;
    /// Takes `LogRecord`s and connects remote edges (per epoch, across workers)
    fn make_remote_edges(&self) -> Stream<S, (PagEdge, S::Timestamp, isize)>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> ConstructPAG<S> for Stream<S, LogRecord> {
    fn construct_pag(&self, index: usize) -> Stream<S, (PagEdge, S::Timestamp, isize)> {
        self.make_local_edges(index)
            .concat(&self.make_remote_edges())
    }

    fn make_local_edges(&self, index: usize) -> Stream<S, (PagEdge, S::Timestamp, isize)> {
        // A differential join looks nicer and doesn't depend on order, but is
        // ~7x slower. Getting its semantics right is also tricky, since some `seq_no`s
        // are cut up due to `peel_ops`.

        let mut vector = Vec::new();
        let mut buffer: HashMap<usize, LogRecord> = HashMap::new();

        self.unary_frontier(Pipeline, "Local Edges", move |_, _| { move |input, output| {
            input.for_each(|cap, data| {
                data.swap(&mut vector);
                for lr in vector.drain(..) {
                    let local_worker = lr.local_worker as usize;

                    if let Some(prev_lr) = buffer.get(&local_worker) {
                        // we've seen a lr from this local_worker before

                        assert!(lr.epoch >= prev_lr.epoch, format!("w{}: {:?} should happen before {:?}", index, prev_lr.epoch, lr.epoch));
                        assert!(lr.timestamp >= prev_lr.timestamp, format!("w{}: {:?} should happen before {:?}", index, prev_lr, lr));

                        if prev_lr.epoch == lr.epoch {
                            // only join lrs within an epoch
                            output.session(&cap).give((Self::build_local_edge(&prev_lr, &lr), cap.time().clone(), 1));
                        }
                    }

                    buffer.insert(local_worker, lr);
                }
            });

            trace!("made local edges");
        }})
    }

    fn build_local_edge(prev: &LogRecord, record: &LogRecord) -> PagEdge {
        // Rules for a well-formatted PAG

        // @TODO: In some cases, this assertion doesn't hold and a DataMessage is sent before the
        // operator it belongs to has started. In this case, we probably misreport the operator type
        // (Spinning instead of Processing) and its length (0 instead of the DataMessage's contents).
        // I think that this has to do with differential arrangements, so it's worth thinking about
        // them and this bug in tandem.
        // No data messages outside a Schedules event
        // assert!((record.event_type != Start) || prev.activity_type != DataMessage, format!("{:?}, {:?}", prev, record));

        assert!((prev.event_type != End) || record.activity_type != DataMessage);

        // No control messages within a Schedules event
        assert!((record.event_type != End) || prev.activity_type != ControlMessage);
        assert!((prev.event_type != Start) || record.activity_type != ControlMessage);
        // A message with length != None is always either a SchedEnd or a remote data recv
        assert!(record.length.is_none() || record.activity_type == DataMessage || record.event_type == End);
        assert!(prev.length.is_none() || prev.activity_type == DataMessage || prev.event_type == End);
        // local edges are local and provided in order
        assert!(record.timestamp > prev.timestamp && record.local_worker == prev.local_worker);

        // @TODO: make configurable
        // @TODO: time-based vs. interpreted (cf. screenshot)
        let waiting_or_busy = if (record.timestamp.as_nanos() - prev.timestamp.as_nanos()) > 15_000 {
            Waiting
        } else {
            Busy
        };

        let processing_or_spinning = if record.length.is_some() {
            Processing
        } else {
            Spinning
        };

        let p = prev.event_type;
        let r = record.event_type;
        let edge_type = match (prev.activity_type, record.activity_type) {
            // See TODO above.
            // (ControlMessage, DataMessage) => {println!("{:?}, {:?}", prev, record); unreachable!()},
            // (DataMessage, ControlMessage) => {println!("{:?}, {:?}", prev, record); unreachable!()},

            (Scheduling, Scheduling) if (p == Start && r == End) => processing_or_spinning,
            (Scheduling, Scheduling) if (p == End && r == Start) => waiting_or_busy,
            (ControlMessage, _) => waiting_or_busy,
            (_, ControlMessage) => waiting_or_busy,
            (DataMessage, _) => Processing,
            (_, DataMessage) => Processing,

            _ => panic!("{:?}, {:?}", prev, record)
        };

        let operator_id = if prev.event_type != End && record.event_type != Start {
            prev.operator_id
        } else {
            None
        };

        let traverse = if edge_type == Waiting {
            TraversalType::Block
        } else {
            TraversalType::Unbounded
        };

        // only keep lengths for schedule edges
        let length = if record.activity_type == Scheduling {
            record.length
        } else {
            None
        };

        PagEdge {
            source: PagNode::from(prev),
            destination: PagNode::from(record),
            edge_type,
            operator_id,
            traverse,
            length,
        }
    }

    fn make_remote_edges(&self) -> Stream<S, (PagEdge, S::Timestamp, isize)> {
        let narrowed = self.filter(|x| x.activity_type == ControlMessage || x.activity_type == DataMessage);

        let sent = narrowed
            .filter(|x| x.event_type == Sent)
            .map(|x| ((Some(x.local_worker), x.remote_worker, x.correlator_id, x.channel_id), x));

        let received = narrowed
            .filter(|x| x.event_type == Received)
            .map(|x| {
                // ControlMessage sends are broadcasts; they have no receiver.
                // x.remote_worker is None for them, so here, it has to be, too.
                let receiver = if x.activity_type == ControlMessage {
                    None
                } else {
                    Some(x.local_worker)
                };
                ((x.remote_worker, receiver, x.correlator_id, x.channel_id), x)
            });

        sent.join_edges(&received)
            .map(|(from, to, t)| {
                assert!(to.local_worker != from.local_worker);
                (PagEdge {
                source: PagNode::from(&from),
                destination: PagNode::from(&to),
                edge_type: from.activity_type,
                operator_id: None,
                traverse: TraversalType::Unbounded,
                length: from.length,
                }, t, 1)})
    }
}

trait JoinEdges<S: Scope<Timestamp = Pair<u64, Duration>>, D> where D: Data + Hash + Eq + Abomonation + Send + Sync {
    fn join_edges(&self, other: &Stream<S, (D, LogRecord)>) -> Stream<S, (LogRecord, LogRecord, S::Timestamp)>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>, D> JoinEdges<S, D>
    for Stream<S, (D, LogRecord)>
where D: Data + Hash + Eq + Abomonation + Send + Sync + std::fmt::Debug
{
    fn join_edges(&self, other: &Stream<S, (D, LogRecord)>) -> Stream<S, (LogRecord, LogRecord, S::Timestamp)> {
        // exchange by epoch doesn't make sense for low epoch_in_flight counts
        // let exchange = Exchange::new(|(_, x): &(_, LogRecord)| x.epoch);
        // let exchange2 = Exchange::new(|(_, x): &(_, LogRecord)| x.epoch);

        // exchange by local / remote worker doesn't make sense for STw > TCw
        // let exchange = Exchange::new(|(_, x): &(_, LogRecord)| x.local_worker);
        // let exchange2 = Exchange::new(|(_, x): &(_, LogRecord)| x.remote_worker.unwrap());

        // @TODO: exchange by correlator_id works, but is surprisingly slow
        let exchange = Exchange::new(|(_, x): &(_, LogRecord)| x.correlator_id.expect("no corr id"));
        let exchange2 = Exchange::new(|(_, x): &(_, LogRecord)| x.correlator_id.expect("no corr id"));

        // @TODO: in this join implementation, state continually grows.
        // To fix this, check frontier and remove all state that is from older epochs
        // (cross-epochs join shouldn't happen anyways)
        self.binary(&other, exchange, exchange2, "HashJoin", |_capability, _info| {
            let mut map1 = HashMap::new();
            let mut map2 = HashMap::<_, Vec<LogRecord>>::new();

            let mut vector1 = Vec::new();
            let mut vector2 = Vec::new();

            move |input1, input2, output| {
                // Drain first input, check second map, update first map.
                input1.for_each(|cap, data| {
                    data.swap(&mut vector1);
                    let mut session = output.session(&cap);
                    for (key, val1) in vector1.drain(..) {
                        if let Some(values) = map2.get(&key) {
                            for val2 in values.iter() {
                                // assert!(val1.epoch == val2.epoch);
                                if val1.epoch == val2.epoch {
                                    session.give((val1.clone(), val2.clone(), cap.time().clone()));
                                }
                            }
                        }

                        map1.entry(key).or_insert(Vec::new()).push(val1);
                    }
                });

                input2.for_each(|cap, data| {
                    data.swap(&mut vector2);
                    let mut session = output.session(&cap);
                    for (key, val2) in vector2.drain(..) {
                        if let Some(values) = map1.get(&key) {
                            for val1 in values.iter() {
                                // assert!(val1.epoch == val2.epoch);
                                if val1.epoch == val2.epoch {
                                    session.give((val1.clone(), val2.clone(), cap.time().clone()));
                                }
                            }
                        }

                        map2.entry(key).or_insert(Vec::new()).push(val2);
                    }
                });
            }
        })
    }
}
