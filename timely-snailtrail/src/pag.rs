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
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::Stream;
use timely::dataflow::operators::filter::Filter;
use timely::dataflow::operators::map::Map;

use logformat::{ActivityType, EventType, LogRecord, OperatorId};
use logformat::pair::Pair;
use timely_adapter::{connect::Replayer, create_lrs};

use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::join::JoinCore;

/// A node in the PAG
#[derive(Abomonation, Clone, PartialEq, Hash, Eq, Copy)]
pub struct PagNode {
    /// Timestamp of the event (also a unique identifier!)
    pub timestamp: logformat::Timestamp,
    /// Unique ID of the worker the event belongs to
    pub worker_id: logformat::Worker,
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
        write!(f, "{}|{:?}@w{} (s{})", self.epoch, self.timestamp, self.worker_id, self.seq_no)
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
#[derive(Abomonation, Clone, PartialEq, Hash, Eq)]
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

impl std::fmt::Debug for PagEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?} -> {:?} | {:?} {:?}\t| oid: {:?}\t",
               self.source, self.destination,
               self.traverse, self.edge_type,
               self.operator_id)
    }
}

use timely::dataflow::operators::probe::Handle;

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
    probe: &mut Handle<S::Timestamp>,
) -> Collection<S, PagEdge, isize>
where S::Timestamp: Lattice + Ord {
    create_lrs(scope, replayers, index, throttle)
        .probe_with(probe)
        .construct_pag(index)
}

/// Dump PAG to file
pub trait DumpPAG<S: Scope<Timestamp = Pair<u64, Duration>>>
where S::Timestamp: Lattice + Ord {
    /// Dump PAG to file
    fn dump_pag(&self) -> Collection<S, PagEdge, isize>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> DumpPAG<S> for Collection<S, PagEdge, isize>
where S::Timestamp: Lattice + Ord {
    fn dump_pag(&self) -> Collection<S, PagEdge, isize> {
        self.inspect(|(x, _, _)| println!("[\"{:?}\" \"{:?}\"] \"{:?}\"", x.source, x.destination, x.edge_type))
    }
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
    /// Takes `LogRecord`s and connects control edges (per epoch, across workers)
    fn make_control_edges(&self) -> Collection<S, PagEdge, isize>;
    /// Takes `LogRecord`s and connects data edges (per epoch, across workers)
    fn make_data_edges(&self) -> Collection<S, PagEdge, isize>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> ConstructPAG<S> for Stream<S, (LogRecord, S::Timestamp, isize)>
where S::Timestamp: Lattice + Ord {
    fn construct_pag(&self, index: usize) -> Collection<S, PagEdge, isize> {
        // local edges:
            // preprocessing + map + debug map: ~27
        // preprocessing + map + arrange + debug map: ~33
        // preprocessing + map + arrange + join: ~40

        self
            .make_local_edges(index)
            .concat(&self.make_control_edges())
            .concat(&self.make_data_edges())
            // the following timely join on triangles 30K, 2w, 2lbf (134s) -> ST: 2 4 1
            // takes ~14s, while the same join implemented with differential takes ~16s
            // -> just use differential
        // let sent = self
        //     .filter(|(x, _t, _diff)| {
        //         x.activity_type == ActivityType::ControlMessage && x.event_type == EventType::Sent
        //     })
        //     .map(|(x, t, diff)| ((x.local_worker, x.correlator_id, x.channel_id), x));

        // let received = self
        //     .filter(|(x, _t, _diff)| {
        //         x.activity_type == ActivityType::ControlMessage
        //             && x.event_type == EventType::Received
        //     })
        //     .map(|(x, t, diff)| ((x.remote_worker.unwrap(), x.correlator_id, x.channel_id), x));

        // use std::collections::hash_map::DefaultHasher;
        // use std::hash::{Hash, Hasher};
        // let sent_exchange = Exchange::new(|(x, _): &((u64, Option<u64>, Option<u64>), LogRecord)| {
        //     let mut s = DefaultHasher::new();
        //     x.hash(&mut s);
        //     s.finish()
        // });
        // let received_exchange = Exchange::new(|(x, _): &((u64, Option<u64>, Option<u64>), LogRecord)| {
        //     let mut s = DefaultHasher::new();
        //     x.hash(&mut s);
        //     s.finish()
        // });

        // use timely::dataflow::operators::inspect::Inspect;
        // sent.binary(&received, sent_exchange, received_exchange, "HashJoin", |_capability, _info| {
        //     let mut map1 = HashMap::new();
        //     let mut map2 = HashMap::<(u64, Option<u64>, Option<u64>), Vec<LogRecord>>::new();

        //     let mut vector1 = Vec::new();
        //     let mut vector2 = Vec::new();

        //     move |input1, input2, output| {
        //         // Drain first input, check second map, update first map.
        //         input1.for_each(|time, data| {
        //             data.swap(&mut vector1);
        //             let mut session = output.session(&time);
        //             for (key, val1) in vector1.drain(..) {
        //                 if let Some(values) = map2.get(&key) {
        //                     for val2 in values.iter() {
        //                         session.give((val1.clone(), val2.clone()));
        //                     }
        //                 }

        //                 map1.entry(key).or_insert(Vec::new()).push(val1);
        //             }
        //         });

        //         // Drain second input, check first map, update second map.
        //         input2.for_each(|time, data| {
        //             data.swap(&mut vector2);
        //             let mut session = output.session(&time);
        //             for (key, val2) in vector2.drain(..) {
        //                 if let Some(values) = map1.get(&key) {
        //                     for val1 in values.iter() {
        //                         session.give((val1.clone(), val2.clone()));
        //                     }
        //                 }

        //                 map2.entry(key).or_insert(Vec::new()).push(val2);
        //             }
        //         });
        //     }
        // })
        //     .map(|(from, to)| (PagEdge {
        //     source: PagNode::from(&from),
        //     destination: PagNode::from(&to),
        //     edge_type: ActivityType::ControlMessage,
        //     operator_id: None,
        //     traverse: TraversalType::Unbounded,
        // }, Default::default(), 1))
        //     .as_collection()
    }

    fn make_local_edges(&self, index: usize) -> Collection<S, PagEdge, isize> {
        // A differential join looks nicer and doesn't depend on order, but is
        // ~7x slower. Getting its semantics right is also tricky, since some `seq_no`s
        // are cut up due to `peel_ops`.

        let mut vector = Vec::new();
        let mut buffer: HashMap<usize, (LogRecord, S::Timestamp)> = HashMap::new();
        let mut total = 0;

        self.unary_frontier(Pipeline, "Local Edges", move |_, _| { move |input, output| {
            let timer = std::time::Instant::now();

            input.for_each(|cap, data| {
                data.swap(&mut vector);
                for (lr, t, diff) in vector.drain(..) {
                    assert!(diff == 1);

                    let local_worker = lr.local_worker as usize;

                    if let Some((prev_lr, prev_t)) = buffer.get(&local_worker) {
                        // we've seen a lr from this local_worker before

                        assert!(prev_t.first == t.first || t.first > prev_t.first,
                                format!("w{}: {:?} should happen before {:?}", index, prev_t.first, t.first));

                        if prev_t.first == t.first {
                            // only join lrs within an epoch
                            output.session(&cap).give((Self::build_local_edge(&prev_lr, &lr), t.clone(), 1));
                        }
                    }

                    buffer.insert(local_worker, (lr, t));
                }
            });

            total += timer.elapsed().as_nanos();
            if input.frontier().is_empty() {
                println!("w{} local edges time: {}ms", index, total / 1_000_000);
            }
        }})
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
            .filter(|(x, _t, _diff)| {
                x.activity_type == ActivityType::ControlMessage && x.event_type == EventType::Sent
            })
            .map(|(x, t, diff)| (((x.local_worker, x.correlator_id, x.channel_id), x), t, diff))
            .as_collection()
            .arrange_by_key();

        let received = self
            .filter(|(x, _t, _diff)| {
                x.activity_type == ActivityType::ControlMessage
                    && x.event_type == EventType::Received
            })
            .map(|(x, t, diff)| (((x.remote_worker.unwrap(), x.correlator_id, x.channel_id), x), t, diff))
            .as_collection()
            .arrange_by_key();

        sent.join_core(&received, |_key, from, to| Some(PagEdge {
            source: PagNode::from(from),
            destination: PagNode::from(to),
            edge_type: ActivityType::ControlMessage,
            operator_id: None,
            traverse: TraversalType::Unbounded,
        }))
    }

    fn make_data_edges(&self) -> Collection<S, PagEdge, isize> {
        let sent = self
            .filter(|(x, _t, _diff)| x.activity_type == ActivityType::DataMessage && x.event_type == EventType::Sent)
            .map(|(x, t, diff)| (((x.local_worker, x.remote_worker.unwrap(), x.correlator_id, x.channel_id), x), t, diff))
            .as_collection()
            .arrange_by_key();

        let received = self
            .filter(|(x, _t, _diff)| x.activity_type == ActivityType::DataMessage && x.event_type == EventType::Received)
            .map(|(x, t, diff)| (((x.remote_worker.unwrap(), x.local_worker, x.correlator_id, x.channel_id), x), t, diff))
            .as_collection()
            .arrange_by_key();

        sent.join_core(&received, |_key, from, to| Some(PagEdge {
            source: PagNode::from(from),
            destination: PagNode::from(to),
            edge_type: ActivityType::DataMessage,
            operator_id: None, // @TODO could be used to store op info
            traverse: TraversalType::Unbounded
        }))
    }
}
