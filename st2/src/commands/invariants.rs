use crate::pag::PagEdge;
use crate::pag;
use crate::STError;
use crate::pag::PagNode;

use timely::dataflow::Stream;
use timely::dataflow::Scope;
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::delay::Delay;
use timely::dataflow::operators::aggregation::aggregate::Aggregate;
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::filter::Filter;

use std::time::Duration;

use st2_logformat::pair::Pair;
use st2_logformat::ActivityType;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;


/// Checks invariants on the log traces provided by `replay_source`.
pub fn run(timely_configuration: timely::Configuration,
           replay_source: ReplaySource,
           temporal_epoch: Option<u64>,
           temporal_operator: Option<u64>,
           temporal_message: Option<u64>,
           progress_max: Option<u64>) -> Result<(), STError> {

    timely::execute(timely_configuration, move |worker| {
        let index = worker.index();
        let peers = worker.peers();

        // read replayers from file (offline) or TCP stream (online)
        let readers = connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");

        worker.dataflow(|scope| {
            let pag: Stream<_, (PagEdge, Pair<u64, Duration>, isize)>  = pag::create_pag(scope, readers, index, 1);

            pag.some_progress(peers)
                .inspect_time(move |t, x| if x.1 < (peers as u64 - 1) {
                    println!("Progress Issue: w{}@e{} Sent progress to {} of {} other peers", x.0, t.first - 1, x.1, peers - 1)
                });

            if let Some(progress_max) = progress_max {
                let max = Duration::from_millis(progress_max);
                pag.max_progress(max)
                    .inspect(move |(x, y)| println!("Progress Issue: No progress message sent by w{} since {:?}. Maximum allowed is {:?}.", x.worker_id, (y.timestamp - x.timestamp), max));
            }

            if let Some(temporal_epoch) = temporal_epoch {
                let max = Duration::from_millis(temporal_epoch);
                pag.max_epoch(max)
                    .inspect(move |(x, y)|
                             println!("Temporal Issue: Epoch {} ran from {:?} to {:?}, taking {:?}. Maximum allowed is {:?}.",
                                      x.epoch, x.timestamp, y.timestamp, (y.timestamp - x.timestamp), max));
            }

            if let Some(temporal_operator) = temporal_operator {
                let max = Duration::from_millis(temporal_operator);
                pag.max_operator(max)
                    .inspect(move |(first_edge, last_edge)| {
                        println!("Temporal Issue: Operator {} in w{}@e{} ({:?}, {} records processed) ran from {:?} to {:?}, taking {:?}. \
                                  Maximum allowed is {:?}.",
                                 first_edge.operator_id.expect("not an operator?"),
                                 first_edge.source.worker_id,
                                 first_edge.source.epoch,
                                 first_edge.edge_type,
                                 last_edge.length.unwrap_or(0),
                                 first_edge.source.timestamp,
                                 last_edge.destination.timestamp,
                                 (last_edge.destination.timestamp - first_edge.source.timestamp),
                                 max)
                    });
            }

            if let Some(temporal_message) = temporal_message {
                let max = Duration::from_millis(temporal_message);
                pag.max_message(max)
                    .inspect(move |edge| {
                        println!("Temporal Issue: {:?} (payload: {:?}) in e{}, w{} to w{} ran from {:?} to {:?}, taking {:?}. \
                                  Maximum allowed is {:?}.",
                                 edge.edge_type,
                                 edge.length,
                                 edge.source.epoch,
                                 edge.source.worker_id,
                                 edge.destination.worker_id,
                                 edge.source.timestamp,
                                 edge.destination.timestamp,
                                 (edge.destination.timestamp - edge.source.timestamp),
                                 max)
                    });
            }
        });
    }).map_err(|x| STError(format!("error in the timely computation: {}", x)))?;

    Ok(())
}


/// Check invariants on provided `Stream`.
pub trait Invariants<S: Scope<Timestamp = Pair<u64, Duration>>> {
    /// Ensure the reference computation is making progress.
    /// Every worker should send at least one progress message per epoch
    /// to each other peer.
    /// Returns `(worker_id, count)` pairs where `count` of progress
    /// messages per epoch is smaller than `peers`.
    fn some_progress(&self, peers: usize) -> Stream<S, (u64, u64)>;

    /// Ensure that we observe a progress message at least every `max` duration.
    fn max_progress(&self, max: Duration) -> Stream<S, (PagNode, PagNode)>;

    /// Ensure that no round of input of the source computation takes
    /// longer than the provided duration.
    fn max_epoch(&self, max: Duration) -> Stream<S, (PagNode, PagNode)>;

    /// Ensure that no operator of the source computation takes
    /// longer than the provided duration.
    /// Outputs violating operator as first and last edge.
    fn max_operator(&self, max: Duration) -> Stream<S, (PagEdge, PagEdge)>;

    /// Ensure that no message of the source computation takes
    /// longer than the provided duration.
    /// Outputs violating message.
    fn max_message(&self, max: Duration) -> Stream<S, PagEdge>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> Invariants<S> for Stream<S, (PagEdge, S::Timestamp, isize)> {
    fn some_progress(&self, peers: usize) -> Stream<S, (u64, u64)> {
        self
            .delay_batch(|time| Pair::new(time.first + 1, Default::default()))
            .map(|(edge, _t, _diff)| (edge.source.worker_id, edge))
            .aggregate::<_,u64,_,_,_>(
                |_key, edge, acc| {
                    if edge.edge_type == ActivityType::ControlMessage {
                        *acc += 1;
                    }
                },
                |key, acc| (key, acc),
                |key| *key)
            .filter(move |x| x.1 < (peers as u64 - 1))
    }

    fn max_progress(&self, max: Duration) -> Stream<S, (PagNode, PagNode)> {
        self
            .unary(Pipeline, "SlowProgress", move |_, _| {
                let mut vector = Vec::new();
                let mut last_progress = std::collections::HashMap::new();

                // @TODO: not sure whether this is always in order. If it isn't,
                // results aren't correctly reported.
                move |input, output| {
                    input.for_each(|cap, data| {
                        data.swap(&mut vector);

                        for (edge, _t, _diff) in vector.drain(..) {
                            if last_progress.get(&edge.source.worker_id).is_none() {
                                last_progress.insert(edge.source.worker_id, (edge.source, 1));
                            } else {
                                let (last_node, multiplier) = last_progress.get(&edge.source.worker_id).expect("always should have some min");

                                if edge.source.timestamp > last_node.timestamp && (edge.source.timestamp - last_node.timestamp > (max * *multiplier)) {
                                    output.session(&cap).give((last_node.clone(), edge.source));

                                    // increase multiplier by 1
                                    let to_insert = (last_node.clone(), multiplier + 1);
                                    last_progress.insert(edge.source.worker_id, to_insert);
                                }

                                if edge.edge_type == ActivityType::ControlMessage {
                                    last_progress.insert(edge.source.worker_id, (edge.source, 1));
                                }
                            }
                        }
                    })
                }
            })
    }

    fn max_epoch(&self, max: Duration) -> Stream<S, (PagNode, PagNode)> {
        self
            .delay_batch(|time| Pair::new(time.first + 1, Default::default()))
            // exchange by epoch to avoid worker bottleneck
            .map(|(edge, _t, _diff)| (edge.source.epoch, edge))
            .aggregate::<_,(PagNode, PagNode),_,_,_>(
                |_key, edge, acc| {
                    assert!(edge.source.timestamp != Default::default());
                    let new_smallest = if edge.source.timestamp < acc.0.timestamp || acc.0 == Default::default() {
                        edge.source
                    } else {
                        acc.0
                    };

                    let new_largest = if edge.destination.timestamp > acc.1.timestamp {
                        edge.destination
                    } else {
                        acc.1
                    };

                    *acc = (new_smallest, new_largest);
                },
                |_key, acc| (acc.0, acc.1),
                |key| *key)
            .filter(move |(from, to)| (to.timestamp - from.timestamp) > max)
    }

    fn max_operator(&self, max: Duration) -> Stream<S, (PagEdge, PagEdge)> {
        self
            .filter(|(edge, _t, _diff)| edge.edge_type == ActivityType::Processing || edge.edge_type == ActivityType::Spinning)
            .unary(Pipeline, "FirstLastOperator", move |_, _| {
                let mut vector = Vec::new();
                let mut first_edge = None;

                move |input, output| {
                    input.for_each(|cap, data| {
                        data.swap(&mut vector);

                        for (edge, _t, _diff) in vector.drain(..) {
                            if first_edge.is_none() {
                                // single-edge activities
                                if edge.edge_type == ActivityType::Spinning || edge.length.is_some() {
                                    output.session(&cap).give((edge.clone(), edge));
                                } else {
                                    first_edge = Some(edge);
                                }
                            } else {
                                // closing edge
                                if edge.length.is_some() {
                                    let first = std::mem::replace(&mut first_edge, None).expect("no first edge?");
                                    assert!(first.source.worker_id == edge.source.worker_id);
                                    output.session(&cap).give((first, edge));
                                }
                            }
                        }
                    })
                }
            })
            .filter(move |(first_edge, last_edge)| last_edge.destination.timestamp - first_edge.source.timestamp > max)
    }

    fn max_message(&self, max: Duration) -> Stream<S, PagEdge> {
        self
            .map(|(edge, _t, _diff)| edge)
            .filter(|edge| edge.edge_type == ActivityType::ControlMessage || edge.edge_type == ActivityType::DataMessage)
            .filter(move |edge|
                    edge.destination.timestamp > edge.source.timestamp &&
                    (edge.destination.timestamp - edge.source.timestamp > max))
    }
}

// use st2_timely::connect::CompEvent;
// impl<S: Scope<Timestamp = Pair<u64, Duration>>> Invariants<S> for Stream<S, CompEvent>
// {
//     fn consistency(&self, index: usize, peers: usize) -> Stream<S, ()> {
//         // Check that all operators are scheduled in the order they were created in.
//         self
//             .unary(Pipeline, "CheckScheduling", move |_, _| {
//                 let mut vector = Vec::new();
//                 let mut operates = Vec::new();

//                 move |input, output| {
//                     input.for_each(|cap, data| {
//                         data.swap(&mut vector);
//                         for (epoch, seq_no, length, (t, wid, x)) in vector.drain(..) {
//                             match x {
//                                 TimelyEvent::Operates(e) => {
//                                     let mut addr = e.addr.clone();
//                                     addr.pop();
//                                     outer_operates.insert(addr);

//                                     ids_to_addrs.insert(e.id, e.addr);
//                                 }
//                                 TimelyEvent::Schedule(ref e) => {
//                                     assert!(cap.time() > &Pair::new(0, Default::default()));

//                                     let addr = ids_to_addrs.get(&e.id).expect("operates went wrong");
//                                     if !outer_operates.contains(addr) {
//                                         output.session(&cap).give((epoch, seq_no, length, (t, wid, x)));
//                                     }
//                                 }
//                                 _ => {
//                                     assert!(cap.time() > &Pair::new(0, Default::default()));

//                                     output.session(&cap).give((epoch, seq_no, length, (t, wid, x)));
//                                 }
//                             }
//                         }
//                     });
//                 }
//             })
//             .map(|_| ())
//     }
// }
