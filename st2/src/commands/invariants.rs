use crate::pag::PagEdge;
use crate::pag::ConstructPAG;
use crate::pag;
use crate::STError;

use timely::dataflow::operators::probe::Probe;
use timely::dataflow::Stream;
use timely::dataflow::Scope;
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::delay::Delay;
use timely::dataflow::operators::aggregation::aggregate::Aggregate;
use timely::dataflow::operators::inspect::Inspect;
use timely::logging::TimelyEvent;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::filter::Filter;

use std::time::Duration;

use st2_logformat::pair::Pair;
use st2_logformat::ActivityType;

use st2_timely::replay_throttled::ReplayThrottled;
use st2_timely::ConstructLRs;

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

        let mut probe = Handle::new();
        worker.dataflow(|scope| {
            let pag: Stream<_, (PagEdge, Pair<u64, Duration>, isize)>  = pag::create_pag(scope, readers, index, 1);
            pag.consistency(index, peers, &mut probe, temporal_epoch, temporal_operator, temporal_message, progress_max);
        });

        while !probe.done() { worker.step_or_park(None); };

        info!("w{} done", index);
    })
        .map_err(|x| STError(format!("error in the timely computation: {}", x)))?;

    Ok(())
}


/// Check invariants on provided `Stream`.
trait Invariants<S: Scope<Timestamp = Pair<u64, Duration>>> {
    // /// Checks correctness violations. These are patterns that
    // /// occur in healthy computations and violation of those
    // /// would mean that the computation or the underlying engine
    // /// or the logging infrastructure has a bug.
    // fn correctness(&self, index: usize) -> Stream<S, ()>;

    /// We require this type of pattern to appear consistently
    /// during execution. Missing this might indicate that the
    /// system is in faulty state.
    fn consistency(&self,
                   index: usize,
                   peers: usize,
                   probe: &mut Handle<S::Timestamp>,
                   temporal_epoch: Option<u64>,
                   temporal_operator: Option<u64>,
                   temporal_message: Option<u64>,
                   progress_max: Option<u64>);
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> Invariants<S> for Stream<S, (PagEdge, S::Timestamp, isize)> {
    fn consistency(&self,
                   index: usize,
                   peers: usize,
                   probe: &mut Handle<S::Timestamp>,
                   temporal_epoch: Option<u64>,
                   temporal_operator: Option<u64>,
                   temporal_message: Option<u64>,
                   progress_max: Option<u64>) {
        // Ensure that reference computation is making progress.
        // Every worker should send at least one progress message per epoch
        // to each other peer.
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
            .inspect_time(move |t, x| if x.1 < (peers as u64 - 1) {
                println!("Progress Issue: w{}@e{} Sent progress to {} of {} other peers", x.0, t.first - 1, x.1, peers - 1)
            })
            .probe_with(probe);

        // @TODO: This doesn't really help right now, since progress messages are normally sent event if they
        // don't contain _real_ progress and we have no way of spotting the difference.
        // @TODO: PR for timely to add frontier information to ProgressEvents
        if let Some(progress_max) = progress_max {
            let progress_max = Duration::from_millis(progress_max);

            self
                .unary(Pipeline, "SlowProgress", move |_, _| {
                    let mut vector = Vec::new();
                    let mut last_progress = std::collections::HashMap::new();

                    move |input, output| {
                        input.for_each(|cap, data| {
                            data.swap(&mut vector);

                            for (edge, _t, _diff) in vector.drain(..) {
                                if last_progress.get(&edge.source.worker_id).is_none() {
                                    last_progress.insert(edge.source.worker_id, (edge.source.timestamp, 1));
                                } else {
                                    let &(local_last_progress, multiplier) = last_progress.get(&edge.source.worker_id).expect("always should have some min");

                                    if edge.source.timestamp > local_last_progress && (edge.source.timestamp - local_last_progress > (progress_max * multiplier)) {
                                        last_progress.insert(edge.source.worker_id, (local_last_progress, multiplier + 1));
                                        output.session(&cap).give((edge.source.worker_id, (edge.source.timestamp - local_last_progress)));
                                    }

                                    if edge.edge_type == ActivityType::ControlMessage {
                                        last_progress.insert(edge.source.worker_id, (edge.source.timestamp, 1));
                                    }
                                }
                            }
                        })
                    }
                })
                .inspect(move |x| println!("Progress Issue: No progress message sent by w{} since {:?}. Maximum allowed is {:?}.", x.0, x.1, progress_max))
                .probe_with(probe);
        }

        // Ensure that no round of input of the source computation takes
        // longer than the provided duration in ms.
        if let Some(temporal_epoch) = temporal_epoch {
            let temporal_epoch = Duration::from_millis(temporal_epoch);
            self
                .delay_batch(|time| Pair::new(time.first + 1, Default::default()))
                // exchange by epoch to avoid worker bottleneck
                .map(|(edge, _t, _diff)| (edge.source.epoch, edge))
                .aggregate::<_,(Duration, Duration),_,_,_>(
                    |_key, edge, acc| {
                        assert!(edge.source.timestamp != Default::default());
                        let new_smallest = if edge.source.timestamp < acc.0 || acc.0 == Default::default() {
                            edge.source.timestamp
                        } else {
                            acc.0
                        };

                        let new_largest = if edge.destination.timestamp > acc.1 {
                            edge.destination.timestamp
                        } else {
                            acc.1
                        };

                        *acc = (new_smallest, new_largest);
                    },
                    |key, acc| (key, acc.0, acc.1),
                    |key| *key)
                .inspect(move |(epoch, from, to)| if (*to - *from) > temporal_epoch {
                    println!("Temporal Issue: Epoch {} ran from {:?} to {:?}, taking {:?}. Maximum allowed is {:?}.",
                             epoch, from, to, (*to - *from), temporal_epoch);
                })
                .probe_with(probe);
        }

        // Ensure that no operator of the source computation takes
        // longer than the provided duration in ms.
        if let Some(temporal_operator) = temporal_operator {
            let temporal_operator = Duration::from_millis(temporal_operator);
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
                .filter(move |(first_edge, last_edge)| last_edge.destination.timestamp - first_edge.source.timestamp > temporal_operator)
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
                             temporal_operator)
                })
                .probe_with(probe);
        }

        // Ensure that no message of the source computation takes
        // longer than the provided duration in ms.
        if let Some(temporal_message) = temporal_message {
            let temporal_message = Duration::from_millis(temporal_message);
            self
                .filter(|(edge, _t, _diff)| edge.edge_type == ActivityType::ControlMessage || edge.edge_type == ActivityType::DataMessage)
                .filter(move |(edge, _t, _diff)|
                        edge.destination.timestamp > edge.source.timestamp &&
                        (edge.destination.timestamp - edge.source.timestamp > temporal_message))
                .inspect(move |(edge, _t, _diff)| {
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
                             temporal_message)
                })
                .probe_with(probe);
        }
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
