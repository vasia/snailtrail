use crate::pag;
use crate::pag::PagEdge;
use crate::STError;

use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::dataflow::operators::filter::Filter;
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::delay::Delay;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::concat::Concat;
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::aggregation::aggregate::Aggregate;

use std::time::Duration;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

use st2_logformat::pair::Pair;
use st2_logformat::ActivityType;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;



/// Runs graph algorithms on ST2.
pub fn run(
    timely_configuration: timely::Configuration,
    replay_source: ReplaySource) -> Result<(), STError> {

    timely::execute(timely_configuration, move |worker| {
        let index = worker.index();

        // read replayers from file (offline) or TCP stream (online)
        let readers = connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");

        worker.dataflow(|scope| {
            let pag: Stream<_, (PagEdge, Pair<u64, Duration>, isize)>  = pag::create_pag(scope, readers, index, 1);

            pag
                .khops()
                .khops_summary()
                .inspect_time(|t, x| println!("{}: {:?}", t.first, x));
        });
    })
        .map_err(|x| STError(format!("error in the timely computation: {}", x)))?;

    Ok(())
}


/// Summarize khops edges weighted and unweighted for each epoch.
pub trait KHopsSummary<S: Scope<Timestamp = Pair<u64, Duration>>> {
    /// Summarize khops edges weighted and unweighted for each epoch
    /// by activity type and worker_id.
    fn khops_summary(&self) -> Stream<S, ((ActivityType, u64), (u64, u64))>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> KHopsSummary<S> for Stream<S, (PagEdge, u64)>{
    fn khops_summary(&self) -> Stream<S, ((ActivityType, u64), (u64, u64))> {
        self.map(|(edge, weight)| ((edge.edge_type, edge.source.worker_id), (edge, weight)))
            .aggregate::<_,(u64, u64),_,_,_>(
                |_key, (_edge, weight), acc| {
                    *acc = (acc.0 + 1, acc.1 + weight);
                },
                |key, acc| (key, acc),
                |key| calculate_hash(key))
    }
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

/// Run khops on provided `Stream`.
pub trait KHops<S: Scope<Timestamp = Pair<u64, Duration>>> {
    /// Run khops algorithm on provided `Stream`.
    /// Currently, this is hardcoded to a 2-hop pattern.
    /// Returns a stream of reachable edges and the steps necessary to reach them.
    fn khops(&self) -> Stream<S, (PagEdge, u64)>;
}


impl<S: Scope<Timestamp = Pair<u64, Duration>>> KHops<S> for Stream<S, (PagEdge, S::Timestamp, isize)>{
    fn khops(&self) -> Stream<S, (PagEdge, u64)> {
        let epochized = self
            .map(|(x, _, _)| (x.destination.timestamp, (x, 0 as u64)))
            .delay_batch(|time| Pair::new(time.first + 1, Default::default()));

        // processing end events that might also be data ends
        let step_0_processing = epochized.unary_frontier(Pipeline, "DataEnds", move |_, _| {
            let mut vector = Vec::new();
            let mut waiting_buffer: BTreeSet<usize> = BTreeSet::new();
            move |input, output| {
                input.for_each(|cap, data| {
                    data.swap(&mut vector);
                    for (dest, (edge, w)) in vector.drain(..) {
                        let wid = edge.destination.worker_id as usize;

                        if waiting_buffer.contains(&wid) {
                            if edge.edge_type == ActivityType::Processing {
                                output.session(&cap).give((dest, (edge, w)));
                            }
                            waiting_buffer.remove(&wid);
                        } else if edge.edge_type == ActivityType::Waiting {
                            waiting_buffer.insert(wid);
                        }
                    }
                });
            }});

        // hop if data message
        let step_1_processing = step_0_processing.hop(&epochized.filter(|(_, (x, _))| x.edge_type == ActivityType::DataMessage));

        let step_0_waiting = epochized.filter(|(_, (x, _))| x.edge_type == ActivityType::Waiting);
        // first hop shouldn't happen worker-locally.
        let step_1_no_waiting = step_0_waiting.hop(&epochized.filter(|(_, (x, _))| x.edge_type != ActivityType::Waiting));

        let step_1 = step_1_no_waiting.concat(&step_1_processing);

        let step_2 = step_1.hop(&epochized);

        // @TODO: Optionally, only weigh from the second hop onwards
        // step_1.map(|(a, (x, _))| (a, (x, 0))).concat(&step_2).map(|(_, x)| x)
        step_1.concat(&step_2).map(|(_, x)| x)
    }
}


trait KHop<S: Scope<Timestamp = Pair<u64, Duration>>>{
    fn hop(&self, other: &Stream<S, (Duration, (PagEdge, u64))>) -> Stream<S, (Duration, (PagEdge, u64))>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> KHop<S>
    for Stream<S, (Duration, (PagEdge, u64))>
{
    fn hop(&self, other: &Stream<S, (Duration, (PagEdge, u64))>) -> Stream<S, (Duration, (PagEdge, u64))> {
        use std::convert::TryInto;
        let exchange = Exchange::new(|(x, _): &(Duration, _)| x.as_nanos().try_into().unwrap());
        let exchange2 = Exchange::new(|(x, _): &(Duration, _)| x.as_nanos().try_into().unwrap());

        // @TODO: in this join implementation, state continually grows.
        // To fix this, check frontier and remove all state that is from older epochs
        // (cross-epochs join shouldn't happen anyways)
        self.binary(&other, exchange, exchange2, "HashJoin", |_capability, _info| {
            let mut map1 = HashMap::<_, Vec<PagEdge>>::new();
            let mut map2 = HashMap::<_, Vec<PagEdge>>::new();

            let mut vector1 = Vec::new();
            let mut vector2 = Vec::new();

            move |input1, input2, output| {
                // Drain first input, check second map, update first map.
                input1.for_each(|cap, data| {
                    data.swap(&mut vector1);
                    let mut session = output.session(&cap);
                    for (key, (val1, _)) in vector1.drain(..) {
                        if let Some(values) = map2.get(&key) {
                            for val2 in values.iter() {
                                session.give((val2.source.timestamp, (val2.clone(), val2.duration())));
                            }
                        }

                        // weigh with activity duration
                        map1.entry(key).or_insert(Vec::new()).push(val1);
                    }
                });

                input2.for_each(|cap, data| {
                    data.swap(&mut vector2);
                    let mut session = output.session(&cap);
                    for (key, (val2, _)) in vector2.drain(..) {
                        if let Some(values) = map1.get(&key) {
                            for _val1 in values.iter() {
                                session.give((val2.source.timestamp, (val2.clone(), val2.duration())));
                            }
                        }

                        map2.entry(key).or_insert(Vec::new()).push(val2);
                    }
                });
            }
        })
    }
}
