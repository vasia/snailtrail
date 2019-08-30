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

use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::Collection;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::reduce::Reduce;

use std::time::Duration;
use std::collections::BTreeSet;

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
                .khops(2)
                .khops_summary()
                .inspect(move |(x, t, diff)| if *diff > 0 { println!("k-hops summary for epoch {:?}: {:?}", t.first - 1, x); });
        });
    })
        .map_err(|x| STError(format!("error in the timely computation: {}", x)))?;

    Ok(())
}


/// Summarize khops edges weighted and unweighted for each epoch.
pub trait KHopsSummary<S: Scope<Timestamp = Pair<u64, Duration>>> {
    /// Summarize khops edges weighted and unweighted for each epoch
    /// by activity type and worker_id.
    fn khops_summary(&self) -> Collection<S, ((ActivityType, u64), (isize, isize)), isize>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> KHopsSummary<S> for Collection<S, (PagEdge, u32), isize>{
    fn khops_summary(&self) -> Collection<S, ((ActivityType, u64), (isize, isize)), isize> {
        self.map(|(edge, weight)| ((edge.edge_type, edge.source.worker_id), (edge, weight)))
            .reduce(|_key, input, output| {
                let summary = input.iter().fold((0, 0), |acc, ((_edge, weight), diff)| {
                    (acc.0 + *diff, acc.1 + *diff * *weight as isize)
                });
                output.push((summary, 1))
            })
    }
}


/// Run graph algorithms on provided `Stream`.
pub trait KHops<S: Scope<Timestamp = Pair<u64, Duration>>> {
    /// Run khops algorithm on provided `Stream`.
    /// Returns a stream of reachable edges and the steps necessary to reach them.
    fn khops(&self, steps: u32) -> Collection<S, (PagEdge, u32), isize>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> KHops<S> for Stream<S, (PagEdge, S::Timestamp, isize)>{
    fn khops(&self, steps: u32) -> Collection<S, (PagEdge, u32), isize>{
        let epochized = self
            .delay_batch(|time| Pair::new(time.first + 1, Default::default()))
            .map(|(x, t, diff)| (x, Pair::new(t.first + 1, Default::default()), diff));

        let waiting = epochized
            .filter(|(x, _t, _diff)| x.edge_type == ActivityType::Waiting)
            .as_collection()
            .map(|x| (x.destination.timestamp, (x, 0)));

        let all: Arranged<_, TraceAgent<OrdValSpine<Duration, _, Pair<u64, Duration>, isize>>> = epochized
            .as_collection()
            .map(|x| (x.destination.timestamp, x))
            .arrange_by_key();

        // processing end events that might also be data ends
        let data_ends = epochized.unary_frontier(Pipeline, "DataEnds", move |_, _| {
            let mut vector = Vec::new();
            let mut waiting_buffer: BTreeSet<usize> = BTreeSet::new();
            move |input, output| {
                input.for_each(|cap, data| {
                    data.swap(&mut vector);
                    for (edge, t, diff) in vector.drain(..) {
                        let wid = edge.destination.worker_id as usize;

                        if waiting_buffer.contains(&wid) {
                            if edge.edge_type == ActivityType::Processing {
                                output.session(&cap).give(((edge.destination.timestamp, (edge, 0)), t, diff));
                            }
                            waiting_buffer.remove(&wid);
                        } else if edge.edge_type == ActivityType::Waiting {
                            waiting_buffer.insert(wid);
                        }
                    }
                });
            }})
            .as_collection();

        let mut streams = (0 .. steps).fold(vec![data_ends, waiting], |mut acc, n| {
            let last = acc.pop().expect("?");
            let last_a = last.arrange_by_key();
            let new = if n != 0 {
                last_a.join_core(&all, move |_key, _old, new| Some((new.source.timestamp, (new.clone(), steps - n))))
            } else {
                let data_ends = acc.pop().expect("?");
                let data_ends_a = data_ends.arrange_by_key();
                let data_join = data_ends_a
                    .join_core(&all, move |_key, _old, new| Some((new.source.timestamp, (new.clone(), steps - n))))
                    .filter(|(_, (x, _))| x.edge_type == ActivityType::DataMessage);

                let waiting_joined = last_a.join_core(&all, move |_key, _old, new| Some((new.source.timestamp, (new.clone(), steps - n))));
                let no_waiting = waiting_joined.filter(|(_, (x, _))| x.edge_type != ActivityType::Waiting);
                let only_waiting = waiting_joined.filter(|(_, (x, _))| x.edge_type == ActivityType::Waiting);
                let post_waiting = only_waiting.join_core(&all, move |_key, _old, new| Some((new.source.timestamp, (new.clone(), steps - n))));

                no_waiting.concat(&post_waiting).concat(&data_join)
            };

            acc.push(last);
            acc.push(new);
            acc
        });

        // remove `waiting`
        streams.remove(0);
        streams
            .pop()
            .expect("?")
            .concatenate(streams)
            .map(|(_key, new)| new)
    }
}
