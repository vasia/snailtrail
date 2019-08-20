use crate::pag;
use crate::pag::PagEdge;
use crate::STError;

use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::dataflow::operators::filter::Filter;
use timely::Data;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::exchange::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::feedback::Feedback;
use timely::dataflow::operators::concat::Concat;
use timely::dataflow::operators::branch::BranchWhen;
use timely::dataflow::operators::feedback::ConnectLoop;
use timely::dataflow::operators::enterleave::Enter;
use timely::dataflow::operators::enterleave::Leave;
use timely::order::Product;
use timely::dataflow::operators::partition::Partition;

use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;

use std::time::Duration;
use std::hash::Hash;
use std::collections::HashMap;

use st2_logformat::pair::Pair;
use st2_logformat::ActivityType;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;

use abomonation::Abomonation;

/// Inspects a running SnailTrail computation, e.g. for benchmarking of SnailTrail itself.
pub fn run(
    timely_configuration: timely::Configuration,
    replay_source: ReplaySource) -> Result<(), STError> {

    timely::execute(timely_configuration, move |worker| {
        let index = worker.index();

        // read replayers from file (offline) or TCP stream (online)
        let readers = connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");

        worker.dataflow(|scope| {
            let pag: Stream<_, (PagEdge, Pair<u64, Duration>, isize)>  = pag::create_pag(scope, readers, index, 1);
            pag.algo(5);
        });
    })
        .map_err(|x| STError(format!("error in the timely computation: {}", x)))?;

    Ok(())
}


/// Run graph algorithms on provided `Stream`.
trait Algorithms<S: Scope<Timestamp = Pair<u64, Duration>>> {
    /// Run graph algorithms on provided `Stream`.
    fn algo(&self, steps: u32);
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> Algorithms<S> for Stream<S, (PagEdge, S::Timestamp, isize)> {
    fn algo(&self, steps: u32) {
        let waiting = self
            .filter(|(x, _t, _diff)| x.edge_type == ActivityType::Waiting)
            .as_collection()
            .map(|x| (x.source.timestamp, x));

        let all: Arranged<_, TraceAgent<OrdValSpine<Duration, _, Pair<u64, Duration>, isize>>> = self
            .as_collection()
            .map(|x| (x.destination.timestamp, x))
            .arrange_by_key();

        let mut streams = (0 .. steps).fold(vec![waiting], |mut acc, _n| {
            let last = acc.pop().expect("?");
            let last_a = last.arrange_by_key();

            let new = last_a.join_core(&all, |_key, _old, new| Some((new.source.timestamp, new.clone())));

            acc.push(last);
            acc.push(new);
            acc
        });

        // remove `waiting`
        streams.remove(0);
        let k_steps = streams
            .pop()
            .expect("?")
            .concatenate(streams)
            .map(|(_key, new)| new.edge_type);

        // TODO: auswerten
    }
}
