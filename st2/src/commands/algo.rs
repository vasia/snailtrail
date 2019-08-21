use crate::pag;
use crate::pag::PagEdge;
use crate::STError;

use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::dataflow::operators::filter::Filter;
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::delay::Delay;

use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::Collection;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::reduce::Reduce;

use std::time::Duration;
use std::sync::mpsc;
use std::sync::{Mutex, Arc};

use st2_logformat::pair::Pair;
use st2_logformat::ActivityType;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;

use serde::Serialize;

#[derive(Serialize, Debug)]
/// Serialization type for ktop algo
pub enum KTop {
    /// all events (for highlighting)
    All((u128, u128)),
    /// aggregates (for analysis)
    Agg(((ActivityType, (isize, isize)), u64))
}

/// Inspects a running SnailTrail computation, e.g. for benchmarking of SnailTrail itself.
pub fn run(
    timely_configuration: timely::Configuration,
    replay_source: ReplaySource,
    pag_send: Arc<Mutex<mpsc::Sender<KTop>>>) -> Result<(), STError> {

    timely::execute(timely_configuration, move |worker| {
        let index = worker.index();

        let pag_send1 = pag_send.lock().expect("cannot lock pag_send").clone();
        let pag_send2 = pag_send.lock().expect("cannot lock pag_send").clone();

        // read replayers from file (offline) or TCP stream (online)
        let readers = connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");

        worker.dataflow(|scope| {
            let pag: Stream<_, (PagEdge, Pair<u64, Duration>, isize)>  = pag::create_pag(scope, readers, index, 1);
            let k_steps_raw = pag
                .delay_batch(|time| Pair::new(time.first + 1, Default::default()))
                .map(|(x, t, diff)| (x, Pair::new(t.first + 1, Default::default()), diff))
                .algo(5);

            // log to socket
            k_steps_raw.inspect(move |((_, (x, _)), _t, _diff)| {
                pag_send1
                    .send(KTop::All((x.source.timestamp.as_nanos(), x.destination.timestamp.as_nanos())))
                    .expect("ktop_all")
            });

            let k_steps = k_steps_raw.reduce(|_key, input, output| {
                let summary = input.iter().fold((0, 0), |acc, ((_edge, weight), diff)| {
                    (acc.0 + *diff, acc.1 + *diff * *weight as isize)
                });
                output.push((summary, 1))
            });

            // log to socket
            k_steps.inspect(move |(x, t, diff)| {
                if *diff > 0 {
                    pag_send2
                        .send(KTop::Agg((x.clone(), t.first)))
                        .expect("ktop_agg")
                }
            });
        });
    })
        .map_err(|x| STError(format!("error in the timely computation: {}", x)))?;

    Ok(())
}


/// Run graph algorithms on provided `Stream`.
trait Algorithms<S: Scope<Timestamp = Pair<u64, Duration>>> {
    /// Run graph algorithms on provided `Stream`.
    fn algo(&self, steps: u32) -> Collection<S, (ActivityType, (PagEdge, u32)), isize>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> Algorithms<S> for Stream<S, (PagEdge, S::Timestamp, isize)>{
    fn algo(&self, steps: u32) -> Collection<S, (ActivityType, (PagEdge, u32)), isize>{
        let waiting = self
            .filter(|(x, _t, _diff)| x.edge_type == ActivityType::Waiting)
            .as_collection()
            .map(|x| (x.source.timestamp, (x, 0)));

        let all: Arranged<_, TraceAgent<OrdValSpine<Duration, _, Pair<u64, Duration>, isize>>> = self
            .as_collection()
            .map(|x| (x.destination.timestamp, x))
            .arrange_by_key();

        let mut streams = (0 .. steps).fold(vec![waiting], |mut acc, n| {
            let last = acc.pop().expect("?");
            let last_a = last.arrange_by_key();

            let new = last_a.join_core(&all, move |_key, _old, new| Some((new.source.timestamp, (new.clone(), steps - n))));

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
            .map(|(_key, new)| ((new.0).edge_type, new))
    }
}
