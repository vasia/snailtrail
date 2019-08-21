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
use std::fmt::Debug;

use st2_logformat::pair::Pair;
use st2_logformat::ActivityType;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;

use ws::listen;
use ws::util::Token;
use ws::Handler;
use ws::Sender;
use ws::Handshake;

use serde::{Deserialize, Serialize};
use serde_json;

struct Server<'a, T: Debug + Serialize> { out: Sender, pag_recv: &'a mpsc::Receiver<T> }
impl<'a, T: Debug + Serialize> Handler for Server<'a, T> {
    fn on_open(&mut self, _: Handshake) -> ws::Result<()> {
        self.out.timeout(50, Token(1))
    }

    fn on_timeout(&mut self, _event: Token) -> ws::Result<()> {
        for _i in 0 .. 50 {
            if let Ok(recv) = self.pag_recv.try_recv() {
                println!("{:?}", recv);
                self.out.send(serde_json::to_string(&recv).unwrap())?;
            }
        }
        self.out.timeout(10, Token(1))
    }
}

/// Inspects a running SnailTrail computation, e.g. for benchmarking of SnailTrail itself.
pub fn run(
    timely_configuration: timely::Configuration,
    replay_source: ReplaySource) -> Result<(), STError> {

    let (pag_send, pag_recv) = mpsc::channel();
    let pag_send = Arc::new(Mutex::new(pag_send));

    let listener = std::thread::spawn(move || {
        listen("127.0.0.1:3012", |out| { Server { out, pag_recv: &pag_recv } } ).unwrap();
    });

    timely::execute(timely_configuration, move |worker| {
        let index = worker.index();

        let pag_send = pag_send.lock().expect("cannot lock pag_send").clone();

        // read replayers from file (offline) or TCP stream (online)
        let readers = connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");

        worker.dataflow(|scope| {
            let pag: Stream<_, (PagEdge, Pair<u64, Duration>, isize)>  = pag::create_pag(scope, readers, index, 1);
            pag
                .delay_batch(|time| Pair::new(time.first + 1, Default::default()))
                .map(|(x, t, diff)| (x, Pair::new(t.first + 1, Default::default()), diff))
                .algo(5)
                .inspect(move |(x, t, diff)| pag_send.send((x.clone(), t.first, *diff)).unwrap());
        });
    })
        .map_err(|x| STError(format!("error in the timely computation: {}", x)))?;

    listener.join().expect("couldn't join listener");

    Ok(())
}


/// Run graph algorithms on provided `Stream`.
trait Algorithms<S: Scope<Timestamp = Pair<u64, Duration>>> {
    /// Run graph algorithms on provided `Stream`.
    fn algo(&self, steps: u32) -> Collection<S, (ActivityType, (isize, isize)), isize>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> Algorithms<S> for Stream<S, (PagEdge, S::Timestamp, isize)>{
    fn algo(&self, steps: u32) -> Collection<S, (ActivityType, (isize, isize)), isize>{
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
        let k_steps = streams
            .pop()
            .expect("?")
            .concatenate(streams)
            .map(|(_key, new)| ((new.0).edge_type, new))
            .reduce(|_key, input, output| {
                let summary = input.iter().fold((0, 0), |acc, ((_edge, weight), diff)| {
                    (acc.0 + *diff, acc.1 + *diff * *weight as isize)
                });
                output.push((summary, 1))
            });

        k_steps
    }
}
