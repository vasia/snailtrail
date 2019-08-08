#[macro_use]
extern crate log;

use timely_snailtrail::pag;
use timely_snailtrail::pag::PagEdge;

use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::operators::capture::EventReader;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::Data;
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::aggregation::aggregate::Aggregate;
use timely::dataflow::operators::filter::Filter;
use timely::dataflow::operators::delay::Delay;


use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use std::time::Instant;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use logformat::pair::Pair;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;


fn main() {
    env_logger::init();

    let mut args = std::env::args();

    info!("CLAs: {:?}", args);
    let _ = args.next(); // bin name
    let source_peers = args.next().unwrap().parse::<usize>().unwrap();
    let throttle = 1;
    let online = if args.next().unwrap().parse::<bool>().unwrap() {
        Some((args.next().unwrap().parse::<String>().unwrap(), std::env::args().nth(5).unwrap().parse::<u16>().unwrap()))
    } else {
        None
    };
    let _ = args.next(); // --

    let args = args.collect::<Vec<_>>();

    // creates one socket per worker in the computation we're examining
    let replay_source = if let Some((ip, port)) = &online {
        let sockets = connect::open_sockets(ip.parse().expect("couldn't parse IP"), *port, source_peers).expect("couldn't open sockets");
        ReplaySource::Tcp(Arc::new(Mutex::new(sockets)))
    } else {
        let files = (0 .. source_peers)
            .map(|idx| format!("{}.dump", idx))
            .map(|path| Some(PathBuf::from(path)))
            .collect::<Vec<_>>();

        ReplaySource::Files(Arc::new(Mutex::new(files)))
    };

    timely::execute_from_args(args.clone().into_iter(), move |worker| {
        let index = worker.index();

        // read replayers from file (offline) or TCP stream (online)
        let readers: Vec<EventReader<_, timely_adapter::connect::CompEvent, _>> =
            connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");

        let probe: ProbeHandle<Pair<u64, Duration>> = worker.dataflow(|scope| {
            let pag: Stream<_, (PagEdge, Pair<u64, Duration>, isize)>  = pag::create_pag(scope, readers, index, throttle);
            pag.activities(index).probe()
        });

        while !probe.done() { worker.step_or_park(None); };

        info!("w{} done", index);
    })
    .unwrap();
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

/// Benchmarks epoch duration & # of events passing through
trait Metrics<S: Scope<Timestamp = Pair<u64, Duration>>> {
    /// Reports activity type & duration per epoch per worker
    fn activities(&self, index: usize) -> Stream<S, ()>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>> Metrics<S> for Stream<S, (PagEdge, S::Timestamp, isize)> {
    fn activities(&self, index: usize) -> Stream<S, ()> {
        if index == 0 {
            println!("epoch,from_worker,to_worker,activity_type,#(activities),t(activities),#(records)");
        }

        self
            // .filter(|(edge, _t, _diff)| edge.source.worker_id == edge.destination.worker_id)
            .delay_batch(|time| Pair::new(time.first + 1, Default::default()))
            .map(|(edge, _t, _diff)| ((edge.source.worker_id, edge.destination.worker_id, edge.edge_type), edge))
            .aggregate::<_,(u64, u128, u64),_,_,_>(
                |_key, edge, acc| {
                    // @TODO: Due to clock skew, this is sometimes < 0
                    let dst_ts = edge.destination.timestamp.as_nanos();
                    let src_ts = edge.source.timestamp.as_nanos();

                    let duration = if src_ts > dst_ts {
                        0
                    } else {
                        dst_ts - src_ts
                    };

                    let processed = edge.length.unwrap_or(0) as u64;

                    *acc = (acc.0 + 1, acc.1 + duration, acc.2 + processed);
                },
                |key, acc| (key.0, key.1, key.2, acc.0, acc.1, acc.2),
                |key| calculate_hash(key))
            .inspect_time(|t,x| println!("{:?},{},{},{:?},{},{},{}", t.first - 1, x.0, x.1, x.2, x.3, x.4, x.5))
            .map(|_| ())
    }
}
