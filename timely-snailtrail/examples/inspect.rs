#[macro_use]
extern crate log;

use timely_snailtrail::pag;

use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::operators::capture::EventReader;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::Data;

use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use std::time::Instant;

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
    // let throttle = args.next().unwrap().parse::<u64>().unwrap();
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
            pag::create_pag(scope, readers, index, throttle)
                .bench(index)
                .probe()
        });

        while !probe.done() { worker.step_or_park(None); };

        info!("w{} done", index);

        // stall application
        // use std::io::stdin;
        // stdin().read_line(&mut String::new()).unwrap();
    })
    .unwrap();
}


/// Benchmarks epoch duration & # of events passing through
trait Benchmark<S: Scope<Timestamp = Pair<u64, Duration>>, D: Data> {
    /// Benchmark epoch duration & # of events passing through.
    fn bench(&self, index: usize) -> Stream<S, D>;
}

impl<S: Scope<Timestamp = Pair<u64, Duration>>, D: Data> Benchmark<S, D> for Stream<S, D> {
    fn bench(&self, index: usize) -> Stream<S, D> {
        self.unary_frontier(Pipeline, "benchmark", move |_cap, _info| {
            let mut buffer = Vec::new();
            let mut t = Instant::now();

            move |input, output: &mut OutputHandle<_, D, _>| {
                let mut count = 0;

                input.for_each(|cap, data| {
                    data.swap(&mut buffer);
                    count += buffer.len();
                    output.session(&cap).give_vec(&mut buffer);
                });

                if let Some(f) = input.frontier.frontier().get(0) {
                    println!("{}|{}|{}|{}", index, f.first, t.elapsed().as_nanos(), count);
                    t = Instant::now();
                }
            }
        })
    }
}
