use timely_snailtrail::{pag, Config};
use timely_snailtrail::pag::DumpPAG;

use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::operators::capture::replay::Replay;
use timely::dataflow::operators::capture::EventReader;


use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;

use logformat::pair::Pair;

use tdiag_connect::receive as connect;
use tdiag_connect::receive::ReplaySource;

fn main() {
    env_logger::init();

    let worker_peers = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();
    let source_peers = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
    let from_file = if let Some(_) = std::env::args().nth(3) {
        true
    } else {
        false
    };
    let config = Config {
        timely_args: vec!["-w".to_string(), worker_peers.to_string()],
        worker_peers,
        source_peers,
        from_file,
    };

    inspector(config);
}

fn inspector(config: Config) {
    // creates one socket per worker in the computation we're examining
    let replay_source = if config.from_file {
        let files = (0 .. config.source_peers)
            .map(|idx| format!("{}.dump", idx))
            .map(|path| Some(PathBuf::from(path)))
            .collect::<Vec<_>>();

        ReplaySource::Files(Arc::new(Mutex::new(files)))
    } else {
        let sockets = connect::open_sockets("127.0.0.1".parse().expect("couldn't parse IP"), 8000, config.source_peers).expect("couldn't open sockets");
        ReplaySource::Tcp(Arc::new(Mutex::new(sockets)))
    };

    timely::execute_from_args(config.timely_args.clone().into_iter(), move |worker| {
        let timer = std::time::Instant::now();
        let mut timer2 = std::time::Instant::now();

        let index = worker.index();
        if index == 0 {
            println!("{:?}", &config);
        }

        // read replayers from file (offline) or TCP stream (online)
        let readers: Vec<EventReader<_, timely_adapter::connect::CompEvent, _>> =
            connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");

        let probe: ProbeHandle<Pair<u64, Duration>> = worker.dataflow(|scope| {
            pag::create_pag(scope, replayers, index)
            // replayers.replay_into(scope)
            // timely_adapter::create_lrs(scope, replayers, index)
                .probe()
        });

        let mut curr_frontier = vec![];
        // while probe.less_equal(&Pair::new(3, std::time::Duration::from_secs(100000000000))) {
        while !probe.done() {
            probe.with_frontier(|f| {
                let f = f.to_vec();
                if f != curr_frontier {
                    println!("w{} frontier: {:?} | took {:?}ms", index, f, timer2.elapsed().as_millis());
                    timer2 = std::time::Instant::now();
                    curr_frontier = f;
                }
            });
            worker.step();
        }

        println!("done with stepping.");
        println!("w{} done: {}ms", index, timer.elapsed().as_millis());
    })
    .unwrap();
}
