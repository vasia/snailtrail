#[macro_use]
extern crate log;

use timely_snailtrail::{pag, Config};

use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::probe::Probe;
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
    let throttle = std::env::args().nth(3).unwrap().parse::<u64>().unwrap();
    let from_file = if let Some(_) = std::env::args().nth(4) {
        true
    } else {
        false
    };
    let config = Config {
        timely_args: vec!["-w".to_string(), worker_peers.to_string()],
        worker_peers,
        source_peers,
        from_file,
        throttle,
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
        let index = worker.index();
        if index == 0 {println!("{:?}", &config);}

        // read replayers from file (offline) or TCP stream (online)
        let readers: Vec<EventReader<_, timely_adapter::connect::CompEvent, _>> =
            connect::make_readers(replay_source.clone(), worker.index(), worker.peers()).expect("couldn't create readers");

        let probe: ProbeHandle<Pair<u64, Duration>> = worker.dataflow(|scope| {
            pag::create_pag(scope, readers, index, config.throttle)
                .probe()
        });

        // let mut timer = std::time::Instant::now();
        // while probe.less_equal(&Pair::new(2, std::time::Duration::from_secs(0))) {
        while !probe.done() {
            // probe.with_frontier(|f| {
            //     let f = f.to_vec();
            //         println!("w{} frontier: {:?} | took {:?}ms", index, f, timer.elapsed().as_millis());
            //         timer = std::time::Instant::now();
            // });
            worker.step();
        };

        info!("w{} done", index);
        // stall application
        // use std::io::stdin;
        // stdin().read_line(&mut String::new()).unwrap();
    })
    .unwrap();
}


        //         // .filter(|x| x.source.epoch > x.destination.epoch)
        //         // .inspect(move |(x, t, diff)| println!("w{} | {} -> {}: -> w{}\t {:?}", x.source.worker_id, x.source.seq_no, x.destination.seq_no, x.destination.worker_id, x.edge_type))



        // use std::fs::File;
        // use std::io::Write;
        // let mut file = File::create(format!("out_{}_{}_{}.csv", index, config.worker_peers, config.source_peers)).expect("couldn't create file");

        //         // .inner
        //         // .unary_frontier(timely::dataflow::channels::pact::Exchange::new(|_x| 0), "TheVoid", move |_cap, _info| {
        //         //     let mut t0 = Instant::now();
        //         //     let mut last: Pair<u64, Duration> = Default::default();
        //         //     let mut buffer = Vec::new();
        //         //     let mut count = 0;

        //         //     move |input, output: &mut OutputHandle<_, (PagEdge, Pair<u64, Duration>, isize), _>| {
        //         //         let mut received_input = false;
        //         //         input.for_each(|cap, data| {
        //         //             data.swap(&mut buffer);
        //         //             received_input = !buffer.is_empty();
        //         //             count += buffer.len();
        //         //             // for x in buffer.drain(..) {
        //         //             //     count += x.2; // subtract retractions
        //         //             // }
        //         //             buffer.clear();
        //         //         });

        //         //         if input.frontier.is_empty() {
        //         //             println!("[{:?}] inputs to void sink ceased", t0.elapsed());
        //         //             println!("{:?}", count);
        //         //             // writeln!(file, "{},{},{},{}", t0.elapsed().as_millis(), count, last.first, last.second.as_millis()).expect("write failed");
        //         //         } else if received_input && !input.frontier.frontier().less_equal(&last) {
        //         //             // writeln!(file, "{},{},{},{}", t0.elapsed().as_millis(), count, last.first, last.second.as_millis()).expect("write failed");

        //         //             last = input.frontier.frontier()[0].clone();
        //         //             // println!("{:?}", last);
        //         //             t0 = Instant::now();
        //         //         }
        //         //     }
        //         // })
