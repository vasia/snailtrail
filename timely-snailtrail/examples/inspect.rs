use timely_adapter::{
    connect::{make_replayers, open_sockets},
};

use timely_snailtrail::{pag, Config};

use timely::dataflow::{
    operators::{capture::replay::Replay, probe::Probe},
    ProbeHandle,
    Scope
};

use std::time::Duration;

use logformat::pair::Pair;

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
    let sockets = if !config.from_file {
        Some(open_sockets(config.worker_peers))
    } else {
        None
    };

    timely::execute_from_args(config.timely_args.clone().into_iter(), move |worker| {
        let timer = std::time::Instant::now();
        let mut timer2 = std::time::Instant::now();

        let index = worker.index();
        if index == 0 {
            println!("{:?}", &config);
        }

        // read replayers from file (offline) or TCP stream (online)
        let replayers = make_replayers(
            worker.index(),
            config.worker_peers,
            config.source_peers,
            sockets.clone(),
        );

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
                    println!("w{} frontier: {:?} | {:?}", index, f, timer2.elapsed().as_millis());
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
