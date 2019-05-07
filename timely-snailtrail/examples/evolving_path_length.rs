//! Counts the whitelist's paths' length in a PAG provided through replayers.
//! Through manipulating the blacklist, PAG edges can be dropped from the computation.

use std::time::Duration;

use timely::{
    communication::{initialize::WorkerGuards, Allocate},
    dataflow::ProbeHandle,
    worker::Worker,
};

use logformat::pair::Pair;
use logformat::ActivityType;
use timely_adapter::connect::make_file_replayers;
use timely_pag::{
    algo,
    algo::{Blacklist, Whitelist},
    pag::{PagEdge, PagNode, TraversalType},
    Config,
};

fn main() {
    let workers = std::env::args().nth(1).unwrap().parse::<String>().unwrap();
    let source_peers = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
    let config = Config {
        timely_args: vec!["-w".to_string(), workers],
        source_peers,
    };

    evolving_path_length(config).expect("computation finished abnormally");
}

fn evolving_path_length(config: Config) -> Result<WorkerGuards<()>, String> {
    timely::execute_from_args(config.timely_args.clone().into_iter(), move |worker| {
        if worker.index() == 0 {
            println!("{:?}", &config);
        }

        // read replayers from file (offline) or TCP stream (online)
        let replayers = make_file_replayers(worker.index(), config.source_peers, worker.peers());

        // path count computation
        let (probe, whitelist, blacklist) = algo::path_length(worker, replayers);

        modify_inputs(worker, probe, whitelist, blacklist);
    })
}

fn modify_inputs<A: Allocate>(
    worker: &mut Worker<A>,
    probe: ProbeHandle<Pair<u64, Duration>>,
    mut whitelist: Whitelist,
    mut blacklist: Blacklist,
) {
    let timer = std::time::Instant::now();

    if worker.index() == 0 {
        // whitelist.insert(PagNode {
        //     timestamp: Duration::from_nanos(24_126_092_030),
        //     worker_id: 0,
        // });

        // blacklist.update_at(
        //     PagEdge {
        //         source: PagNode {
        //             timestamp: Duration::from_nanos(28_347_146_247),
        //             worker_id: 0,
        //         },
        //         destination: PagNode {
        //             timestamp: Duration::from_nanos(28_347_362_984),
        //             worker_id: 0,
        //         },
        //         edge_type: ActivityType::Scheduling,
        //         operator_id: Some(95),
        //         traverse: TraversalType::Unbounded,
        //     },
        //     Duration::new(17, 0),
        //     -1,
        // );

        // blacklist.update_at(
        //     PagEdge {
        //         source: PagNode {
        //             timestamp: Duration::from_nanos(28_158_438_933),
        //             worker_id: 0,
        //         },
        //         destination: PagNode {
        //             timestamp: Duration::from_nanos(28_158_460_631),
        //             worker_id: 0,
        //         },
        //         edge_type: ActivityType::BusyWaiting,
        //         operator_id: None,
        //         traverse: TraversalType::Unbounded,
        //     },
        //     Duration::new(23, 0),
        //     -1,
        // );
    }

    drop(whitelist);
    drop(blacklist);

    while !probe.done() {
        probe.with_frontier(|f| println!("frontier: {:?}", f.to_vec()));
        worker.step();
    }

    println!("done.");
}
