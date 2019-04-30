//! Counts the whitelist's paths' length in a PAG provided through replayers.
//! Through manipulating the blacklist, PAG edges can be dropped from the computation.

use std::time::Duration;

use differential_dataflow::input::InputSession;

use timely::{
    communication::{initialize::WorkerGuards, Allocate},
    dataflow::ProbeHandle,
    worker::Worker,
};

use logformat::ActivityType;
use timely_adapter::connect::make_file_replayers;
use timely_pag::{
    algo,
    algo::{Whitelist, Blacklist}
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
    probe: ProbeHandle<Duration>,
    mut whitelist: Whitelist,
    mut blacklist: Blacklist,
) {
    let timer = std::time::Instant::now();

    whitelist.insert((
        4,
        PagNode {
            timestamp: Duration::from_nanos(1_592_530_625),
            worker_id: 0,
        },
    ));
    drop(whitelist);

    blacklist.advance_to(Duration::new(9, 0));
    blacklist.flush();

    while !probe.less_equal(&Duration::new(9, 0)) {
        worker.step();
    }
    println!("done: {}s", timer.elapsed().as_secs());

    blacklist.update_at(
        (
            4,
            PagEdge {
                source: PagNode {
                    timestamp: Duration::from_nanos(1592530625),
                    worker_id: 0,
                },
                destination: PagNode {
                    timestamp: Duration::from_nanos(1592704379),
                    worker_id: 0,
                },
                edge_type: ActivityType::BusyWaiting,
                operator_id: None,
                traverse: TraversalType::Unbounded,
            },
        ),
        Duration::new(10, 0),
        -1,
    );
    drop(blacklist);

    while !probe.done() {
        worker.step();
    }
    println!("done2: {}s", timer.elapsed().as_secs());
}
