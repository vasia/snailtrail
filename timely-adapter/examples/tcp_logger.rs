use std::time::Duration;

use timely_adapter::record_collection;
use timely_adapter::connect::{open_sockets, make_replayers};
use timely_adapter::connect::{make_file_replayers};
use timely::dataflow::operators::capture::replay::Replay;
use timely::dataflow::operators::probe::Probe;

fn main() {
    // the number of workers in the computation we're examining
    let source_peers = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();
    let iterations = std::env::args().nth(2).unwrap().parse::<u64>().unwrap();

    // one socket per worker in the computation we're examining
    // let sockets = open_sockets(source_peers);

    timely::execute_from_args(std::env::args(), move |worker| {
        let timer = std::time::Instant::now();

        // let sockets = sockets.clone();
        // let replayers = make_replayers(sockets, worker.index(), worker.peers());
        let replayers = make_file_replayers(worker.index(), source_peers, worker.peers());

        let idx = worker.index();

        // define a new computation.
        let probe = worker.dataflow(|scope| {
            // let stream = replayers.replay_into(scope);
            let stream = record_collection(scope, replayers);

            stream
                // .inspect(|x| println!("@{:?} : {}", x.1, x.0))
                .probe()
        });

        // let mut timer = std::time::Instant::now();
        // let mut epoch = Duration::new(0,0);
        while !probe.done() {
            worker.step();
        }
        println!("done: {}s", timer.elapsed().as_secs());
        // while epoch < Duration::new(iterations,0) {
        //     while probe.less_equal(&epoch) {
        //         worker.step();
        //     }
        //     timer = std::time::Instant::now();
        //     epoch += Duration::new(0, 10000000);
        // }

        // let mut epoch = Duration::new(0,0);
        // while epoch < Duration::new(10, 0) {
        //     let timer = std::time::Instant::now();
        //     while probe.less_equal(&epoch) {
        //         worker.step();
        //     }
        //     println!("{}@{:?}: {}", idx, epoch, timer.elapsed().as_millis());
        //     epoch += Duration::new(1,0);
        // }

        // stall application
        // use std::io::stdin;
        // stdin().read_line(&mut String::new()).unwrap();
    })
    .expect("Something went wrong.");
}
