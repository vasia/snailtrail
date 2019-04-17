use timely_adapter::record_collection;
use timely_adapter::connect::make_file_replayers;

fn main() {
    // the number of workers in the computation we're examining
    let source_peers = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();

    // one socket per worker in the computation we're examining
    // let sockets = open_sockets(source_peers);

    timely::execute_from_args(std::env::args(), move |worker| {
        // (un)comment lines to switch between TCP and offline filedump reader
        // let sockets = sockets.clone();
        // let replayers = make_replayers(sockets, worker.index(), worker.peers());
        let replayers = make_file_replayers(worker.index(), source_peers, worker.peers());

        let idx = worker.index();

        // define a new computation.
        worker.dataflow(|scope| {
            let stream = record_collection(scope, replayers);

            // stream.inspect(|x| println!("{:?}@{:?} : {}", idx, x.1, x.0));
        });

        // stall application
        // use std::io::stdin;
        // stdin().read_line(&mut String::new()).unwrap();
    })
    .expect("Something went wrong.");
}
