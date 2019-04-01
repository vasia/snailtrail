// Copyright 2019 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Connects to a TimelyDataflow / DifferentialDataflow instance that is run with
//! `TIMELY_WORKER_LOG_ADDR` env variable set and constructs a single epoch PAG
//! from the received log trace.

#![deny(missing_docs)]

use timely::dataflow::operators::capture::replay::Replay;
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::operators::map::Map;
use timely::logging::TimelyEvent::{Channels, Operates};

use differential_dataflow::collection::AsCollection;

mod connect;
use crate::connect::{make_file_replayers, make_replayers, open_sockets};

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

        // define a new computation.
        worker.dataflow(|scope| {
            let stream = replayers.replay_into(scope);

            stream.inspect(|x| println!("{:?}", x));

            // let _operates = stream
            //     .flat_map(|(t, _, x)| {
            //         if let Operates(event) = x {
            //             Some((event, t, 1 as isize))
            //         } else {
            //             None
            //         }
            //     })
            //     .as_collection()
            //     // only keep elements that came from worker 0
            //     // (the first element of "addr" is the worker id)
            //     .filter(|x| *x.addr.first().unwrap() == 0)
            //     .inspect(|x| println!("Operates: {:?}", x));

            // let _channels = stream
            //     .flat_map(|(t, _, x)| {
            //         if let Channels(event) = x {
            //             Some((event, t, 1 as isize))
            //         } else {
            //             None
            //         }
            //     })
            //     .as_collection()
            //     .filter(|x| *x.scope_addr.first().unwrap() == 0)
            //     .inspect(|x| println!("Channels: {:?}", x));
        });

        // stall application
        // use std::io::stdin;
        // stdin().read_line(&mut String::new()).unwrap();
    })
    .expect("Something went wrong.");
}
