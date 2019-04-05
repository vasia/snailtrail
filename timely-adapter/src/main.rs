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

use std::time::Duration;

use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::capture::replay::Replay;
use timely::dataflow::operators::delay::Delay;
use timely::dataflow::operators::filter::Filter;
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::operators::map::Map;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::logging::TimelyEvent;
use timely::logging::TimelyEvent::{Channels, Operates};
use timely::Data;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Reduce;

mod connect;
use crate::connect::{make_file_replayers, make_replayers, open_sockets};

fn reconstruct_dataflow<S: Scope<Timestamp = Duration>>(
    stream: Stream<S, (Duration, usize, TimelyEvent)>,
) {
    let operates = stream
        .filter(|(_, worker, _)| *worker == 0 as usize)
        .flat_map(|(t, _worker, x)| {
            if let Operates(event) = x {
                if event.addr.len() > 1 {
                    Some(((event.addr[1], event), t, 1))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .as_collection();

    stream
        .filter(|(_, worker, _)| *worker == 0 as usize)
        .flat_map(|(t, _worker, x)| {
            if let Channels(event) = x {
                Some(((event.source.0, event), t, 1))
            } else {
                None
            }
        })
        .as_collection()
        .join(&operates) // join sources
        .map(|(_key, (a, b))| (a.target.0, (a, b)))
        .join(&operates) // join targets
        .map(|(_key, ((_a, b), c))| (0, (b, c)))
        .reduce(|_key, input, output| {
            for ((from, to), _t) in input {
                output.push((
                    format!("({}, {}) -> ({}, {})", from.id, from.name, to.id, to.name),
                    1,
                ))
            }
        })
        .map(|(_key, x)| x)
        .inspect(|x| println!("Dataflow: {:?}", x));
}

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

            reconstruct_dataflow(stream);
        });

        // stall application
        // use std::io::stdin;
        // stdin().read_line(&mut String::new()).unwrap();
    })
    .expect("Something went wrong.");
}
