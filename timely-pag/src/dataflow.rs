// Copyright 2019 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std;
use std::convert::From as StdFrom;
use std::time::Duration;

use timely;
use timely::communication::initialize::WorkerGuards;
use timely::dataflow::Scope;

use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Reduce;
use differential_dataflow::Collection;

use crate::{PagEdge, PagNode, PagOutput, TraversalType};

use logformat::{ActivityType, EventType, LogRecord};

use timely_adapter::connect::make_file_replayers;
use timely_adapter::record_collection;

#[derive(Clone, Debug)]
pub struct Config {
    pub timely_args: Vec<String>,
    pub source_peers: usize,
}

pub fn end_to_end_analysis(config: Config) -> Result<WorkerGuards<()>, String> {
    timely::execute_from_args(config.timely_args.clone().into_iter(), move |worker| {
        let timer = std::time::Instant::now();
        let config = config.clone();
        if worker.index() == 0 {
            println!("{:?}", config);
        }

        let replayers = make_file_replayers(worker.index(), config.source_peers, worker.peers());

        // construct dataflow
        let probe = worker.dataflow(|scope| {
            let records: Collection<_, LogRecord, isize> = record_collection(scope, replayers);

            // PAG Analysis dataflow
            // build_dataflow(config.clone(), records)
            records
                .inspect(|x| println!("{}", x.0))
                .probe()
        });

        while !probe.done() {
            worker.step();
        }
        println!("done: {}s", timer.elapsed().as_secs());

        // provide input
        // if worker.index() == 0 {
        //     // create named probe wrappers
        //     let names = vec!["pag", "bc", "sp", "summary", "sp_summary"];
        //     let mut probe_wrappers = Vec::with_capacity(probes.len());
        //     for (probe, name) in probes.into_iter().zip(names.into_iter()) {
        //         probe_wrappers.push(ProbeWrapper::new(StdFrom::from(name), probe));
        //     }
    })
}


    // vec![probe_pag,
    //      probe_bc,
    //      probe_sp,
    //      probe_summary,
    //      probe_sp_summary];
}
