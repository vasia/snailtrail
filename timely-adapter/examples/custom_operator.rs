//! This is an exemplary dataflow that includes instrumentation to be used by SnailTrail.
//!
//! For barebones logging of TimelyEvents, env var `TIMELY_WORKER_LOG_ADDR=<IP:Port>` can
//! be passed. This then logs every message handled by any worker.
//!
//! Alternatively, [register_file_dumper](register_file_dumper) can be used to create a
//! dump to be read back in by the [timely adapter](timely_adapter).

#[macro_use]
extern crate log;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::reduce::Reduce;

use timely::communication::allocator::Generic;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::CapabilityRef;
use timely::logging::{Logger, TimelyEvent};
use timely::worker::Worker;

use timely_adapter::connect::register_logger;

use logformat::pair::Pair;
use std::time::Duration;

fn main() {
    env_logger::init();

    let load_balance_factor = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();

    timely::execute_from_args(std::env::args().skip(1), move |worker| {
        register_logger::<Pair<u64, Duration>>(worker, load_balance_factor);
        let timer = std::time::Instant::now();

        let index = worker.index();
        let mut input = InputSession::new();

        // define a new computation.
        let probe = worker.dataflow(|scope| {
            // create a new collection from our input.
            let input_coll = input.to_collection(scope);

            // let mut vector: Vec<(usize, usize, usize)> = Vec::new();

            input_coll
                .inspect(move |(x, t, diff)| info!("1: w{:?} - {:?} @ {:?}d{:?}", index, x, t, diff))
                .map(|x| (0, x))
                .reduce(|_key, input, output| {
                    let mut sum = 0;
                    for (x, diff) in input {
                        for i in 0..*diff {
                            if i >= 0 {
                                sum += *x;
                            }
                        }
                    }
                    output.push((sum * 100, 1))
                })
                .inspect(move |(x, t, diff)| info!("2: w{:?} - {:?} @ {:?}d{:?}", index, x, t, diff))
                // .inner
                // .unary(
                //     Pipeline,
                //     "example",
                //     |default_cap: Capability<usize>, _info| {
                //         let mut cap = Some(default_cap.delayed(&12));
                //         let mut vector = Vec::new();
                //         move |input, output| {
                //             if let Some(ref c) = cap.take() {
                //                 output.session(&c).give(((100, 100), 0, 0));
                //             }
                //             while let Some((time, data)) = input.next() {
                //                 data.swap(&mut vector);
                //                 output.session(&time).give_vec(&mut vector);
                //             }
                //         }
                //     },
                // )
                .probe()
        });

        let batch = 5;
        let rounds = 2;
        // let batch = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();
        // let rounds = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();

        // handle to `timely` events logger
        let timely_logger = worker.log_register().get::<TimelyEvent>("timely");

        input.advance_to(0);

        if let Some(timely_logger) = &timely_logger {
            timely_logger.log(TimelyEvent::Text(format!(
                "[st] begin computation at epoch: {:?}",
                input.time()
            )));
        }

        for round in 0..rounds {
            for i in 0..batch {
                if worker.index() == i % worker.peers() {
                    input.insert((round + 1) * i);
                }
            }

            input.advance_to(round + 1);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
            info!("{:?}\tRound {} complete", timer.elapsed(), round);

            if let Some(timely_logger) = &timely_logger {
                timely_logger.log(TimelyEvent::Text(format!(
                    "[st] closed times before: {:?}",
                    input.time()
                )));
            }
        }

        if let Some(timely_logger) = &timely_logger {
            timely_logger.log(TimelyEvent::Text("[st] computation done".to_string()));
        }

        // stall application
        // use std::io::stdin;
        // stdin().read_line(&mut String::new()).unwrap();
    })
    .expect("Computation terminated abnormally");
}
