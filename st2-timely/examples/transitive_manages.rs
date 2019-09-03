//! This is an exemplary dataflow that includes instrumentation to be used by SnailTrail.
//!
//! For barebones logging of TimelyEvents, env var `TIMELY_WORKER_LOG_ADDR=<IP:Port>` can
//! be passed. This then logs every message handled by any worker.

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::operators::Join;

use timely::communication::allocator::Generic;
use timely::logging::TimelyEvent;
use timely::order::PartialOrder;
use timely::worker::Worker;

use timely_adapter::connect::register_file_dumper;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let index = worker.index();
        let mut input = InputSession::new();

        // (Un)comment to toggle between write to file & write to TCP
        register_file_dumper(worker);

        // define a new computation.
        let probe = worker.dataflow(|scope| {
            // create a new collection from our input.
            let manages = input.to_collection(scope);

            manages // transitive contains (manager, person) for many hops.
                .iterate(|transitive| {
                    let manages = manages.enter(&transitive.scope());

                    transitive
                        .map(|(mk, m1)| (m1, mk))
                        .join(&manages)
                        .map(|(m1, (mk, p))| (mk, p))
                        .concat(&manages)
                        .distinct()
                })
                // .inspect(|x| println!("{}, {:?}", index, x))
                .probe()
        });

        // handle to `timely` events logger
        let timely_logger = worker
            .log_register()
            .get::<TimelyEvent>("timely")
            .expect("Timely logger absent.");

        let size = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();
        let rounds = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
        let sleep_ms = std::env::args().nth(3).unwrap().parse::<u64>().unwrap();
        input.advance_to(0);

        timely_logger.log(TimelyEvent::Text(format!(
            "[st] begin computation at epoch: {:?}",
            input.time()
        )));

        for round in 0..rounds {
            for person in 0..size {
                if worker.index() == person % worker.peers() {
                    input.insert((person / 2, person));
                    input.remove((person / 3, person));
                }
            }

            let timer = std::time::Instant::now();
            input.advance_to(round + 1);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
            println!(
                "{}@{}: epoch done in {}",
                index,
                round,
                timer.elapsed().as_millis()
            );

            timely_logger.log(TimelyEvent::Text(format!(
                "[st] closed times before: {:?}",
                input.time()
            )));

            if sleep_ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(sleep_ms));
            }
        }
    })
    .expect("Computation terminated abnormally");
}
