//! This is an exemplary dataflow that includes instrumentation to be used by SnailTrail.
//!
//! For barebones logging of TimelyEvents, env var `TIMELY_WORKER_LOG_ADDR=<IP:Port>` can
//! be passed. This then logs every message handled by any worker.
//!
//! Alternatively, [register_file_dumper](register_file_dumper) can be used to create a
//! dump to be read back in by the [timely adapter](timely_adapter).

use differential_dataflow::input::InputSession;

use timely::communication::allocator::Generic;
use timely::logging::{Logger, TimelyEvent};
use timely::worker::Worker;

/// capture timely log messages to file. Alternatively use `TIMELY_WORKER_LOG_ADDR`.
fn register_file_dumper(worker: &mut Worker<Generic>) {
    use timely::dataflow::operators::capture::EventWriter;
    use timely::logging::BatchLogger;

    use std::error::Error;
    use std::fs::File;
    use std::path::Path;

    let name = format!("{:?}.dump", worker.index());
    let path = Path::new(&name);
    let file = match File::create(&path) {
        Err(why) => panic!("couldn't create {}: {}", path.display(), why.description()),
        Ok(file) => file,
    };

    let writer = EventWriter::new(file);
    let mut logger = BatchLogger::new(writer);

    worker
        .log_register()
        .insert::<TimelyEvent, _>("timely", move |time, data| {
            logger.publish_batch(time, data);
        });
}

/// Create a custom logger that logs user-defined events
fn create_user_level_logger(worker: &mut Worker<Generic>) -> Logger<String> {
    worker
        .log_register()
        // _time: lower bound timestamp of the next event that could be seen
        // data: (Duration, Id, T) - timestamp of event, worker id, custom message
        .insert::<String, _>("custom_log", |_time, data| {
            println!("time: {:?}", _time);
            println!("log: {:?}", data);
        });

    worker
        .log_register()
        .get::<String>("custom_log")
        .expect("custom_log logger absent")
}

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let index = worker.index();
        let mut input = InputSession::new();

        // for now, dump logs to file instead of TCP
        register_file_dumper(worker);

        // define a new computation.
        let probe = worker.dataflow(|scope| {
            // create a new collection from our input.
            let input_coll = input.to_collection(scope);

            input_coll
                .map(|x| x)
                .inspect(|(x, t, diff)| println!("w{:?} - {:?} @ {:?}d{:?}", index, x, t, diff))
                .probe()
        });

        let batch = 3;
        let rounds = 2;
        // let batch = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();
        // let rounds = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
        println!("batch: {}, rounds: {}", batch, rounds);

        // handle to `timely` events logger
        let timely_logger = worker
            .log_register()
            .get::<TimelyEvent>("timely")
            .expect("Timely logger absent.");

        input.advance_to(0);

        timely_logger.log(TimelyEvent::Text(format!(
            "[st] begin computation at epoch: {:?}",
            input.time()
        )));

        for round in 0..rounds {
            for i in 0..batch {
                if worker.index() == i % worker.peers() {
                    input.insert(i);
                }
            }

            input.advance_to(round + 1);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }

            // @TODO: this and other timely events aren't consistently
            // flushed when stalling the application beforehand.
            timely_logger.log(TimelyEvent::Text(format!(
                "[st] closed times before: {:?}",
                input.time()
            )));
        }

        // stall application
        // use std::io::stdin;
        // stdin().read_line(&mut String::new()).unwrap();
    })
    .expect("Computation terminated abnormally");
}

// use differential_dataflow::logging::DifferentialEvent;
// use std::net::TcpStream;
// use timely::communication::allocator::Generic;
// use timely::dataflow::operators::capture::EventWriter;
// use timely::logging::{BatchLogger, TimelyEvent};
// use timely::worker::Worker;

// /// Custom logger that combines `timely`, `differential/arrange` and user-defined events
// pub fn register_logging(worker: &mut Worker<Generic>) {
//     if let Ok(addr) = ::std::env::var("LOGGING_CONN") {
//         if let Ok(stream) = TcpStream::connect(&addr) {
//             let timely_writer = EventWriter::new(stream);
//             let mut timely_logger = BatchLogger::new(timely_writer);
//             worker
//                 .log_register()
//                 .insert::<Uber, _>("timely", move |time, data| {
//                     timely_logger.publish_batch(time, data)
//                 });

//             worker
//                 .log_register()
//                 .insert::<Uber, _>("differential/arrange", move |time, data| {
//                     timely_logger.publish_batch(time, data)
//                 });
//         } else {
//             panic!("Could not connect logging stream to: {:?}", addr);
//         }
//     } else {
//         panic!("Provide LOGGING_CONN env var");
//     }
// }
