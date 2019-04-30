//! Helpers to obtain traces from timely / differential.
//! The TCP connector \[1\] allows connecting to a live timely / differential instance.
//! The file connector allows to replay serialized trace dumps.
//! The file dumper can be used to dump traces to be read by the file connector.
//!
//! \[1\] Modified from TimelyDataflow examples & https://github.com/utaal/timely-viz

use std::{
    error::Error,
    fs::File,
    net::{TcpListener, TcpStream},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use timely::communication::allocator::Generic;
use timely::dataflow::operators::capture::EventReader;
use timely::logging::{TimelyEvent, WorkerIdentifier};
use timely::worker::Worker;

/// Listens on 127.0.0.1:8000 and opens `source_peers` sockets from the
/// computations we're examining.
pub fn open_sockets(source_peers: usize) -> Arc<Mutex<Vec<Option<TcpStream>>>> {
    let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    Arc::new(Mutex::new(
        (0..source_peers)
            .map(|_| Some(listener.incoming().next().unwrap().unwrap()))
            .collect::<Vec<_>>(),
    ))
}

/// A replayer that reads data to be streamed into timely
pub type Replayer<R> = EventReader<Duration, (Duration, WorkerIdentifier, TimelyEvent), R>;

/// Construct replayers that read data from sockets and can stream it into
/// timely dataflow.
pub fn make_replayers(
    sockets: Arc<Mutex<Vec<Option<TcpStream>>>>,
    index: usize,
    peers: usize,
) -> Vec<Replayer<TcpStream>> {
    sockets
        .lock()
        .unwrap()
        .iter_mut()
        .enumerate()
        .filter(|(i, _)| *i % peers == index)
        .map(move |(_, s)| s.take().unwrap())
        .map(|r| EventReader::<Duration, (Duration, WorkerIdentifier, TimelyEvent), _>::new(r))
        .collect::<Vec<_>>()
}

/// Construct replayers that read data from a file and can stream it into
/// timely dataflow.
pub fn make_file_replayers(
    index: usize,
    source_peers: usize,
    worker_peers: usize,
) -> Vec<Replayer<File>> {
    println!("{}, {}, {}", index, source_peers, worker_peers);
    (0..source_peers)
        .filter(|i| i % worker_peers == index)
        .map(|i| {
            let name = format!("{:?}.dump", i);
            let path = Path::new(&name);

            match File::open(&path) {
                Err(why) => panic!("couldn't open. {}", why.description()),
                Ok(file) => file,
            }
        })
        .map(|f| EventReader::new(f))
        .collect::<Vec<_>>()
}

/// capture timely log messages to file. Alternatively use `TIMELY_WORKER_LOG_ADDR`.
pub fn register_file_dumper(worker: &mut Worker<Generic>) {
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

// /// Create a custom logger that logs user-defined events
// fn create_user_level_logger(worker: &mut Worker<Generic>) -> Logger<String> {
//     worker
//         .log_register()
//         // _time: lower bound timestamp of the next event that could be seen
//         // data: (Duration, Id, T) - timestamp of event, worker id, custom message
//         .insert::<String, _>("custom_log", |_time, data| {
//             println!("time: {:?}", _time);
//             println!("log: {:?}", data);
//         });

//     worker
//         .log_register()
//         .get::<String>("custom_log")
//         .expect("custom_log logger absent")
// }

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
