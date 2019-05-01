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
    io::Write,
};

use timely::{
    communication::allocator::Generic,
    worker::Worker,
    logging::{BatchLogger, TimelyEvent, WorkerIdentifier},
    dataflow::operators::capture::{
        EventReader,
        Event,
        EventWriter,
        event::EventPusher
    }
};

use TimelyEvent::{Operates, Channels, Progress, Messages, Schedule, Text};


/// Listens on 127.0.0.1:8000 and opens `source_peers` sockets from the
/// computations we're examining (one socket for every worker on the
/// examined computation)
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

// @TODO look into extracting the .next logic, instead dump the whole TCP stream into a buffer
// and read from that with replay so that computation isn't stalled by large snailtrail batch
// ingestion
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

/// Logger to use for computation event logging
pub enum Logger {
    /// Batches events to TCP or file. Outputs each batch at last event's
    /// time. No further event preprocessing in any way.
    All,
    /// Batches events to TCP or file. Filters events so that they are
    /// optimized for use in PAG construction.
    Pag,
}

// @TODO: pass as CLA
const MAX_FUEL: isize = 30_000;

/// Batched logging of events to TCP or file.
/// For live analysis, provide `SNAILTRAIL_ADDR` as env variable.
/// Else, the computation will log to file for later replay.
pub fn register_logger(worker: &mut Worker<Generic>, logger: Logger) {
    if let Ok(addr) = ::std::env::var("SNAILTRAIL_ADDR") {
        if let Ok(stream) = TcpStream::connect(&addr) {
            let writer = EventWriter::new(stream);
            match logger {
                Logger::All => log_all(worker, writer),
                Logger::Pag => log_pag(worker, writer),
            };
        } else {
            panic!("Could not connect logging stream to: {:?}", addr);
        }
    } else {
        let name = format!("{:?}.dump", worker.index());
        let path = Path::new(&name);
        let file = match File::create(&path) {
            Err(why) => panic!("couldn't create {}: {}", path.display(), why.description()),
            Ok(file) => file,
        };
        let writer = EventWriter::new(file);

        match logger {
            Logger::All => log_all(worker, writer),
            Logger::Pag => log_pag(worker, writer),
        };
    }
}

fn log_all<W: 'static + Write> (worker: &mut Worker<Generic>, writer: EventWriter<Duration, (Duration, usize, TimelyEvent), W>) {
    let mut logger = BatchLogger::new(writer);

    let mut fuel = MAX_FUEL;
    let mut buffer = Vec::new();

    let index = worker.index();

    worker.log_register()
        .insert::<TimelyEvent,_>("timely", move |time, data| {
            buffer.append(data);
            fuel -= 1;

            if fuel <= 0 {
                fuel = MAX_FUEL;
                println!("all publishing {}@{:?}", index, &time);
                logger.publish_batch(&time, &mut buffer);
                buffer.drain(..);
            }
        });
}

// @TODO docstring
fn log_pag<W: 'static + Write> (worker: &mut Worker<Generic>, mut writer: EventWriter<Duration, (Duration, usize, TimelyEvent), W>) {
    use std::collections::HashMap;

    let mut fuel = MAX_FUEL;
    let mut buffer = HashMap::new();
    let mut time_order = Vec::new();

    let index = worker.index();

    let mut frontier = Duration::new(0,1);
    time_order.push(frontier.clone());

    let mut old_frontier = Duration::new(0,0);

    worker.log_register()
        .insert::<TimelyEvent,_>("timely", move |time, data| {

            if data.len() > 0 {
                fuel -= 1;
            }

            for tuple in data.drain(..) {
                match &tuple.2 {
                    Operates(_) | Channels(_) | Progress(_) | Messages(_) | Schedule(_)  => {
                        let entry = buffer.entry(frontier).or_insert(Vec::new());
                        entry.push(tuple);
                    },
                    Text(_) => {
                        let entry = buffer.entry(frontier).or_insert(Vec::new());
                        entry.push(tuple);
                        frontier = time.clone();
                        time_order.push(frontier.clone());
                    },
                    _ => {}
                }
            }

            // every `fuel` event batches, we push them all to the writer
            if fuel <= 0 {
                fuel = MAX_FUEL;
                // println!("pag publishing {}@{:?}", index, &time);

                // the newest frontier is wip, so we won't consider it
                let newest_frontier = time_order.pop().expect("time_order should never be empty");

                // we iterate through the time_order to make sure we push events in the correct order
                for time in time_order.drain(..) {
                    let batch = buffer.remove(&time).expect("time_order and buffer should stay in sync");

                    // println!("publish @{:?}, count: {}", time, batch.len());

                    writer.push(Event::Messages(time, batch));
                    writer.push(Event::Progress(vec![(time, 1), (old_frontier, -1)]));
                    old_frontier = time;
                }
                time_order.push(newest_frontier.clone());
            }
        });
}


// /// send timely msgs via tcp
// pub fn register_tcp_dumper(worker: &mut Worker<Generic>) {

//     if let Ok(addr) = ::std::env::var("TIMELY_WORKER_ADDR") {
//         if let Ok(stream) = TcpStream::connect(&addr) {
//             let mut writer = EventWriter::new(stream);

//             let mut x = 0;
//             let mut buffer = Vec::new();

//             worker.log_register()
//                 .insert::<TimelyEvent,_>("timely", move |_time, data| {
//                     for (ts, wid, event) in data {
//                         match event {
//                             Operates(_) => { x += 1; buffer.push((*ts, *wid, event.clone())); },
//                             Channels(_) => {  x += 1; buffer.push((*ts, *wid, event.clone())); },
//                             Progress(_) => {  x += 1; buffer.push((*ts, *wid, event.clone())); },
//                             Messages(_) => {  x += 1; buffer.push((*ts, *wid, event.clone())); },
//                             Schedule(_) => {  x += 1; buffer.push((*ts, *wid, event.clone())); },
//                             Text(_) => { x += 1; buffer.push((*ts, *wid, event.clone())); },
//                             _ => {}
//                         }

//                         if x > 10_000 {
//                             println!("(1)writing out");
//                             writer.push(Event::Messages(Duration::new(0,0), ::std::mem::replace(&mut buffer, Vec::new())));
//                             x = 0;
//                         }
//                     }
//                 });
//         }
//         else {
//             panic!("Could not connect logging stream to: {:?}", addr);
//         }
//     }
// }

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
