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


// @TODO: pass as CLA
const MAX_FUEL: isize = 1;

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

/// Batched logging of events to TCP or file.
/// For live analysis, provide `SNAILTRAIL_ADDR` as env variable.
/// Else, the computation will log to file for later replay.
pub fn register_logger(worker: &mut Worker<Generic>) {
    if let Ok(addr) = ::std::env::var("SNAILTRAIL_ADDR") {
        if let Ok(stream) = TcpStream::connect(&addr) {
            let writer = EventWriter::new(stream);
            log_pag(worker, writer);
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

/// Registers a `TimelyEvent` logger which outputs relevant log events for PAG construction.
/// 1. Only relevant events are written to `writer`.
/// 2. Using `Text` events as markers, logged events are written out at one time per epoch.
/// 3. For higher performance, multiple event batches are written out together as a batch.
/// 4. Dataflow setup is collapsed into t=1ns so that peel_operators is more efficient.
/// 5. If the computation is bounded, capabilities will be dropped correctly at the end of
///    computation.
fn log_pag2<W: 'static + Write> (worker: &mut Worker<Generic>, mut writer: EventWriter<Duration, (Duration, usize, TimelyEvent), W>) {
    use std::collections::HashMap;

    // every `fuel` events, they are written out to `writer`
    let mut fuel = MAX_FUEL;

    // buffer of epochs and events for that epoch
    let mut buffer = HashMap::new();

    // epoch times in the order they advance (used to write out
    // `buffer` in correct order
    let mut time_order = Vec::new();

    // first real frontier, used for setting up the computation
    // (`Operates` et al.)
    let mut frontier = Duration::from_nanos(1);
    time_order.push(frontier);

    // initialized to 0, which we'll drop as soon as the
    // computation makes progress
    let mut old_frontier = Duration::default();

    // keeps track of whether the computation is shutting down to
    // free up last frontier and write out remaining log messages.
    let mut done = false;

    // debug logs
    let index = worker.index();
    let mut total = 0;

    worker.log_register()
        .insert::<TimelyEvent,_>("timely", move |time, data| {
            for tuple in data.drain(..) {
                match &tuple.2 {
                    Channels(_) | Progress(_) | Messages(_) | Schedule(_)  => {
                        fuel -= 1;
                        let entry = buffer.entry(frontier).or_insert(Vec::new());
                        entry.push(tuple);
                    },
                    Operates(_) => {
                        fuel -= 1;
                        let entry = buffer.entry(frontier).or_insert(Vec::new());

                        // all operates events should happen in the initialization epoch
                        assert!(frontier == Duration::from_nanos(1));
                        entry.push((frontier, tuple.1, tuple.2));
                    },
                    // Text events mark epochs in the computation
                    Text(e) => {
                        if e.starts_with("[st] computation done") {done = true;}

                        fuel -= 1;

                        // advance frontier at which events are buffered to current time
                        frontier = *time;
                        time_order.push(frontier);
                    },
                    _ => {}
                }
            }

            if fuel <= 0 || done {
                fuel = MAX_FUEL;
                // the newest frontier is still WIP, so don't consider it
                let newest_frontier = time_order.pop();

                // we iterate through the time_order to make sure we push events in the correct order
                for mut time in time_order.drain(..) {
                    if let Some(batch) = buffer.remove(&time) {
                        // debug logging
                        total += batch.len();
                        info!("sent epoch {:?}@w{}, count: {}, total: {}", time, index, batch.len(), total);

                        writer.push(Event::Messages(time, batch));

                        time += Duration::from_nanos(1); // frontier should move past the current batch
                        writer.push(Event::Progress(vec![(time, 1), (old_frontier, -1)]));

                        old_frontier = time;
                    } else {
                        assert!(done == true, "time order & buffer should only desync on logging wrap-up");
                    }
                }

                // add the newest frontier back in
                if let Some(frontier) = newest_frontier {
                    time_order.push(frontier);
                } else {
                    assert!(done == true, "time order should only dry up on logging wrap-up");
                }

                if done {
                    // prevents this from being called over and over
                    done = false;

                    info!("timely logging wrapping up");
                    writer.push(Event::Progress(vec![(old_frontier, -1)]));
                }
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
