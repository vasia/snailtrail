use std::time::Duration;

use timely::dataflow::operators::capture::replay::Replay;
use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::InputHandle;
use timely::logging::OperatesEvent;
use timely::logging::ScheduleEvent;
use timely::logging::TimelyEvent::Operates;
use timely::logging::TimelyEvent::Schedule;
use timely::logging::{StartStop, TimelyEvent};
use timely_adapter::connect::make_file_replayers;
use timely_adapter::connect::{make_replayers, open_sockets};
use timely_adapter::record_collection;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::reduce::Threshold;

fn main() {
    // the number of workers in the computation we're examining
    let source_peers = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();
    let iterations = std::env::args().nth(2).unwrap().parse::<u64>().unwrap();

    // one socket per worker in the computation we're examining
    // let sockets = open_sockets(source_peers);

    timely::execute_from_args(std::env::args(), move |worker| {
        let timer = std::time::Instant::now();

        // let sockets = sockets.clone();
        // let replayers = make_replayers(sockets, worker.index(), worker.peers());
        let replayers = make_file_replayers(worker.index(), source_peers, worker.peers());

        let idx = worker.index();

        let mut input = InputHandle::new();

        // define a new computation.
        let probe = worker.dataflow(|scope| {
            // let stream = replayers.replay_into(scope);
            // let stream = input.to_stream(scope);
            let stream = record_collection(scope, replayers);

            stream
                // .map(|(t, w, _x)| (w, t, 1))
                // .as_collection()
                .inspect_batch(|t, x| println!("a: @{:?}: {:?}", t, x))
                .distinct()
                .inspect_batch(|t, x| println!("b: @{:?}: {:?}", t, x))
                .probe()
        });

        // let mut timer = std::time::Instant::now();
        // let mut epoch = Duration::new(0,0);
        // while probe.less_equal(&Duration::new(0,0)) {

        input.send((
            Duration::new(0, 0),
            0,
            Operates(OperatesEvent {
                id: 0,
                name: "DEBUG".to_string(),
                addr: vec![0],
            }),
        ));
        input.send((
            Duration::new(0, 0),
            0,
            Operates(OperatesEvent {
                id: 0,
                name: "DEBUG".to_string(),
                addr: vec![0],
            }),
        ));
        input.send((
            Duration::new(0, 0),
            0,
            Operates(OperatesEvent {
                id: 0,
                name: "DEBUG".to_string(),
                addr: vec![0],
            }),
        ));
        input.send((
            Duration::new(0, 0),
            0,
            Operates(OperatesEvent {
                id: 0,
                name: "DEBUG".to_string(),
                addr: vec![0],
            }),
        ));
        input.send((
            Duration::new(0, 0),
            0,
            Operates(OperatesEvent {
                id: 0,
                name: "DEBUG".to_string(),
                addr: vec![0],
            }),
        ));
        input.send((
            Duration::new(0, 0),
            0,
            Operates(OperatesEvent {
                id: 0,
                name: "DEBUG".to_string(),
                addr: vec![0],
            }),
        ));
        input.advance_to(Duration::new(0, 5));
        // while probe.less_than(&input.time()) {
        //     worker.step();
        // }

        input.send((
            Duration::new(0, 5),
            1,
            Schedule(ScheduleEvent {
                id: 1,
                start_stop: StartStop::Start,
            }),
        ));
        input.advance_to(Duration::new(2, 0));
        while probe.less_than(&input.time()) {
            worker.step();
        }

        input.send((
            Duration::new(2, 0),
            1,
            Schedule(ScheduleEvent {
                id: 5,
                start_stop: StartStop::Start,
            }),
        ));
        input.advance_to(Duration::new(3, 0));

        input.send((
            Duration::new(3, 0),
            1,
            Schedule(ScheduleEvent {
                id: 7,
                start_stop: StartStop::Start,
            }),
        ));
        input.advance_to(Duration::new(4, 0));

        input.send((
            Duration::new(4, 0),
            2,
            Schedule(ScheduleEvent {
                id: 9,
                start_stop: StartStop::Start,
            }),
        ));
        input.advance_to(Duration::new(5, 0));

        input.send((
            Duration::new(5, 0),
            2,
            Schedule(ScheduleEvent {
                id: 2,
                start_stop: StartStop::Start,
            }),
        ));
        input.advance_to(Duration::new(6, 0));
        while probe.less_than(&input.time()) {
            worker.step();
        }

        println!("done: {}s", timer.elapsed().as_secs());
        // while epoch < Duration::new(iterations,0) {
        //     while probe.less_equal(&epoch) {
        //         worker.step();
        //     }
        //     timer = std::time::Instant::now();
        //     epoch += Duration::new(0, 10000000);
        // }

        // let mut epoch = Duration::new(0,0);
        // while epoch < Duration::new(10, 0) {
        //     let timer = std::time::Instant::now();
        //     while probe.less_equal(&epoch) {
        //         worker.step();
        //     }
        //     println!("{}@{:?}: {}", idx, epoch, timer.elapsed().as_millis());
        //     epoch += Duration::new(1,0);
        // }

        // stall application
        // use std::io::stdin;
        // stdin().read_line(&mut String::new()).unwrap();
    })
    .expect("Something went wrong.");
}
