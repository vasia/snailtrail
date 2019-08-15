use timely::dataflow::InputHandle;
use timely::dataflow::operators::{Input, Exchange, Inspect, Probe};
use timely::dataflow::operators::map::Map;

use timely::logging::TimelyEvent;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        // (A) Create SnailTrail adapter at the beginning of the worker closure
        use st2_timely::connect::Adapter;
        let adapter = Adapter::attach(worker);

        // worker
        //     .log_register()
        //     .insert::<TimelyEvent, _>("timely", move |_time, data| {
        //         for datum in data {
        //             let (_t, w, datum) = datum;
        //             match datum {
        //                 TimelyEvent::Operates(_) | TimelyEvent::Channels(_) |
        //                 TimelyEvent::Messages(_) | TimelyEvent::Schedule(_) |
        //                 TimelyEvent::Text(_) => {
        //                     println!("w-{} {:?}", w, datum);
        //                 },
        //                 _ => {}
        //             }
        //         }
        //     });

        // Some computation
        let mut input = InputHandle::new();
        let index = worker.index();
        let probe = worker.dataflow(|scope| {
            scope.input_from(&mut input)
                .map(|x| x + 1 as u64)
                .exchange(|x| *x)
                .inspect(|x| {})
                // .inspect_batch(move |t,x| println!("w{}: {}", index, x.len()))
                .probe()
        });

        // let logger = worker.log_register().get::<TimelyEvent>("timely").expect("timely logger not found");
        for round in 0..5 {
            if worker.index() == 0 {
                for x in 0 .. (round + 1) * 20000 {
                    input.send(x);
                }
            }
            input.advance_to(round + 1);
            while probe.less_than(input.time()) { worker.step_or_park(None); }

            // (B) Communicate epoch completion
            println!("w{} closed {}", worker.index(), round);
            adapter.tick_epoch();
            // logger.log(TimelyEvent::Text("epoch done".to_string()));
        }
    }).unwrap();
}
