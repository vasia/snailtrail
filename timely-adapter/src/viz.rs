/// Takes a stream of `TimelyEvent`s and returns a crude
/// reconstructed String representation of its dataflow.
fn reconstruct_dataflow<S: Scope<Timestamp = Duration>>(
    stream: &Stream<S, (Duration, usize, TimelyEvent)>,
) {
    let operates = stream
        .filter(|(_, worker, _)| *worker == 0)
        .flat_map(|(t, _worker, x)| {
            if let Operates(event) = x {
                Some(event)
            } else {
                None
            }
        })
        .inspect_batch(|_t, xs| {
            use std::fs::OpenOptions;
            use std::io::prelude::*;

            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .open("ops.edn")
                .unwrap();

            for x in xs {
                writeln!(
                    &mut file,
                    "{{ :id {} :addr {:?} :name {} }}",
                    x.id, x.addr, x.name
                )
                .unwrap();
            }
        });

    let channels = stream
        .filter(|(_, worker, _)| *worker == 0)
        .flat_map(|(t, _worker, x)| {
            if let Channels(event) = x {
                Some(event)
            } else {
                None
            }
        })
        .inspect_batch(|_t, xs| {
            use std::fs::OpenOptions;
            use std::io::prelude::*;

            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .open("chs.edn")
                .unwrap();

            for x in xs {
                let mut src_addr = x.scope_addr.clone();
                let mut target_addr = x.scope_addr.clone();
                if x.source.0 != 0 {
                    src_addr.push(x.source.0);
                }
                if x.target.0 != 0 {
                    target_addr.push(x.target.0);
                }
                writeln!(
                    &mut file,
                    "{{ :id {} :src {:?} :target {:?} :scope {:?} }}",
                    x.id, src_addr, target_addr, x.scope_addr
                )
                .unwrap();
            }
        });

    // let operates = stream
    //     .filter(|(_, worker, _)| *worker == 0)
    //     .flat_map(|(t, _worker, x)| if let Operates(event) = x {Some(((event.addr.clone(), event), t, 1))} else {None})
    //     .as_collection();
    // .inspect(|x| println!("{:?}", x));

    // let channels_source = stream
    //     .filter(|(_, worker, _)| *worker == 0)
    //     .flat_map(|(t, _worker, x)| if let Channels(event) = x {
    //         // key by source
    //         let mut absolute_addr = event.scope_addr.clone();
    //         absolute_addr.push(event.source.0);
    //         Some(((absolute_addr, event), t, 1))
    //     } else {None})
    //     .as_collection();
    // .inspect(|x| println!("{:?}", x));

    // channels_source
    //     // join source
    //     .join(&operates)
    //     .map(|(_key, (ch, op_src))| {
    //         // key by target
    //         let mut absolute_addr = ch.scope_addr.clone();
    //         absolute_addr.push(ch.target.0);
    //         (absolute_addr, (ch, op_src))
    //     })
    //     // join target
    //     .join(&operates)
    //     .map(|(_key, ((ch, op_src), op_target))|
    //          format!("{:?} Ch{}: ({}, {}) -> ({}, {})", ch.scope_addr, ch.id, op_src.id, op_src.name, op_target.id, op_target.name))
    //     .inspect(|x| println!("{:?}", x.0));
}
