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

            // @TODO: add an optional checking operator that tests individual logrecord timelines for sanity
            // e.g. sched start -> sched end, no interleave, start & end always belong to scheduling,
            // sent/received always to remote messages, we don't see message types that we can't handle yet,
            // t1 is always > t0, same count of sent and received remote messages for every epoch
            // same results regardless of worker count, received events always have a remote worker
            // matched remote events are (remote-count / 2), remote event count is always even

            let single_worker_edges = make_single_worker_edges(&records);
            let control_edges = make_control_edges(&records);
            // @TODO: DataMessages
            // let data_edges = make_data_edges(&records);

            // @TODO: connect everything

            // PAG Analysis dataflow
            // build_dataflow(config.clone(), records)
            records
                // .inspect(|x| println!("{}", x.0))
                .probe()
        });

        while !probe.done() {
            worker.step();
        }
        println!("done: {}s", timer.elapsed().as_secs());

        // in the end this outputs PagOutput

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

fn make_single_worker_edges<S: Scope<Timestamp = Duration>>(
    records: &Collection<S, LogRecord, isize>,
) -> Collection<S, PagEdge, isize> {
    records
        .map(|x| ((x.epoch, x.local_worker), x))
        .reduce(|_key, input, output| {
            let mut prev_record: Option<&LogRecord> = None;
            let mut prev_node: Option<PagNode> = None;

            for (record, diff) in input {
                if diff > &0 {
                    if prev_record.is_none() {
                        // first node for this worker
                        prev_node = Some(PagNode::from(*record));
                        prev_record = Some(*record);
                    } else {
                        let make_edge_type = |prev: &LogRecord, curr: &LogRecord| {
                            match (prev.event_type, curr.event_type) {
                                // SchedStart ----> SchedEnd
                                (EventType::Start, EventType::End) => ActivityType::Scheduling,
                                // SchedEnd ----> SchedStart
                                (EventType::End, EventType::Start) => ActivityType::BusyWaiting,
                                // something ---> msgreceive
                                (_, EventType::Received) => ActivityType::Waiting,
                                // schedend -> remotesend, remote -> schedstart, remote -> remotesend
                                (_, _) => ActivityType::Unknown, // @TODO
                            }
                        };

                        let make_op_id = |prev: &LogRecord, curr: &LogRecord| {
                            if prev.event_type == EventType::Start
                                && curr.event_type == EventType::End
                            {
                                curr.operator_id
                            } else {
                                None
                            }
                        };

                        let destination = PagNode::from(*record);

                        let edge_type = make_edge_type(&prev_record.unwrap(), &record);
                        let edge = PagEdge {
                            source: prev_node.unwrap(),
                            destination,
                            edge_type,
                            operator_id: make_op_id(&prev_record.unwrap(), &record),
                            traverse: if edge_type == ActivityType::Waiting {
                                TraversalType::Block
                            } else {
                                TraversalType::Unbounded
                            },
                        };

                        output.push((edge, 1));

                        prev_node = Some(destination);
                        prev_record = Some(*record);
                    }
                } else {
                    unimplemented!();
                }
            }
        })
        .map(|(_key, x)| x)
}

fn make_control_edges<S: Scope<Timestamp = Duration>>(
    records: &Collection<S, LogRecord, isize>,
) -> Collection<S, PagEdge, isize> {
    let control_messages_send = records
        .filter(|x| {
            x.activity_type == ActivityType::ControlMessage && x.event_type == EventType::Sent
        })
        .filter(|x| x.epoch == 5)
        .map(|x| ((x.local_worker, x.correlator_id, x.channel_id), x));

    let control_messages_received = records
        .filter(|x| {
            x.activity_type == ActivityType::ControlMessage && x.event_type == EventType::Received
        })
        .filter(|x| x.epoch == 5)
        .map(|x| ((x.remote_worker.unwrap(), x.correlator_id, x.channel_id), x));

    control_messages_send
        .join(&control_messages_received)
        .map(|(_key, (from, to))| PagEdge {
            source: PagNode::from(&from),
            destination: PagNode::from(&to),
            edge_type: ActivityType::ControlMessage,
            operator_id: None,
            traverse: TraversalType::Unbounded,
        })
        .inspect(|x| println!("{:?}", x))
}
