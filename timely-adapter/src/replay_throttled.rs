// This code adapted from https://github.com/TimelyDataflow/timely-dataflow/blob/master/timely/src/dataflow/operators/capture/replay.rs
//
// Timely Dataflow carries the following license:
//
// The MIT License (MIT)
//
// Copyright (c) 2014 Frank McSherry
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

//! Custom replay operator that supports stopping replay arbitrarily
//! and throttling the number of epochs in flight that are introduced by it.

use std::sync::{Arc, atomic::AtomicBool, atomic::Ordering};

use timely::{Data, dataflow::{Scope, Stream}, progress::Timestamp};
use timely::dataflow::channels::pushers::{Counter as PushCounter, buffer::Buffer as PushBuffer};
use timely::dataflow::operators::generic::builder_raw::OperatorBuilder;
use timely::progress::frontier::MutableAntichain;

use timely::dataflow::operators::capture::event::{Event, EventIterator};

use logformat::pair::Pair;
use std::time::Duration;

/// Replay a capture stream into a scope with the same timestamp.
pub trait ReplayThrottled<D: Data + std::fmt::Debug> {
    /// Replays `self` into the provided scope, as a `Stream<S, D>`.
    fn replay_throttled_into<S: Scope<Timestamp=Pair<u64, Duration>>>(self, worker: usize, scope: &mut S, is_running: Option<Arc<AtomicBool>>, epochs_in_flight: u64) -> Stream<S, D>;
}

impl<D: Data + std::fmt::Debug, I> ReplayThrottled<D> for I
where I : IntoIterator,
      <I as IntoIterator>::Item: EventIterator<Pair<u64, Duration>, D>+'static {
    fn replay_throttled_into<S: Scope<Timestamp=Pair<u64, Duration>>>(self, worker: usize, scope: &mut S, is_running: Option<Arc<AtomicBool>>, epochs_in_flight: u64) -> Stream<S, D> {

        let mut builder = OperatorBuilder::new("ReplayThrottled".to_owned(), scope.clone());

        let address = builder.operator_info().address;
        let activator = scope.activator_for(&address[..]);

        let (targets, stream) = builder.new_output();

        let mut output = PushBuffer::new(PushCounter::new(targets));
        let mut event_streams = self.into_iter().collect::<Vec<_>>();
        let mut started = false;

        let mut antichain: MutableAntichain<Pair<u64, Duration>> = MutableAntichain::new();

        let mut buffer: Vec<Vec<_>> = Vec::new();

        for _ in event_streams.iter() {
            buffer.push(vec![]);
        }

        let mut file_read = 2 * event_streams.len() + 1;

        let mut timer = std::time::Instant::now();
        let mut total = 0;

        builder.build(
            move |_frontier| { },
            move |_consumed, internal, produced| {
                if !started {
                    // The first thing we do is modify our capabilities to match the number of streams we manage.
                    // This should be a simple change of `self.event_streams.len() - 1`. We only do this once, as
                    // our very first action.
                    internal[0].update(Default::default(), (event_streams.len() as i64) - 1);
                    antichain.update_iter(Some((Default::default(), (event_streams.len() as i64))).into_iter());

                    started = true;
                }

                let frontier = antichain.frontier().to_vec();
                let frontier = frontier.get(0);

                let running = if let Some(x) = &is_running {
                    x.load(Ordering::Acquire)
                } else {
                    true
                };

                if running {
                    for (idx, event_stream) in event_streams.iter_mut().enumerate() {
                        if let Some(event) = event_stream.next() {
                            buffer[idx].push(event.clone());
                            while let Some(event) = event_stream.next() {
                                buffer[idx].push(event.clone());
                            }
                        } else {
                            if file_read > 0 {
                                file_read -= 1;
                            }
                            if file_read == 1 {
                                println!("{}@{:?} file read in {}ms", worker, frontier.unwrap(), timer.elapsed().as_millis());
                            }
                        }
                    }

                    // println!("{} - {:?}", worker, antichain.frontier().to_vec());

                    if file_read == 0 {
                        timer = std::time::Instant::now();

                        for (idx, event_stream) in buffer.iter_mut().enumerate() {
                            let drainable = event_stream.iter().filter(|event| {
                                if let Some(f) = frontier {
                                    match event {
                                        Event::Progress(ref vec) => {
                                            let epoch = vec[0].0.first;

                                            // we should never see a progress message for
                                            // a new time that is earlier than the frontier
                                            assert!(epoch >= f.first);

                                            epoch >= f.first && epoch <= f.first + epochs_in_flight
                                        },
                                        Event::Messages(ref time, ref _data) => {
                                            // all messages from previous times should've been
                                            // processed already
                                            assert!(time.first >= f.first);

                                            time.first >= f.first && time.first <= f.first + epochs_in_flight - 1
                                        }
                                    }
                                } else {
                                    false
                                }
                            }).count();

                            // push to stream
                            for event in event_stream.drain(..drainable) {
                                match event {
                                    Event::Progress(ref vec) => {
                                        let mut vec = vec.clone();
                                        if vec[0].1 == -1 {
                                            println!("w{}@{:?} replay time: {}ms", worker, frontier.unwrap(), total / 1_000_000);
                                        }

                                        // println!("{}@{}: {:?}", worker, idx, vec);
                                        antichain.update_iter(vec.iter().cloned());
                                        internal[0].extend(vec.iter().cloned());
                                    },
                                    Event::Messages(ref time, ref data) => {
                                        // println!("{}@{}: data{:?}", worker, idx, time);
                                        output.session(time).give_iterator(data.iter().cloned());
                                    }
                                }
                            }
                        }

                        total += timer.elapsed().as_nanos();
                    }

                    // Always reschedule `replay`.
                    activator.activate();

                    output.cease();
                    output.inner().produced().borrow_mut().drain_into(&mut produced[0]);
                } else {
                    while !antichain.is_empty() {
                        let elements = antichain.frontier().iter().map(|t| (t.clone(), -1)).collect::<Vec<_>>();
                        for (t, c) in elements.iter() {
                            internal[0].update(t.clone(), *c);
                        }
                        antichain.update_iter(elements);
                    }
                }

                false
            }
        );

        stream
    }
}
