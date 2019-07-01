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

        let len = event_streams.len();
        let mut buffer = Vec::with_capacity(len);
        for _ in 0 .. len {
            buffer.push(Vec::new());
        }

        let mut antichain: MutableAntichain<Pair<u64, Duration>> = MutableAntichain::new();

        let mut started = false;

        let mut total_events = 0;
        let mut total_time = 0;
        let mut done = false;

        builder.build(
            move |_frontier| { },
            move |_consumed, internal, produced| {
                let timer = std::time::Instant::now();

                if !started {
                    // The first thing we do is modify our capabilities to match the number of streams we manage.
                    // This should be a simple change of `self.event_streams.len() - 1`. We only do this once, as
                    // our very first action.
                    internal[0].update(Default::default(), (event_streams.len() as i64) - 1);
                    antichain.update_iter(Some((Default::default(), (event_streams.len() as i64))).into_iter());

                    started = true;
                }

                let running = if let Some(x) = &is_running {
                    x.load(Ordering::Acquire)
                } else {
                    true
                };

                if running {
                    let frontier = antichain.frontier().to_vec();
                    let frontier = frontier.get(0);

                    if let Some(f) = frontier {

                        // loops as long as we haven't read in `epochs_in_flight` epochs
                        let mut should_continue = true;
                        while should_continue {
                            for (idx, event_stream) in event_streams.iter_mut().enumerate() {
                                while let Some(event) = event_stream.next() {
                                    buffer[idx].push(event.clone());

                                    if let Event::Progress(ref vec) = event {
                                        let epoch = vec[0].0.first;

                                        // yield once enough epochs are in flight,
                                        // or if we're retracting all capabilities
                                        if (epoch > f.first + (epochs_in_flight - 1)) || vec[0].1 < 0 {
                                            should_continue = false;
                                        }

                                        // progress to next stream after each epoch
                                        if epoch > f.first {
                                            break;
                                        }
                                    }
                                }
                            }

                            // sort by epoch time
                            buffer.sort_by_key(|events| {
                                if let Some(event) = events.get(0) {
                                    match event {
                                        Event::Progress(ref vec) => vec[0].0.first,
                                        Event::Messages(ref time, _data) => time.first
                                    }
                                } else { 0 }
                            });

                            // provide sorted events to computation
                            for events in buffer.iter_mut() {
                                for event in events.drain(..) {
                                    match event {
                                        Event::Progress(ref vec) => {
                                            let epoch = vec[0].0.first;

                                            antichain.update_iter(vec.iter().cloned());
                                            internal[0].extend(vec.iter().cloned());
                                        },
                                        Event::Messages(ref time, ref data) => {
                                            total_events += 1;
                                            output.session(time).give_iterator(data.iter().cloned());
                                        }
                                    }
                                }
                            }

                        }
                    } else {
                        if !done {
                            total_time += timer.elapsed().as_nanos();
                            println!("w{} replay_throttled: total {}ms", worker, total_time / 1_000_000);
                            println!("w{} replayed {} messages", worker, total_events);
                            done = true;
                        }
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

                total_time += timer.elapsed().as_nanos();

                false
            }
        );

        stream
    }
}
