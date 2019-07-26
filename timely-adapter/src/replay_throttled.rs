// This code adapted from https://github.com/TimelyDataflow/timely-dataflow/blob/master/timely/src/dataflow/operators/capture/replay.rs

//! Custom replay operator that supports stopping replay arbitrarily
//! and throttling the number of epochs in flight that are introduced by it.
//! It also provides events in order from multiple files. For this to work
//! properly, all events of one epoch have to be written to the same file.

use std::sync::{Arc, atomic::AtomicBool, atomic::Ordering};

use timely::{Data, dataflow::{Scope, Stream}};
use timely::dataflow::channels::pushers::{Counter as PushCounter, buffer::Buffer as PushBuffer};
use timely::dataflow::operators::generic::builder_raw::OperatorBuilder;
use timely::progress::frontier::MutableAntichain;

use timely::dataflow::operators::capture::event::{Event, EventIterator};

use logformat::pair::Pair;
use std::time::Duration;

/// Replay a capture stream into a scope with the same timestamp.
/// This replay operator preserves ordering across an arbitrary amount of files,
/// and can control how many epochs should be put into flight simultaneously.
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

        let mut buffer: Vec<(_, _)> = Vec::new();
        let mut future_progress: Vec<Vec<(Pair<_,_>, i64)>> = Vec::new();

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
                        // apply future progress where possible
                        future_progress.iter().for_each(|vec| {
                            if vec[0].0.first <= f.first + epochs_in_flight {
                                antichain.update_iter(vec.iter().cloned());
                                internal[0].extend(vec.iter().cloned());
                            }
                        });
                        future_progress.retain(|vec| vec[0].0.first > f.first + epochs_in_flight);

                        // consume new events
                        for event_stream in event_streams.iter_mut() {
                            while let Some(event) = event_stream.next() {
                                match event {
                                    Event::Progress(ref vec) => {
                                        if vec[0].0.first <= f.first + epochs_in_flight {
                                            antichain.update_iter(vec.iter().cloned());
                                            internal[0].extend(vec.iter().cloned());
                                        } else {
                                            future_progress.push(vec.clone());
                                            break;
                                        }
                                    },
                                    Event::Messages(time, data) => { buffer.push((time.clone(), data.clone())); }
                                }
                            }
                        }

                        // update frontier information
                        let curr_f = antichain.frontier().to_vec();
                        let curr_f = curr_f.get(0);

                        if let Some(curr_f) = curr_f {
                            // sort buffered events by time
                            buffer.sort_by_key(|(time, _data)| time.clone());

                            // write out buffered events up to the frontier
                            buffer.iter().for_each(|(time, data)| {
                                if time <= curr_f {
                                    total_events += data.len();
                                    output.session(&time).give_iterator(data.iter().cloned());
                                }
                            });
                            buffer.retain(|(time, _data)| time > curr_f);
                        } else {
                            // sort buffered events by time
                            buffer.sort_by_key(|(time, _data)| time.clone());

                            for (time, data) in buffer.drain(..) {
                                total_events += data.len();
                                output.session(&time).give_iterator(data.iter().cloned());
                            }
                        }
                    } else {
                        if !done {
                            total_time += timer.elapsed().as_nanos();
                            info!("w{} replay_throttled: total {}ms", worker, total_time / 1_000_000);
                            info!("w{} replayed {} messages", worker, total_events);
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
