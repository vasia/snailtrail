<p align="center">
  <img src="https://github.com/li1/snailtrail/raw/master/snail.png" width="250">
</p>

# SnailTrail

This is a fork of [SnailTrail](https://github.com/strymon-system/snailtrail), a tool to run online critical path analysis on various stream processors (see also the [SnailTrail NSDI'18 Paper](https://doi.org/10.3929/ethz-b-000228581)).

The fork builds upon the original repository and implements further algorithms for analyzing stream processors. It currently focuses on the 0.10 version of [Timely Dataflow and Differential Dataflow](https://github.com/timelydataflow) and won't refrain from breaking existing upstream abstractions (even though they should be relatively easy to add back in at a later point in time).

## Getting Started 

### 1. Attach SnailTrail to a source computation via `timely-adapter`

Use `timely-adapter/examples/minimal.rs` as a starting point:

```rust
use timely_adapter::connect::Adapter;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        // (A) Create SnailTrail adapter at the beginning of the worker closure
        let adapter = Adapter::attach(worker);

        // Some computation
        let mut input = InputHandle::new();
        let probe = worker.dataflow(|scope|
            scope.input_from(&mut input)
                 .exchange(|x| *x)
                 .inspect(move |x| println!("hello {}", x))
                 .probe()
        );

        for round in 0..100 {
            if worker.index() == 0 { input.send(round); }
            input.advance_to(round + 1);
            while probe.less_than(input.time()) { worker.step(); }

            // (B) Communicate epoch completion
            adapter.tick_epoch();
        }
    }).unwrap();
}
```

**Make sure to place the adapter at the top of the timely closure.**

### 2. Run in offline mode

1. Run the source computation as you would normally.
2. To analyze the generated offline trace with SnailTrail, from `timely-snailtrail`, run `cargo run --example inspect <# source computation peers> false`.

### 3. Run in online mode

1. From `timely-snailtrail`, run `cargo run --example inspect <# source computation peers> true <IP> <PORT>`.
2. Attach the source computation by running it with `SNAILTRAIL_ADDR=<IP>:<PORT>` set as env variable.

## Examples

- Visit `timely-adapter/examples` for source computation examples.
- Visit `timely-snailtrail/examples/inspect.rs` for a basic SnailTrail inspector example.

### Show me the code!

Check out the `Structure` section of this `README` for a high-level overview.

The "magic" mostly happens at
- `timely-adapter/src/connect.rs` for logging a computation
- `timely-adapter/src/lib.rs` for the `LogRecord` creation,
- `timely-snailtrail/src/pag.rs` for the `PAG` creation, and the
- `inspect.rs`, `triangles.rs`, and `minimal.rs` tying it all together.

## Structure

### Overview

(Roughly in order of appearance)

#### In this repository

|Type | Crate    | Description |
| --------- | -------- | ----------- |
| adapter | `timely-adapter` | timely / differential 0.9 adapter |
| infrastructure | `logformat` | Shared definitions of core data types and serialization of traces. |
| infrastructure, algorithms | `timely-snailtrail` | PAG generation & algorithms for timely 0.9 with epochal semantics. |

#### Upstream

|Type | Crate    | Description |
| --------- | -------- | ----------- |
| adapter | `spark-parser` | Spark adapter |
| adapter | `tensorflow` | TensorFlow adapter |
| adapter | Flink  | not publicly available |
| adapter | Timely < 0.9 | not publicly available |
| adapter | Heron  | not publicly available |
| infrastructure | `logformat` | Shared definitions of core data types and serialization of traces (in Rust, Java). |
| infrastructure | `pag-construction` | Constructs the Program Activity Graph (PAG) from a flat stream of events which denote the start/end of computation and communication. Also has scripts to generate various plots. |
| algorithms | `snailtrail` | Calculates a ranking for PAG edges by computing how many times an edge appears in the set of all-pairs shortest paths ("critical participation", cf. the paper). |

### Adapters

Adapters read log traces from a stream processor (or a serialized representation) and convert the logged messages to `logformat`'s `LogRecord` representation. This representation can then be used for PAG construction.

Depending on the stream processor, window semantics also come into play here. For example, the `timely-adapter` currently uses an epoch-based window, which should make many algorithms on the PAG easier than working on a fixed window PAG.

### Infrastructure

Glue code, type definitions, (de)serialization, and intermediate representations that connect adapters to algorithms.

### Algorithms

Implementation of various algorithms that run on top of the PAG to provide insights into the analyzed distributed dataflow's health and performance.

## Docs

See the `docs` subfolder for additional documentation. Of course, also check out the `examples` and code documentation built with `cargo doc`.

## Resources

* [Hoffmann et al.: **SnailTrail Paper** (NSDI '18)](https://doi.org/10.3929/ethz-b-000228581)
* [Malte Sandstede: **A Short Introduction to SnailTrail** (ETH '19)](https://github.com/li1/talks/raw/master/snailtrail.pdf)
* [Vasia Kalavri: **Towards self-managed, re-configurable streaming dataflow systems** (UGENT '19)](https://www.youtube.com/watch?v=E947ynd_vGI)
* [Moritz Hoffmann: **SnailTrail: Generalizing Critical Paths for Online Analysis of Distributed Dataflows** (NSDI '18)](https://www.youtube.com/watch?v=h5kPd59v0U0)
* [Vasia Kalavri: **Online performance analysis of distributed dataflow systems** (O'Reilly Velocity London '18)](https://www.youtube.com/watch?v=AUQJkjx1Uh8)

## License

SnailTrail is primarily distributed under the terms of both the MIT license and the Apache License (Version 2.0), with portions covered by various BSD-like licenses.

See LICENSE-APACHE, and LICENSE-MIT for details.
