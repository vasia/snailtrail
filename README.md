<p align="center">
  <img src="https://github.com/li1/snailtrail/raw/master/snail.png" width="250">
</p>

# SnailTrail

This is a fork of [SnailTrail](https://github.com/strymon-system/snailtrail), a tool to run online critical path analysis on various stream processors (see also the [SnailTrail NSDI'18 Paper](https://doi.org/10.3929/ethz-b-000228581)).

The fork builds upon the original repository and implements further algorithms for analyzing stream processors. It currently focuses on the 0.10 version of [Timely Dataflow and Differential Dataflow](https://github.com/timelydataflow) and won't refrain from breaking existing upstream abstractions (even though they should be relatively easy to add back in at a later point in time).

## Naming conventions

Similar to [Timely Diagnostics](https://github.com/timelydataflow/diagnostics), we will refer to the dataflow that is being analysed as the _source computation_. The workers of the dataflow that is being analysed are the _source peers_, while we unsurprisingly refer to Snailtrail's workers as _SnailTrail peers_.

## Getting Started 

### 1. Attach SnailTrail to a source computation with `timely-adapter`

Attach SnailTrail at `(A)` and `(B)` to any source computation (the example can be found at `timely-adapter/examples/minimal.rs`):

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

**Make sure to place the adapter at the top of the timely closure.** Otherwise, some logging events might not get picked up correctly by SnailTrail.

### 2. Install the SnailTrail CLI (`timely-snailtrail`)

1. Run `cargo install --path timely-snailtrail timely-snailtrail` from the project root.
2. Explore the CLI: `timely-snailtrail --help`

### 3. Inspect your computation

For example, we might want to generate aggregate metrics for an online 2 worker source computation using 2 SnailTrail peers:

1. Run `timely-snailtrail -i 127.0.0.1 -p 1234 -s 2 -w 2 -o metrics.csv`.
2. Attach the source computation by running it with `SNAILTRAIL_ADDR="127.0.0.1:1234"` as env variable.
3. See `metrics.csv` for the aggregate metrics.

## Commands

- `viz` creates an interactive HTML-based PAG visualization (cf. `docs/graphs` for examples). Try it out: `timely-snailtrail -f <path/to/dumps> -s <source peers> viz` -> check `graph.html`
- `metrics` exports aggregate metrics for the source computation (cf. `docs/metrics` for examples). Try it out: `timely-snailtrail -f <path/to/dumps> -s <source peers> metrics` -> check `metrics.csv`

## Online vs. Offline

There are three differences between running online and offline:
1. In offline mode, the source computation is executed as usual — in online mode, you pass `SNAILTRAIL_ADDR=<IP>:<port>` as environment variable.
2. In offline mode, you start the source computation first, then SnailTrail — vice versa in online mode.
3. In offline mode, you pass `-f <path/to/dumps>` as CLA — in online mode, you pass `-i <IP>` and `-p <port>`.

For example:

In offline mode, 
1. Run the source computation. This will generate `*.dump` files.
2. Analyze the generated offline trace with SnailTrail: `timely-snailtrail -f <path/to/dumps> -s <source peers> <subcommand>`
  
In online mode,
1. Run SnailTrail: `timely-snailtrail -i <IP> -p <port> -s <source peers> <subcommand>`
2. Attach the source computation by running it with `SNAILTRAIL_ADDR=<IP>:<port>` set as env variable.

## Examples

Visit `timely-adapter/examples` for source computation examples.

### Show me the code!

Check out the `Structure` section of this `README` for a high-level overview.

The "magic" mostly happens at
- `timely-adapter/src/connect.rs` for logging a computation
- `timely-adapter/src/lib.rs` for the `LogRecord` creation,
- `timely-snailtrail/src/pag.rs` for the `PAG` creation, and the
- `inspect.rs` command, `triangles.rs` source computation, and `minimal.rs` source computation tying it all together.

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

_(WIP)_

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
