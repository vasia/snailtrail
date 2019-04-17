// Copyright 2019 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use timely_pag::dataflow::{Config, end_to_end_analysis};

fn main() {
    let config = Config {
        timely_args: vec!["-w".to_string(), "2".to_string()],
        source_peers: 2,
    };

    end_to_end_analysis(config).unwrap();
}
