use std::process;

use clap::{App, Arg};
use rand::Rng;

use sled_workload_insert_loop::*;

const DB_DIR: &str = "workload_dir";
const DEFAULT_LOOP_COUNT: usize = CYCLE * 10;

fn main() -> Result<(), sled::Error> {
    let matches = App::new("insert_loop_workload")
        .arg(
            Arg::with_name("loop_count")
                .takes_value(true)
                .index(1)
                .required(false)
        )
        .get_matches();
    let loop_count = if let Some(loop_count) = matches.value_of("loop_count") {
        if let Ok(loop_count) = loop_count.parse() {
            loop_count
        } else {
            eprintln!("{}", matches.usage());
            process::exit(1);
        }
    } else {
        DEFAULT_LOOP_COUNT
    };

    let db = config(DB_DIR).open()?;

    let mut hu = 0;
    for _ in 0..loop_count {
        hu += 1;

        if hu / CYCLE >= CYCLE {
            hu = 0;
        }

        let key = u32_to_vec((hu % CYCLE) as u32);

        let mut value = u32_to_vec((hu / CYCLE) as u32);
        let additional_len = rand::thread_rng().gen_range(0, SEGMENT_SIZE / 3);
        value.append(&mut vec![0u8; additional_len]);

        db.insert(&key, value)?;
    }

    Ok(())
}
