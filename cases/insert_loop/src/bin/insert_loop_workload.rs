use std::env;
use std::process;

use rand::Rng;

use sled_workload_insert_loop::*;

fn main() -> Result<(), sled::Error> {
    const DEFAULT_LOOP_COUNT: usize = CYCLE * 10;
    let loop_count = if let Some(argument) = env::args().skip(1).next() {
        if argument.is_empty() {
            DEFAULT_LOOP_COUNT
        } else if let Ok(loop_count) = argument.parse() {
            loop_count
        } else {
            eprintln!("Could not parse loop count argument");
            process::exit(1);
        }
    } else {
        DEFAULT_LOOP_COUNT
    };

    let db = config().open()?;

    let mut hu = 0;
    for _  in 0..loop_count {
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
