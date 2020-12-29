use std::process;

use rand::Rng;

use sled_workload_batches::*;

const DEFAULT_BATCH_COUNT: u32 = 10;
const BATCH_COUNTER_KEY: &[u8] = b"batch_counter";

fn do_batch(i: u32, tree: &sled::Tree) -> Result<(), sled::Error> {
    let mut rng = rand::thread_rng();

    let mut batch = sled::Batch::default();
    if rng.gen_bool(0.1) {
        for key in 0..BATCH_SIZE {
            batch.remove(u32_to_vec(key));
        }
    } else {
        let base_value = u32_to_vec(i);
        for key in 0..BATCH_SIZE {
            let mut value = base_value.clone();
            let additional_len = rng.gen_range(0, SEGMENT_SIZE / 3);
            value.append(&mut vec![0u8; additional_len]);

            batch.insert(u32_to_vec(key), value);
        }
    }
    tree.apply_batch(batch)
}

fn run(batch_count: u32, crash: bool) -> Result<(), sled::Error> {
    let crash_during_initialization = rand::thread_rng().gen_bool(0.1);

    block_on_database_lock(WORKLOAD_DIR)?;

    if crash && crash_during_initialization {
        start_sigkill_timer();
    }

    let db = config(WORKLOAD_DIR).open()?;

    if crash && !crash_during_initialization {
        start_sigkill_timer();
    }

    let mut i = verify(&db)?;

    let counter_option = db.open_tree(BATCH_COUNTER_KEY)?.get(BATCH_COUNTER_KEY)?;
    let counter_start = if let Some(counter_ivec) = counter_option {
        slice_to_u32(&*counter_ivec)
    } else {
        0
    };

    for counter in counter_start..batch_count {
        i += 1;
        do_batch(i, &db)?;
        db.open_tree(BATCH_COUNTER_KEY)?
            .insert(BATCH_COUNTER_KEY, u32_to_vec(counter))?;
    }

    Ok(())
}

fn main() {
    let matches = App::new("batches_workload")
        .arg(
            Arg::with_name("batch_count")
                .index(1)
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("crash")
                .long("crash")
                .short("c")
                .takes_value(false),
        )
        .get_matches();
    let batch_count = if let Some(batch_count) = matches.value_of("batch_count") {
        if let Ok(batch_count) = batch_count.parse() {
            batch_count
        } else {
            eprintln!("{}", matches.usage());
            process::exit(1);
        }
    } else {
        DEFAULT_BATCH_COUNT
    };
    let crash = matches.is_present("crash");

    crash_recovery_loop(run, batch_count, crash);
}
