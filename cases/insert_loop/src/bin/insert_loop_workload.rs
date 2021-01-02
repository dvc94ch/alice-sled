use std::process;

use rand::Rng;

use sled_workload_insert_loop::*;

const DEFAULT_LOOP_COUNT: usize = 40;

fn main() {
    let matches = App::new("insert_loop_workload")
        .version(crate_version!())
        .arg(
            Arg::with_name("loop_count")
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
    let crash = matches.is_present("crash");

    crash_recovery_loop(run, loop_count, crash);
}

fn run(loop_count: usize, crash: bool) -> Result<(), sled::Error> {
    let crash_during_initialization = rand::thread_rng().gen_bool(0.1);

    // wait for previous crashed process's file lock to be released
    block_on_database_lock(WORKLOAD_DIR)?;

    if crash && crash_during_initialization {
        start_sigkill_timer();
    }

    let db = config(WORKLOAD_DIR, CACHE_CAPACITY, SEGMENT_SIZE, true).open()?;

    if crash && !crash_during_initialization {
        start_sigkill_timer();
    }

    let (key, highest, wrap_count) = verify(&db)?;
    let mut wrap_count = wrap_count;

    let mut hu = ((highest as usize) * CYCLE) + key as usize;
    assert_eq!(hu % CYCLE, key as usize);
    assert_eq!(hu / CYCLE, highest as usize);
    while (hu + CYCLE * CYCLE * wrap_count as usize) < loop_count {
        hu += 1;

        if hu / CYCLE >= CYCLE {
            hu = 0;
            wrap_count += 1;
            db.open_tree(WRAP_COUNT_KEY)?
                .insert(WRAP_COUNT_KEY, u32_to_vec(wrap_count))?;
        }

        let key = u32_to_vec((hu % CYCLE) as u32);

        let mut value = u32_to_vec((hu / CYCLE) as u32);
        let additional_len = rand::thread_rng().gen_range(0, SEGMENT_SIZE / 3);
        value.append(&mut vec![0u8; additional_len]);

        db.insert(&key, value)?;
    }

    Ok(())
}
