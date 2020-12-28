use std::process;

use clap::{App, Arg};
use fs2::FileExt;
use rand::Rng;

use sled_workload_insert_loop::*;

const DEFAULT_LOOP_COUNT: usize = CYCLE * 10;

fn main() {
    let matches = App::new("insert_loop_workload")
        .arg(
            Arg::with_name("loop_count")
                .takes_value(true)
                .index(1)
                .required(false),
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

    if !crash {
        if let Err(e) = run(loop_count, false) {
            eprintln!("{}", e);
            process::exit(1);
        }
    } else {
        loop {
            let child = unsafe { libc::fork() };
            if child == 0 {
                if let Err(e) = run(loop_count, crash) {
                    eprintln!("{}", e);
                    process::exit(1);
                } else {
                    break;
                }
            } else if child == -1 {
                eprintln!("fork failed, errno is {}", unsafe {
                    *libc::__errno_location()
                });
                process::exit(1);
            } else {
                let mut status: libc::c_int = 0;
                let rv = unsafe { libc::waitpid(child, &mut status as *mut libc::c_int, 0) };
                if rv == -1 {
                    eprintln!("waitpid failed, errno is {}", unsafe {
                        *libc::__errno_location()
                    });
                    process::exit(1);
                }
                match (
                    libc::WIFEXITED(status),
                    libc::WEXITSTATUS(status),
                    libc::WIFSIGNALED(status),
                    libc::WTERMSIG(status),
                ) {
                    (true, 0, _, _) => break,
                    (true, exit_status, _, _) => {
                        eprintln!("child exited with status {}", exit_status);
                        process::exit(1);
                    }
                    (_, _, true, 9) => continue,
                    _ => {
                        eprintln!("child exited abnormally");
                        process::exit(1);
                    }
                }
            }
        }
    }
}

fn run(loop_count: usize, crash: bool) -> Result<(), sled::Error> {
    let crash_during_initialization = rand::thread_rng().gen_bool(0.1);

    if crash && crash_during_initialization {
        start_sigkill_timer();
    }

    // wait for previous crashed process's file lock to be released
    let db_file_path = std::path::PathBuf::from(WORKLOAD_DIR).join("db");
    if db_file_path.is_file() {
        std::fs::File::open(db_file_path)?.lock_exclusive()?;
    }

    let db = config(WORKLOAD_DIR).open()?;

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
