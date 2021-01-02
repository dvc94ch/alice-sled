use std::env;
use std::error;
use std::fs;
use std::io;
use std::mem::size_of;
use std::path::Path;
use std::process;
use std::thread;
use std::time::Duration;

pub use clap::{crate_version, App, Arg};
use fs2::FileExt;
use rand::Rng;
pub use sled;
use sled::Config;

pub const WORKLOAD_DIR: &str = "workload_dir";

pub fn config<P: AsRef<Path>>(
    path: P,
    cache_capacity: u64,
    segment_size: usize,
    flusher: bool,
) -> Config {
    Config::new()
        .cache_capacity(cache_capacity)
        .flush_every_ms(if flusher { Some(1) } else { None })
        .path(path)
        .segment_size(segment_size)
}

pub fn checker_arguments() -> (String, String) {
    let mut args = env::args().skip(1);
    let crashed_state_directory = args.next().unwrap();
    let stdout_file = args.next().unwrap();
    (crashed_state_directory, stdout_file)
}

pub fn start_sigkill_timer() {
    thread::spawn(|| {
        let runtime = rand::thread_rng().gen_range(0, 60);
        thread::sleep(Duration::from_millis(runtime));
        unsafe {
            libc::raise(9);
        }
    });
}

/// This function provides the scaffolding and unsafe libc calls required for a crash
/// recovery test. It takes a function to be called in the forked child process, an
/// argument to be passed to that function, and a boolean indicating whether the crash
/// recovery loop should be used, or if instead forking should be skipped, and the
/// function should be called once immediately.
pub fn crash_recovery_loop<F: Fn(I, bool) -> Result<(), E>, I, E: error::Error>(
    function: F,
    argument: I,
    crash: bool,
) -> ! {
    crash_recovery_loop_with_hooks(|| {}, function, || {}, || {}, argument, crash)
}

/// This is the same as `crash_recovery_loop`, but with three more callbacks, `setup` is
/// called before forking, `parent_after_fork` is called from the parent after forking,
/// and `teardown` is called from the parent after the child process has exited.
pub fn crash_recovery_loop_with_hooks<
    S: Fn(),
    F: Fn(I, bool) -> Result<(), E>,
    P: Fn(),
    T: Fn(),
    I,
    E: error::Error,
>(
    setup: S,
    function: F,
    parent_after_fork: P,
    teardown: T,
    argument: I,
    crash: bool,
) -> ! {
    if !crash {
        if let Err(e) = function(argument, false) {
            eprintln!("{}", e);
            process::exit(1);
        } else {
            process::exit(0);
        }
    }
    loop {
        setup();
        let child = unsafe { libc::fork() };
        if child == 0 {
            if let Err(e) = function(argument, true) {
                eprintln!("{}", e);
                process::exit(1);
            } else {
                process::exit(0);
            }
        } else if child == -1 {
            parent_after_fork();
            teardown();
            eprintln!("fork failed, errno is {}", unsafe {
                *libc::__errno_location()
            });
            process::exit(1);
        } else {
            parent_after_fork();
            let mut status: libc::c_int = 0;
            let rv = unsafe { libc::waitpid(child, &mut status as *mut libc::c_int, 0) };
            if rv == -1 {
                teardown();
                eprintln!("waitpid failed, errno is {}", unsafe {
                    *libc::__errno_location()
                });
                process::exit(1);
            }
            teardown();
            match (
                libc::WIFEXITED(status),
                libc::WEXITSTATUS(status),
                libc::WIFSIGNALED(status),
                libc::WTERMSIG(status),
            ) {
                (true, 0, _, _) => process::exit(0),
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

pub fn block_on_database_lock<P: AsRef<Path>>(directory: P) -> io::Result<()> {
    let db_file_path = directory.as_ref().join("db");
    if db_file_path.is_file() {
        let file = fs::File::open(db_file_path)?;
        file.lock_exclusive()?;
    }
    Ok(())
}

pub fn u32_to_vec(u: u32) -> Vec<u8> {
    let buf: [u8; size_of::<u32>()] = u.to_be_bytes();
    buf.to_vec()
}

pub fn slice_to_u32(b: &[u8]) -> u32 {
    let mut buf = [0u8; size_of::<u32>()];
    buf.copy_from_slice(&b[..size_of::<u32>()]);

    u32::from_be_bytes(buf)
}
