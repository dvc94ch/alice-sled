use std::env;
use std::path::Path;
use std::thread;
use std::time::Duration;

use rand::Rng;
pub use sled;
use sled::Config;

pub const WORKLOAD_DIR: &str = "workload_dir";
pub const SEGMENT_SIZE: usize = 1024;

pub fn config<P: AsRef<Path>>(path: P) -> Config {
    Config::new()
        .cache_capacity(128 * 1024 * 1024)
        .flush_every_ms(Some(1))
        .path(path)
        .segment_size(SEGMENT_SIZE)
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
