use sled_workload_batches::*;

fn main() -> Result<(), sled::Error> {
    let (crashed_state_directory, _stdout_file) = checker_arguments();
    let db = config(crashed_state_directory, CACHE_CAPACITY, SEGMENT_SIZE, true).open()?;

    let _ = verify(&db)?;

    Ok(())
}
