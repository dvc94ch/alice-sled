use sled_workload_insert_loop::*;

fn main() -> Result<(), sled::Error> {
    let mut args = std::env::args().skip(1);
    let crashed_state_directory = args.next().unwrap();
    let _stdout_file = args.next().unwrap();
    let db = config(crashed_state_directory).open()?;

    let _ = verify(&db)?;

    Ok(())
}
