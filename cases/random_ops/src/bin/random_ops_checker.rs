use std::fs::File;

use sled_workload_random_ops::*;

fn main() -> Result<(), sled::Error> {
    let (crashed_state_directory, stdout_file) = checker_arguments();
    let ops: Vec<Op> = OpReader::new(File::open(stdout_file)?)
        .map(Result::unwrap)
        .collect();

    let db = config(crashed_state_directory, CACHE_CAPACITY, SEGMENT_SIZE, true)
        .idgen_persist_interval(1)
        .open()?;

    let mut reference = verify_against_ops(&db, &ops)?;

    let id = db.generate_id()?;
    let virtual_op = Op::IdResultVirtualOp(id);
    reference.update_before(&virtual_op);

    Ok(())
}
