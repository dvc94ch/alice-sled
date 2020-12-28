use sled_workload_insert_loop::*;

/// Verifies that the keys in the tree are correctly recovered.
fn main() -> Result<(), sled::Error> {
    // key 0 should always have the highest value, as that's where we increment
    // at some key, the values may go down by one
    // no other values than these two should be seen

    let mut args = std::env::args().skip(1);
    let crashed_state_directory = args.next().unwrap();
    let _stdout_file = args.next().unwrap();
    let db = config(crashed_state_directory).open()?;

    let mut iter = db.iter();
    let highest = if let Some(res) = iter.next() {
        let (_k, v) = res?;
        slice_to_u32(&*v)
    } else {
        return Ok(());
    };
    let highest_vec = u32_to_vec(highest);

    // find out how far we got
    let mut lowest = 0;
    for res in &mut iter {
        let (_k, v) = res?;
        if v[..4] != highest_vec[..4] {
            let expected = if highest == 0 {
                CYCLE as u32 - 1
            } else {
                (highest - 1) % CYCLE as u32
            };
            let actual = slice_to_u32(&*v);
            assert_eq!(expected, actual);
            lowest = actual;
            break;
        }
    }

    // ensure nothing changes after this point
    for res in iter {
        let (k, v) = res?;
        assert_eq!(
            slice_to_u32(&*v),
            lowest,
            "expected key {} to have value {}, instead it had value {} in db: {:?}",
            slice_to_u32(&*k),
            lowest,
            slice_to_u32(&*v),
            db
        );
    }

    db.verify_integrity()?;

    Ok(())
}
