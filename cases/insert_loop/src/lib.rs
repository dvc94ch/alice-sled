pub use common_utils::*;

pub const CYCLE: usize = 256;
pub const WRAP_COUNT_KEY: &[u8] = b"wrap_count";

/// Verifies that the keys in the tree are correctly recovered.
pub fn verify(db: &sled::Db) -> Result<(u32, u32, u32), sled::Error> {
    // key 0 should always have the highest value, as that's where we increment
    // at some key, the values may go down by one
    // no other values than these two should be seen

    let tree: &sled::Tree = &*db;
    let mut iter = tree.iter();
    let highest = if let Some(res) = iter.next() {
        let (_k, v) = res?;
        slice_to_u32(&*v)
    } else {
        return Ok((0, 0, 0));
    };
    let highest_vec = u32_to_vec(highest);

    // find out how far we got
    let mut contiguous = 0;
    let mut lowest = 0;
    for res in &mut iter {
        let (_k, v) = res?;
        if v[..4] == highest_vec[..4] {
            contiguous += 1;
        } else {
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
            tree
        );
    }

    tree.verify_integrity()?;

    let wrap_count =
        if let Some(wrap_count_ivec) = db.open_tree(WRAP_COUNT_KEY)?.get(WRAP_COUNT_KEY)? {
            slice_to_u32(&*wrap_count_ivec)
        } else {
            0
        };

    Ok((contiguous, highest, wrap_count))
}
