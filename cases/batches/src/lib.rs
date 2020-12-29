pub use common_utils::*;

pub const BATCH_SIZE: u32 = 8;

pub fn verify(tree: &sled::Tree) -> Result<u32, sled::Error> {
    let mut iter = tree.iter();
    let first_value = if let Some(res) = iter.next() {
        let (_k, v) = res?;
        slice_to_u32(&*v)
    } else {
        return Ok(0);
    };
    for key in 0..BATCH_SIZE {
        let option = tree.get(u32_to_vec(key))?;
        let v = match option {
            Some(v) => v,
            None => panic!(
                "expected key {} to have a value, instead it was missing",
                key,
            ),
        };
        let value = slice_to_u32(&*v);
        assert_eq!(
            first_value, value,
            "expected key {} to have value {}, instead it had value {}",
            key, first_value, value
        );
    }

    tree.verify_integrity()?;

    Ok(first_value)
}
