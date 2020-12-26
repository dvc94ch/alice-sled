const RANDOM_BYTES: &[u8] = include_bytes!("../../random.bin");

fn shift_bytes_by(shift: usize) -> sled::IVec {
    let mut buf = Vec::with_capacity(RANDOM_BYTES.len());
    for i in 0..RANDOM_BYTES.len() {
        buf.push(RANDOM_BYTES[(i + shift) % RANDOM_BYTES.len()]);
    }
    buf.into()
}

fn main() -> Result<(), sled::Error> {
    let db = sled::open("workload_dir")?;
    for i in 0..10 {
        let key = shift_bytes_by(i);
        let value = shift_bytes_by(i + 10);
        db.insert(key, value)?;
    }
    db.flush()?;
    println!("Flushed");
    Ok(())
}
