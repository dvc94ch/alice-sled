const RANDOM_BYTES: &[u8] = include_bytes!("../../random.txt");

fn shift_bytes_by(shift: usize) -> sled::IVec {
    let mut buf = Vec::with_capacity(RANDOM_BYTES.len());
    for i in 0..RANDOM_BYTES.len() {
        buf.push(RANDOM_BYTES[(i + shift) % RANDOM_BYTES.len()]);
    }
    buf.into()
}

fn main() -> anyhow::Result<()>{
    let crashed_state_directory =  std::env::args().skip(1).next().unwrap();
    println!("{}", crashed_state_directory);
    let db = sled::open(crashed_state_directory)?;
    for i in 0..10 {
        let key = shift_bytes_by(i);
        let value = shift_bytes_by(i + 10);
        assert_eq!(db.get(key)?, Some(value));
    }
    Ok(())
}
