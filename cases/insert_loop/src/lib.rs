use std::mem::size_of;
use std::path::Path;

use sled::Config;

pub const SEGMENT_SIZE: usize = 1024;
pub const CYCLE: usize = 256;

pub fn config<P: AsRef<Path>>(path: P) -> Config {
    Config::new()
        .cache_capacity(128 * 1024 * 1024)
        .flush_every_ms(Some(1))
        .path(path)
        .segment_size(SEGMENT_SIZE)
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
