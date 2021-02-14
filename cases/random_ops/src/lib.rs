use std::{
    collections::BTreeMap,
    io::{self, BufRead, BufReader, Read},
};

use rand::Rng;

pub use common_utils::*;

pub const SEGMENT_SIZE: usize = 256;
pub const CACHE_CAPACITY: usize = 256;
pub const BATCH_COUNTER_KEY: &[u8] = b"batch_counter";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Set,
    Del(u8),
    Id,
    Batched(Vec<BatchOp>),
    Restart,
    Flush,
    DelayedCrash,
    CrashAndRecoveryVirtualOp(u32),
    IdResultVirtualOp(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOp {
    Set,
    Del(u8),
}

#[derive(Debug)]
pub struct OpDecodeError;

impl Op {
    pub fn generate<R: Rng>(rng: &mut R, crash: bool) -> Op {
        if crash && rng.gen_bool(1. / 30.) {
            return Op::DelayedCrash;
        }
        if rng.gen_bool(1. / 10.) {
            return Op::Restart;
        }
        match rng.gen_range(0, 5) {
            0 => Op::Set,
            1 => Op::Del(rng.gen()),
            2 => Op::Id,
            3 => {
                let size = rng.gen_range(0, 20);
                let mut ops = Vec::with_capacity(size);
                ops.resize_with(size, || BatchOp::generate(rng));
                Op::Batched(ops)
            }
            4 => Op::Flush,
            _ => unreachable!(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        match self {
            Op::Set => vec![b's'],
            Op::Del(key) => format!("d{}", key).into_bytes(),
            Op::Id => vec![b'i'],
            Op::Batched(ops) => {
                let mut output = Vec::with_capacity(16);
                output.push(b'b');
                for op in ops.iter() {
                    match op {
                        BatchOp::Set => output.push(b's'),
                        BatchOp::Del(key) => output.append(&mut format!("d{}", key).into_bytes()),
                    }
                }
                output
            }
            Op::Restart => vec![b'-'],
            Op::Flush => vec![b'f'],
            Op::DelayedCrash => vec![b'_'],
            Op::CrashAndRecoveryVirtualOp(batch_counter) => {
                format!("c{}", batch_counter).into_bytes()
            }
            Op::IdResultVirtualOp(id) => format!("I{}", id).into_bytes(),
        }
    }

    pub fn decode(data: &[u8]) -> Result<Op, OpDecodeError> {
        fn parse_u8(data: &[u8]) -> Result<u8, OpDecodeError> {
            if data.len() == 0 {
                return Err(OpDecodeError);
            }
            let mut value: u8 = 0;
            for byte in data {
                if *byte >= b'0' && *byte <= b'9' {
                    value = value
                        .checked_mul(10)
                        .ok_or(OpDecodeError)?
                        .checked_add(byte - b'0')
                        .ok_or(OpDecodeError)?;
                } else {
                    return Err(OpDecodeError);
                }
            }
            Ok(value)
        }

        fn parse_u32(data: &[u8]) -> Result<u32, OpDecodeError> {
            if data.len() == 0 {
                return Err(OpDecodeError);
            }
            let mut value: u32 = 0;
            for byte in data {
                if *byte >= b'0' && *byte <= b'9' {
                    value = value
                        .checked_mul(10)
                        .ok_or(OpDecodeError)?
                        .checked_add((byte - b'0') as u32)
                        .ok_or(OpDecodeError)?;
                } else {
                    return Err(OpDecodeError);
                }
            }
            Ok(value)
        }

        fn parse_u64(data: &[u8]) -> Result<u64, OpDecodeError> {
            if data.len() == 0 {
                return Err(OpDecodeError);
            }
            let mut value: u64 = 0;
            for byte in data {
                if *byte >= b'0' && *byte <= b'9' {
                    value = value
                        .checked_mul(10)
                        .ok_or(OpDecodeError)?
                        .checked_add((byte - b'0') as u64)
                        .ok_or(OpDecodeError)?;
                } else {
                    return Err(OpDecodeError);
                }
            }
            Ok(value)
        }

        if data.len() == 0 {
            return Err(OpDecodeError);
        }
        match data[0] {
            b's' => Ok(Op::Set),
            b'd' => Ok(Op::Del(parse_u8(&data[1..])?)),
            b'i' => Ok(Op::Id),
            b'b' => {
                let mut ops = vec![];
                let mut data = &data[1..];
                while !data.is_empty() {
                    match data[0] {
                        b's' => {
                            ops.push(BatchOp::Set);
                            data = &data[1..];
                        }
                        b'd' => {
                            let number_end = if let Some(pos) = data
                                .iter()
                                .skip(1)
                                .position(|byte| *byte < b'0' || *byte > b'9')
                            {
                                pos + 1
                            } else {
                                data.len()
                            };
                            ops.push(BatchOp::Del(parse_u8(&data[1..number_end])?));
                            data = &data[number_end..];
                        }
                        _ => return Err(OpDecodeError),
                    }
                }
                Ok(Op::Batched(ops))
            }
            b'-' => Ok(Op::Restart),
            b'f' => Ok(Op::Flush),
            b'_' => Ok(Op::DelayedCrash),
            b'c' => Ok(Op::CrashAndRecoveryVirtualOp(parse_u32(&data[1..])?)),
            b'I' => Ok(Op::IdResultVirtualOp(parse_u64(&data[1..])?)),
            _ => Err(OpDecodeError),
        }
    }
}

impl BatchOp {
    fn generate<R: Rng>(rng: &mut R) -> BatchOp {
        if rng.gen::<bool>() {
            BatchOp::Set
        } else {
            BatchOp::Del(rng.gen::<u8>())
        }
    }
}

#[derive(Debug)]
pub enum OpReaderError {
    Decode(OpDecodeError),
    IO(io::Error),
}

impl From<OpDecodeError> for OpReaderError {
    fn from(e: OpDecodeError) -> OpReaderError {
        OpReaderError::Decode(e)
    }
}

impl From<io::Error> for OpReaderError {
    fn from(e: io::Error) -> OpReaderError {
        OpReaderError::IO(e)
    }
}

pub struct OpReader<R: Read> {
    reader: BufReader<R>,
    buffer: Vec<u8>,
}

impl<R: Read> OpReader<R> {
    pub fn new(input: R) -> OpReader<R> {
        OpReader {
            reader: BufReader::new(input),
            buffer: vec![],
        }
    }
}

impl<R: Read> Iterator for OpReader<R> {
    type Item = Result<Op, OpReaderError>;

    fn next(&mut self) -> Option<Result<Op, OpReaderError>> {
        self.buffer.clear();
        let res = self.reader.read_until(b'\n', &mut self.buffer);
        let count = match res {
            Ok(count) => count,
            Err(e) => return Some(Err(e.into())),
        };
        if count == 0 || *self.buffer.last().unwrap() != b'\n' {
            return None;
        }
        match Op::decode(&self.buffer[..self.buffer.len() - 1]) {
            Ok(op) => Some(Ok(op)),
            Err(e) => Some(Err(e.into())),
        }
    }
}

pub fn decode_value(bytes: &[u8]) -> u16 {
    if bytes[0] % 4 != 0 {
        assert_eq!(bytes.len(), 2);
    }
    (u16::from(bytes[0]) << 8) + u16::from(bytes[1])
}

pub fn value_factory(set_counter: u16) -> Vec<u8> {
    let hi = (set_counter >> 8) as u8;
    let lo = set_counter as u8;
    let mut val = vec![hi, lo];
    if hi % 4 == 0 {
        val.extend(vec![
            lo;
            hi as usize * SEGMENT_SIZE / 4 * set_counter as usize
        ]);
    }
    val
}

#[derive(Debug)]
pub struct ReferenceVersion {
    value: Option<u16>,
    batch: Option<u32>,
}

#[derive(Debug)]
pub struct ReferenceEntry {
    versions: Vec<ReferenceVersion>,
    crash_epoch: u32,
}

#[derive(Debug)]
pub struct Reference {
    pub map: BTreeMap<u16, ReferenceEntry>,
    pub set_counter: u16,
    pub max_id: isize,
    pub crash_counter: u32,
    pub batch_counter: u32,
}

// For each Set operation, one entry is inserted to the tree with a two-byte
// key, and a variable-length value. The key is set to the encoded value
// of the `set_counter`, which increments by one with each Set
// operation. The value starts with the same two bytes as the
// key does, but some values are extended to be many segments long.
//
// Del operations delete one entry from the tree. Only keys from 0 to 255
// are eligible for deletion.

impl Reference {
    pub fn new() -> Reference {
        Reference {
            map: BTreeMap::new(),
            set_counter: 0,
            max_id: -1,
            crash_counter: 0,
            batch_counter: 1,
        }
    }

    pub fn update_before(&mut self, op: &Op) {
        match op {
            Op::Set => {
                // Update the reference to show that this key could be present.
                // The next Flush operation will update the reference again,
                // and require this key to be present (unless there's a crash
                // before then).
                let crash_counter_copy = self.crash_counter;
                let entry = self
                    .map
                    .entry(self.set_counter)
                    .or_insert_with(|| ReferenceEntry {
                        versions: vec![ReferenceVersion {
                            value: None,
                            batch: None,
                        }],
                        crash_epoch: crash_counter_copy,
                    });
                entry.versions.push(ReferenceVersion {
                    value: Some(self.set_counter),
                    batch: None,
                });
                entry.crash_epoch = self.crash_counter;
            }
            Op::Del(k) => {
                let crash_counter_copy = self.crash_counter;
                self.map.entry(u16::from(*k)).and_modify(|v| {
                    v.versions.push(ReferenceVersion {
                        value: None,
                        batch: None,
                    });
                    v.crash_epoch = crash_counter_copy;
                });
            }
            Op::Id => {}
            Op::Batched(batch_ops) => {
                let crash_counter_copy = self.crash_counter;
                let batch_counter_copy = self.batch_counter;
                for batch_op in batch_ops {
                    match batch_op {
                        BatchOp::Set => {
                            let entry = self.map.entry(self.set_counter).or_insert_with(|| {
                                ReferenceEntry {
                                    versions: vec![ReferenceVersion {
                                        value: None,
                                        batch: None,
                                    }],
                                    crash_epoch: crash_counter_copy,
                                }
                            });
                            entry.versions.push(ReferenceVersion {
                                value: Some(self.set_counter),
                                batch: Some(batch_counter_copy),
                            });
                            entry.crash_epoch = crash_counter_copy;
                            self.set_counter += 1;
                        }
                        BatchOp::Del(key) => {
                            self.map.entry(u16::from(*key)).and_modify(|v| {
                                v.versions.push(ReferenceVersion {
                                    value: None,
                                    batch: Some(batch_counter_copy),
                                });
                                v.crash_epoch = crash_counter_copy;
                            });
                        }
                    }
                }
            }
            Op::Restart => {}
            Op::Flush => {}
            Op::DelayedCrash => {}
            Op::CrashAndRecoveryVirtualOp(batch_counter) => {
                self.crash_counter += 1;
                prune_reference(&mut self.map, *batch_counter);
            }
            Op::IdResultVirtualOp(id) => {
                assert!(
                    *id as isize > self.max_id,
                    "generated id of {} is not larger \
                     than previous max id of {}",
                    id,
                    self.max_id,
                );
                self.max_id = *id as isize;
            }
        }
    }

    pub fn update_after(&mut self, op: &Op) {
        match op {
            Op::Set => self.set_counter += 1,
            Op::Del(_) => {}
            Op::Id => {}
            Op::Batched(_) => self.batch_counter += 1,
            Op::Restart => {}
            Op::Flush => {
                // Once a flush has been successfully completed, recent Set/Del
                // operations should be durable. Go through the reference, and
                // if a Set/Del operation was done since the last crash, keep
                // the value for that key corresponding to the most recent
                // operation, and toss the rest.
                for (_key, entry) in self.map.iter_mut() {
                    if entry.versions.len() > 1 && entry.crash_epoch == self.crash_counter {
                        entry.versions.drain(..entry.versions.len() - 1);
                    }
                }
            }
            Op::DelayedCrash => {}
            Op::CrashAndRecoveryVirtualOp(_) => {}
            Op::IdResultVirtualOp(_) => {}
        }
    }
}

fn construct_reference(ops: &[Op]) -> Reference {
    let mut reference = Reference::new();
    for op in ops {
        reference.update_before(op);
        reference.update_after(op);
    }
    reference
}

fn prune_reference(reference: &mut BTreeMap<u16, ReferenceEntry>, stable_batch: u32) {
    for (_, ref_entry) in reference.iter_mut() {
        if ref_entry.versions.len() == 1 {
            continue;
        }
        // find the last version from a stable batch, if there is one,
        // and throw away all preceeding versions
        let committed_find_result = ref_entry.versions.iter().enumerate().rev().find(
            |(_, ReferenceVersion { batch, value: _ })| match batch {
                Some(batch) => *batch <= stable_batch,
                None => false,
            },
        );
        if let Some((committed_index, _)) = committed_find_result {
            let tail_versions = ref_entry.versions.split_off(committed_index);
            ref_entry.versions = tail_versions;
        }
        // find the first version from a batch that wasn't committed,
        // throw away it and all subsequent versions
        let discarded_find_result = ref_entry.versions.iter().enumerate().find(
            |(_, ReferenceVersion { batch, value: _ })| match batch {
                Some(batch) => *batch > stable_batch,
                None => false,
            },
        );
        if let Some((discarded_index, _)) = discarded_find_result {
            ref_entry.versions.truncate(discarded_index);
        }
    }
}

pub fn verify_against_ops(tree: &sled::Tree, ops: &[Op]) -> Result<Reference, sled::Error> {
    let mut reference = construct_reference(ops);
    verify_against_reference(tree, &mut reference)?;
    Ok(reference)
}

pub fn verify_against_reference(
    tree: &sled::Tree,
    reference: &mut Reference,
) -> Result<(), sled::Error> {
    let mut ref_iter = reference.map.iter().map(|(ref rk, ref rv)| (**rk, *rv));
    for res in tree.iter() {
        let tree_key = &*res?.0;
        if tree_key == BATCH_COUNTER_KEY {
            continue;
        }
        let actual = decode_value(tree_key);

        // make sure the tree value is in the reference
        while let Some((ref_key, ref_expected)) = ref_iter.next() {
            if ref_expected
                .versions
                .iter()
                .all(|version| version.value.is_none())
            {
                // this key should not be present in the tree, skip it and move on to the
                // next entry in the reference
                continue;
            } else if ref_expected
                .versions
                .iter()
                .all(|version| version.value.is_some())
            {
                // this key must be present in the tree, check if the keys from both iterators match
                assert_eq!(
                    actual, ref_key,
                    "expected to iterate over key {:?} but got {:?} instead due to it being \
                     missing in\n\ntree: {:?}\n\nreference: {:?}\n",
                    ref_key, actual, tree, &reference.map
                );
                break;
            } else {
                // according to the reference, this key could either be present or absent,
                // depending on whether recent writes were successful. check whether the
                // keys from the two iterators match, if they do, the key happens to be
                // present, which is okay, if they don't and the tree iterator is further
                // ahead than the reference iterator, the key happens to be absent, so we
                // skip the entry in the reference. if the reference iterator ever gets
                // further than the tree iterator, that means the tree has a key that it
                // should not.
                if actual == ref_key {
                    // tree and reference agree, we can move on to the next tree item
                    break;
                } else if ref_key > actual {
                    // we have a bug, the reference iterator should always be <= tree
                    // (this means that the key `actual` was in the tree, but it wasn't in
                    // the reference, so the reference iterator has advanced on past `actual`)
                    panic!(
                        "tree verification failed: expected {:?} got {:?}\
                         \n\ntree: {:?}\n\nreference: {:?}\n",
                        ref_key, actual, tree, &reference.map
                    );
                } else {
                    // we are iterating through the reference until we have an item that
                    // must be present or an uncertain item that matches the tree's real
                    // item anyway
                    continue;
                }
            }
        }
    }

    while let Some((ref_key, ref_expected)) = ref_iter.next() {
        if ref_expected
            .versions
            .iter()
            .all(|version| version.value.is_some())
        {
            // this key had to be present, but we got to the end of the tree without
            // seeing it
            panic!(
                "tree verification failed: expected {:?} got end\nexpected: {:?}\ntree: {:?}",
                ref_key, ref_expected, tree
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{BatchOp, Op};

    #[test]
    fn op_serialization_round_trip() {
        assert_eq!(Op::decode(&Op::encode(&Op::Set)).unwrap(), Op::Set);
        assert_eq!(Op::decode(&Op::encode(&Op::Del(0))).unwrap(), Op::Del(0));
        assert_eq!(
            Op::decode(&Op::encode(&Op::Del(255))).unwrap(),
            Op::Del(255)
        );
        assert_eq!(Op::decode(&Op::encode(&Op::Id)).unwrap(), Op::Id);
        assert_eq!(
            Op::decode(&Op::encode(&Op::Batched(vec![
                BatchOp::Set,
                BatchOp::Del(0),
                BatchOp::Del(255)
            ])))
            .unwrap(),
            Op::Batched(vec![BatchOp::Set, BatchOp::Del(0), BatchOp::Del(255)])
        );
        assert_eq!(Op::decode(&Op::encode(&Op::Restart)).unwrap(), Op::Restart);
        assert_eq!(Op::decode(&Op::encode(&Op::Flush)).unwrap(), Op::Flush);
        assert_eq!(
            Op::decode(&Op::encode(&Op::DelayedCrash)).unwrap(),
            Op::DelayedCrash
        );
        assert_eq!(
            Op::decode(&Op::encode(&Op::CrashAndRecoveryVirtualOp(1))).unwrap(),
            Op::CrashAndRecoveryVirtualOp(1)
        );
        assert_eq!(
            Op::decode(&Op::encode(&Op::CrashAndRecoveryVirtualOp(78))).unwrap(),
            Op::CrashAndRecoveryVirtualOp(78)
        );
        assert_eq!(
            Op::decode(&Op::encode(&Op::IdResultVirtualOp(0))).unwrap(),
            Op::IdResultVirtualOp(0)
        );
        assert_eq!(
            Op::decode(&Op::encode(&Op::IdResultVirtualOp(123456))).unwrap(),
            Op::IdResultVirtualOp(123456)
        );
    }
}
