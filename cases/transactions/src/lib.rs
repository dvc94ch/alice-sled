use serde::{Deserialize, Serialize};

pub use common_utils::*;

pub const SEGMENT_SIZE: usize = 256;
pub const CACHE_CAPACITY: usize = 256;

#[derive(Debug, Serialize, Deserialize)]
pub struct GetOperation {
    pub key: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertOperation {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RemoveOperation {
    pub key: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Operation {
    Get(GetOperation),
    Insert(InsertOperation),
    Remove(RemoveOperation),
}

impl Operation {
    pub fn key(&self) -> &[u8] {
        match self {
            Operation::Get(GetOperation { key }) => key,
            Operation::Insert(InsertOperation { key, .. }) => key,
            Operation::Remove(RemoveOperation { key }) => key,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionSpec {
    pub ops: Vec<Operation>,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionStartOutput {
    pub transaction_idx: usize,
    pub start: u128,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionEndOutput {
    pub transaction_idx: usize,
    pub end: u128,
    pub get_results: Vec<Option<Vec<u8>>>,
}

#[derive(Serialize, Deserialize)]
pub enum TransactionOutput {
    Start(TransactionStartOutput),
    End(TransactionEndOutput),
}
