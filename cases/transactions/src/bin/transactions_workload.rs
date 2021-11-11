use std::{
    collections::BTreeSet,
    convert::TryInto,
    process,
    sync::{Arc, Barrier},
    thread,
    time::Instant,
};

use rand::{distributions::Distribution, Rng};

use sled_workload_transactions::*;

const DEFAULT_TRANSACTION_COUNT: usize = 10;
const DEFAULT_OPS_PER_TX: usize = 4;
const DEFAULT_THREADS: usize = 4;
const DEFAULT_CARDINALITY: usize = 25;
const DEFAULT_MAX_BYTE_LENGTH: usize = 512;
const DEFAULT_WRITE_PROBABILITY: f64 = 0.4;
const DEFAULT_DELETE_PROBABILITY: f64 = 0.1;

fn bytes_factory<R: Rng>(rng: &mut R, max_byte_length: usize) -> Vec<u8> {
    let beta_statistic = rand_distr::Beta::new(1.2, 10.0).unwrap().sample(rng);
    let max_byte_length_float: f64 = TryInto::<u32>::try_into(max_byte_length).unwrap().into();
    let length_float = max_byte_length_float * beta_statistic;
    let length = std::cmp::max(length_float as usize, 1);
    let mut vec = Vec::with_capacity(length);
    vec.resize_with(length, || rng.gen());
    vec
}

fn build_key_space<R: Rng>(
    rng: &mut R,
    max_byte_length: usize,
    cardinality: usize,
) -> Vec<Vec<u8>> {
    let mut keys = BTreeSet::new();
    while keys.len() < cardinality {
        keys.insert(bytes_factory(rng, max_byte_length));
    }
    keys.into_iter().collect()
}

fn main() {
    let matches = App::new("transactions_workload")
        .version(crate_version!())
        .arg(
            Arg::with_name("transactions")
                .index(1)
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("operations_per_transaction")
                .long("ops_per_transaction")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("threads")
                .long("threads")
                .short("j")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("cardinality")
                .long("cardinality")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("max_byte_length")
                .long("max_byte_length")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("write_probability")
                .long("write_probability")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("delete_probability")
                .long("delete_probability")
                .required(false)
                .takes_value(true),
        )
        .get_matches();
    let transaction_count = if let Some(transactions) = matches.value_of("transactions") {
        if let Ok(transactions) = transactions.parse() {
            transactions
        } else {
            eprintln!("{}", matches.usage());
            process::exit(1);
        }
    } else {
        DEFAULT_TRANSACTION_COUNT
    };
    let ops_per_tx =
        if let Some(operations_per_transaction) = matches.value_of("operations_per_transaction") {
            if let Ok(operations_per_transaction) = operations_per_transaction.parse() {
                operations_per_transaction
            } else {
                eprintln!("{}", matches.usage());
                process::exit(1);
            }
        } else {
            DEFAULT_OPS_PER_TX
        };
    let thread_count = if let Some(threads) = matches.value_of("threads") {
        if let Ok(threads) = threads.parse() {
            threads
        } else {
            eprintln!("{}", matches.usage());
            process::exit(1);
        }
    } else {
        DEFAULT_THREADS
    };
    let cardinality = if let Some(cardinality) = matches.value_of("cardinality") {
        if let Ok(cardinality) = cardinality.parse() {
            cardinality
        } else {
            eprintln!("{}", matches.usage());
            process::exit(1);
        }
    } else {
        DEFAULT_CARDINALITY
    };
    let max_byte_length = if let Some(max_byte_length) = matches.value_of("max_byte_length") {
        if let Ok(max_byte_length) = max_byte_length.parse() {
            max_byte_length
        } else {
            eprintln!("{}", matches.usage());
            process::exit(1);
        }
    } else {
        DEFAULT_MAX_BYTE_LENGTH
    };
    if max_byte_length == 0 {
        eprintln!("max_byte_length cannot be zero");
        process::exit(1);
    }
    let write_probability = if let Some(write_probability) = matches.value_of("write_probability") {
        if let Ok(write_probability) = write_probability.parse() {
            write_probability
        } else {
            eprintln!("{}", matches.usage());
            process::exit(1);
        }
    } else {
        DEFAULT_WRITE_PROBABILITY
    };
    let delete_probability =
        if let Some(delete_probability) = matches.value_of("delete_probability") {
            if let Ok(delete_probability) = delete_probability.parse() {
                delete_probability
            } else {
                eprintln!("{}", matches.usage());
                process::exit(1);
            }
        } else {
            DEFAULT_DELETE_PROBABILITY
        };

    // Generate transactions consisting of random operations.
    // Constraints:
    // * Each transaction reads any given key at most once
    // * Each transaction writes any given key at most once
    // * One transaction can't read and write to the same key
    // (then order of operations within a transaction would matter, and that's annoying)
    let mut rng = rand::thread_rng();
    let key_space = build_key_space(&mut rng, max_byte_length, cardinality);
    let mut transactions = Vec::with_capacity(transaction_count);
    transactions.resize_with(transaction_count, || {
        let mut ops = Vec::with_capacity(ops_per_tx);
        let mut keys_used = BTreeSet::new();
        while ops.len() < ops_per_tx && ops.len() < key_space.len() {
            let key_idx = rng.gen_range(0..key_space.len());
            if !keys_used.insert(key_idx) {
                continue;
            }
            if rng.gen_bool(write_probability) {
                let key = key_space[key_idx].clone();
                let op = if rng.gen_bool(delete_probability) {
                    Operation::Remove(RemoveOperation { key })
                } else {
                    Operation::Insert(InsertOperation {
                        key,
                        value: bytes_factory(&mut rng, max_byte_length),
                    })
                };
                ops.push(op);
            } else {
                let key = key_space[key_idx].clone();
                ops.push(Operation::Get(GetOperation { key }));
            }
        }
        TransactionSpec { ops }
    });

    println!("{}", serde_json::to_string(&transactions).unwrap());

    let db_config = config(WORKLOAD_DIR, CACHE_CAPACITY, SEGMENT_SIZE, true);
    let db = Arc::new(db_config.open().unwrap());

    let mut handles = Vec::new();
    let barrier = Arc::new(Barrier::new(thread_count));
    let transactions = Arc::new(transactions);
    let chunk_size = transaction_count / thread_count
        + if transaction_count % thread_count > 0 {
            1
        } else {
            0
        };
    let t0 = Instant::now();
    for thread_idx in 0..thread_count {
        let db = db.clone();
        let barrier = Arc::clone(&barrier);
        let transactions = Arc::clone(&transactions);
        handles.push(thread::spawn(move || {
            let transactions = &*transactions;
            let transaction_idx_range = std::cmp::min(thread_idx * chunk_size, transactions.len())
                ..std::cmp::min((thread_idx + 1) * chunk_size, transactions.len());
            barrier.wait();
            for (transaction_idx, transaction) in transaction_idx_range
                .clone()
                .zip(transactions[transaction_idx_range].iter())
            {
                let start_instant = Instant::now();
                let start = (start_instant - t0).as_nanos();
                let output = TransactionOutput::Start(TransactionStartOutput {
                    transaction_idx,
                    start,
                });
                let serialized = serde_json::to_string(&output).unwrap();
                println!("{}", serialized);

                let get_results: Vec<Option<Vec<u8>>> = db
                    .transaction::<_, _, ()>(|tree| {
                        transaction
                            .ops
                            .iter()
                            .map(|op| {
                                Ok(match op {
                                    Operation::Get(GetOperation { key }) => {
                                        tree.get(key)?.map(|value| value.as_ref().to_owned())
                                    }
                                    Operation::Insert(InsertOperation { key, value }) => {
                                        tree.insert(key.clone(), value.clone())?;
                                        None
                                    }
                                    Operation::Remove(RemoveOperation { key }) => {
                                        tree.remove(key.clone())?;
                                        None
                                    }
                                })
                            })
                            .collect()
                    })
                    .unwrap();

                let end_instant = Instant::now();
                let end = (end_instant - t0).as_nanos();
                let output = TransactionOutput::End(TransactionEndOutput {
                    transaction_idx,
                    end,
                    get_results,
                });
                let serialized = serde_json::to_string(&output).unwrap();
                println!("{}", serialized);
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
}
