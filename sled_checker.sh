#!/bin/bash

export ALICE_HOME=/home/dvc/ipld/alice
PATH=$PATH:/home/dvc/ipld/alice/bin

cargo build --release

alice-check --traces_dir=traces_dir --checker=target/release/sled-checker
