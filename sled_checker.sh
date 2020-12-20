#!/bin/bash

export ALICE_HOME=/home/david/Code/alice
PATH=$PATH:/home/david/Code/alice/bin

cargo build --release

alice-check --traces_dir=traces_dir --checker=target/release/sled-checker
