#!/bin/bash

export ALICE_HOME=alice
PATH=$PATH:alice/bin

cargo build --release

alice-check --traces_dir=traces_dir --checker=target/release/sled-checker
