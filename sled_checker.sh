#!/bin/bash

export ALICE_HOME=/home/david/Source/alice
PATH=$PATH:/home/david/Source/alice/bin

cargo build --release

alice-check --traces_dir=traces_dir --checker=target/release/sled-checker
