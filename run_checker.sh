#!/bin/bash

CASE=basic

TOPSRCDIR="$(realpath "$(dirname "$0")")"
export ALICE_HOME="$TOPSRCDIR/alice"
PATH="$PATH:$TOPSRCDIR/alice/bin"
source "$TOPSRCDIR/.venv/bin/activate"

cd "$TOPSRCDIR/cases/$CASE"

cargo build --release

alice-check --traces_dir=traces_dir \
    --checker="$TOPSRCDIR/target/release/${CASE}_checker"
