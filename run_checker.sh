#!/bin/bash

CASE="$1"
if [[ -z $CASE || ! -d "cases/$CASE" ]];
then
    echo "Usage: $0 CASE_NAME"
    exit 2
fi

TOPSRCDIR="$(realpath "$(dirname "$0")")"
export ALICE_HOME="$TOPSRCDIR/alice"
PATH="$PATH:$TOPSRCDIR/alice/bin"
source "$TOPSRCDIR/.venv/bin/activate"

cd "$TOPSRCDIR/cases/$CASE"

set -e
cargo build --release

alice-check --traces_dir=traces_dir \
    --checker="$TOPSRCDIR/target/release/${CASE}_checker"
