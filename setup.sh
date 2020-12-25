#!/bin/bash
pushd alice/alice-strace
./configure
make
popd

python2 -m virtualenv --python=python2 .venv
.venv/bin/pip install -r alice/requirements.txt
