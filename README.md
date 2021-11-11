# Commands

```
git clone --recursive https://github.com/divergentdave/alice-sled.git
cd alice-sled
./setup.sh

./run_workload.sh basic
./run_checker.sh basic

./run_workload.sh insert_loop 20
./run_checker.sh insert_loop

./run_workload.sh insert_loop 20 --crash
./run_checker.sh insert_loop

./run_workload.sh batches 5
./run_checker.sh batches

./run_workload.sh batches 5 --crash
./run_checker.sh batches

./run_workload.sh random_ops 40
./run_checker.sh random_ops

./run_workload.sh random_ops 40 --crash --flusher
./run_checker.sh random_ops

(requires installation of https://github.com/sambayless/monosat first)
./run_workload.sh transactions
./run_checker.sh transactions
```
