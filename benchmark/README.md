# Benchmark - A PostgreSQL Benchmark executor 

## Installation
```shell
python3 -m venv /home/jan/timescaledb/benchmark/.venv
source .venv/bin/activate
```

## Execution
```shell
python -m src.benchmark --with-commit-ids 453454..53454534 [--repository https://github.com/jnidzwetzki/timescaledb] [--pg=/sr/local/rel_13]
python -m src.benchmark --with-connection pgsq://jan@localhost:5432/test2 --with-connection pgsq://jan@localhost:5432/test2
python -m src.benchmark --with-connection pgsq://jan@localhost:5432/test2 --with-connection reuse-and-pause 

[--benchmark=test1]
```

```
python -m src.benchmark  -v -v --benchmark="chu*"
```

## Benchmark Files
* `generate-data` is executed one time per benchmark and should be used to generate datasets and store them in the filesystem
* `prepare-benchmark` is executed one time per connection and can be used to create tables that should be used later in the benchmark
* `benchmark` is executed one time per connection and the execution time per step is measured (performed by pgbench)

## Run Unit-Tests
```shell
python -m unittest discover
python -m unittest tests.test_pgbench
```
