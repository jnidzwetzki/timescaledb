import sys
import typing
import logging
import tempfile
import distutils.spawn

from typing import Any
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import yaml

logger = logging.getLogger(__name__)

def benchmark_files_find(included_benchmarks: str) -> typing.List[str]:
    # Use all benchmarks if no are included
    if len(included_benchmarks) == 0:
        included_benchmarks.append('*')

    logger.debug('Included benchmarks are: %s', included_benchmarks)

    # Determine benchmark files
    benchmark_files = []
    for benchmark_name in included_benchmarks:
        benchmarks = Path('benchmarks').glob('**/' + benchmark_name + '.yml')
        for filename in benchmarks:
            benchmark_files.append(str(filename))

    return benchmark_files


def benchmark_file_parse(benchmark_file: str) -> Any:
    with open(benchmark_file, encoding="utf-8") as file:
        return yaml.load(file, Loader=yaml.FullLoader)

def benchmark_files_find_and_parse(included_benchmarks: str) -> Any:
    benchmark_files = benchmark_files_find(included_benchmarks)
    return list(map(benchmark_file_parse, benchmark_files))

def open_database_connection(connection_url: str) -> Any:
    connection_url_parsed = urlparse(connection_url)
    username = connection_url_parsed.username
    password = connection_url_parsed.password
    database = connection_url_parsed.path[1:]
    hostname = connection_url_parsed.hostname
    port = connection_url_parsed.port

    connection = psycopg2.connect(
        database=database,
        user=username,
        password=password,
        host=hostname,
        port=port
    )

    return connection


def replace_placeholder_in_query(query: str, tmpdir: str) -> str:

    if '%%WORK_DIR%%' in query:
        query = query.replace('%%WORK_DIR%%', tmpdir)

    return query

def generate_benchmark_data(benchmark_spec: Any, connection_url: str, tmpdir: str) -> None:
    print(f"*** Generating data for benchmark '{benchmark_spec['name']}'")
    logger.debug('Temp dir is %s', tmpdir)

    if 'generate-data' not in benchmark_spec:
        logger.debug("No data generation is specified")
        return

    if 'steps' not in benchmark_spec['generate-data']:
        logger.debug("No data generation steps are specified")
        return

    steps = benchmark_spec['generate-data']['steps']

    try:
        connection = open_database_connection(connection_url)
        cur = connection.cursor()

        for step in steps:
            logger.debug('Performing step \'%s\'', step['name'])

            query_with_placeholder = step['run']
            query = replace_placeholder_in_query(query_with_placeholder, tmpdir)
            logger.debug('Query: %s', query)

            cur.execute(query)

        cur.commit()
        cur.close()
        connection.close()
    except psycopg2.OperationalError as error:
        print(f'Unable to connect to the database {connection_url}')
        print(f'{error}')
        sys.exit(1)

def execute_benchmark(benchmark_spec: Any, connection_url: str, tmpdir: str) -> None:
    print(f"*** Executing benchmark '{benchmark_spec['name']}'")

    try:
        connection = open_database_connection(connection_url)

        # Generate data

        # Prepare experiment

        connection.close()
    except psycopg2.OperationalError as error:
        print(f'Unable to connect to the database {connection_url}')
        print(f'{error}')
        sys.exit(1)


def locate_pg_bench() -> str:
    pgbench = distutils.spawn.find_executable("pgbench")
    logger.debug("Using pgbench binary: %s", pgbench)
    return pgbench


def execute_benchmarks_on_connections(included_benchmarks: str, connections: list[str]) -> None:

    benchmark_specs = benchmark_files_find_and_parse(included_benchmarks)
    pgbench = locate_pg_bench()

    with tempfile.TemporaryDirectory() as tmpdir:

        # Generate benchmark data (the first connection is used to generate the benchmark data)
        for benchmark_spec in benchmark_specs:
            generate_benchmark_data(benchmark_spec, connections[0], tmpdir)

        # Execute benchmarks on connections 
        # All benchmarks are run one after another per connection. This reduces 
        # the number of connection changes (e.g., when 'reuse-and-pause' is used).
        for index, connection in enumerate(connections):

            # Reuse the previous connection URL
            if connection == "reuse-and-pause":
                if index == 0:
                    raise ValueError(
                        "reuse-and-pause can not be used as the first connection URL")

                connection = connections[index - 1]
                input(
                    "Reusing connection, please make the needed adjustments and press enter...")

            print(f'*** Processing benchmarks on connection: {connection}')

            for benchmark_spec in benchmark_specs:
                execute_benchmark(benchmark_spec, connection, tmpdir)

    print("*** All benchmarks are executed - DONE")


def execute_benchmarks_by_commits(included_benchmarks: str, commit1: str,
                                  commit2: str, pg_path: str, repository: str) -> None:

    logger.debug("Performing benchmark of commit %s and %s of repository %s with PG %s",
                 commit1, commit2, repository, pg_path)

    benchmark_files = benchmark_files_find(included_benchmarks)
    pgbench = locate_pg_bench()
    with tempfile.TemporaryDirectory() as tmpdir:

        # Checkout repository
        print("*** Cloning repository %s", repository)

        # Build commit 1
        print("*** Building commit %s", commit1)

        # Start PostgreSQL for commit 1
        print("*** Starting Postgres for commit %s", commit1)

        # Process benchmarks for commit 1
        print("*** Executing benchmarks for commit %s", commit1)
        for benchmark_file in benchmark_files:
            execute_benchmark(benchmark_file, None, tmpdir)

        # Stop Postgres for commit 1
        print("*** Stopping postgres for commit %s", commit1)

        # Build commit w
        print("*** Building commit %s", commit2)

        # Start PostgreSQL for commit w

        # Process benchmarks for commit w
        for benchmark_file in benchmark_files:
            execute_benchmark(benchmark_file, None, tmpdir)

        # Stop Postgres for commit w

    print("*** Benchmarks done")
