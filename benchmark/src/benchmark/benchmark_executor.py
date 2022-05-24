import os
import sys
import time
import typing
import logging
import tempfile
import subprocess
import distutils.spawn

from typing import Any
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import yaml

from .pgbench_util import parse_pgbench_result
from .result_reporter import AbstractResultReporter

logger = logging.getLogger(__name__)

class BenchmarkExecutor:

    def __init__(self, included_benchmarks: str, result_reporter: AbstractResultReporter):
        self.benchmark_specs = BenchmarkExecutor.benchmark_files_find_and_parse(included_benchmarks)
        self.result_reporter = result_reporter
        self.pg_bench = BenchmarkExecutor.locate_pg_bench()

    @staticmethod
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

    @staticmethod
    def benchmark_file_parse(benchmark_file: str) -> Any:
        with open(benchmark_file, encoding="utf-8") as file:
            return yaml.load(file, Loader=yaml.FullLoader)

    @staticmethod
    def benchmark_files_find_and_parse(included_benchmarks: str) -> Any:
        benchmark_files = BenchmarkExecutor.benchmark_files_find(included_benchmarks)
        return list(map(BenchmarkExecutor.benchmark_file_parse, benchmark_files))

    @staticmethod
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

    @staticmethod
    def replace_placeholder_in_query(query: str, tmpdir: str) -> str:

        if '%%WORK_DIR%%' in query:
            query = query.replace('%%WORK_DIR%%', tmpdir)

        return query

    def generate_benchmark_data(self, benchmark_spec: Any, connection_url: str, tmpdir: str) -> None:
        print(f"*** Generating data for benchmark '{benchmark_spec['name']}'")
        logger.debug('Temp dir is %s', tmpdir)

        if (('generate-data' not in benchmark_spec)
                or (benchmark_spec['generate-data'] is None)):
            logger.debug("No data generation is specified")
            return

        if (('steps' not in benchmark_spec['generate-data'])
                or (benchmark_spec['generate-data']['steps'] is None)):
            logger.debug("No data generation steps are specified")
            return

        steps = benchmark_spec['generate-data']['steps']
        self.execute_steps_with_pgbench(connection_url, tmpdir, steps, 1)

    def execute_benchmark(self, benchmark_spec: Any, connection_url: str, tmpdir: str) -> None:
        print(f"*** Executing benchmark '{benchmark_spec['name']}'")

        # Prepare experiment
        if (('prepare-benchmark' not in benchmark_spec)
                or ('steps' not in benchmark_spec['prepare-benchmark'])):
            logger.debug("No preparation steps are specified")
        else:
            steps = benchmark_spec['prepare-benchmark']['steps']
            BenchmarkExecutor.execute_steps_directly(connection_url, tmpdir, steps)

        if 'benchmark' not in benchmark_spec or 'steps' not in benchmark_spec['benchmark']:
            logger.warning("No benchmark steps are found in benchmark %s",
                        {benchmark_spec['name']})
            return

        executions = 5
        if (('executions' in benchmark_spec['benchmark'])
                and (benchmark_spec['benchmark']['executions'] is not None)):
            executions = benchmark_spec['benchmark']['executions']

        benchmark_results = self.execute_steps_with_pgbench(
            connection_url, tmpdir, benchmark_spec['benchmark']['steps'], executions)

        cur_time = time.time()
        for index, value in enumerate(benchmark_results):
            self.result_reporter.add_result(benchmark_spec['name'], cur_time, index, value)

    @staticmethod
    def execute_steps_directly(connection_url: str, tmpdir: str, steps: list[str]) -> None:
        try:
            connection = BenchmarkExecutor.open_database_connection(connection_url)
            cur = connection.cursor()

            for step in steps:
                logger.info('Performing step \'%s\'', step['name'])

                query_with_placeholder = step['run']
                query = BenchmarkExecutor.replace_placeholder_in_query(
                    query_with_placeholder, tmpdir)
                logger.debug('Query: %s', query)

                cur.execute(query)

            connection.commit()
            cur.close()
            connection.close()
        except psycopg2.OperationalError as error:
            print(f'Unable to connect to the database {connection_url}')
            print(f'{error}')
            sys.exit(1)



    def execute_steps_with_pgbench(self, connection_url: str, tmpdir: str, benchmark_steps: dict[Any, Any],
                executions: int) -> str:

        # Write file for pgbench
        with tempfile.NamedTemporaryFile(dir=tmpdir, delete=False,
                                        mode="w", encoding="utf-8") as pgbench_file:
            steps = benchmark_steps
            for step in steps:
                query = BenchmarkExecutor.replace_placeholder_in_query(step['run'], tmpdir)
                pgbench_file.write(query)
            pgbench_file.close()

            # Execute pgbench
            result_file = self.execute_pgbench_file(
                connection_url, executions, pgbench_file.name)

        pg_result = parse_pgbench_result(result_file)

        return pg_result


    def execute_pgbench_file(self, connection_url: str, executions: int,
                            pgbench_file: str) -> str:

        logger.debug("Executing file %s on connection %s with pgbench %s (%i times)",
                    pgbench_file, connection_url, self.pg_bench, executions)

        connection_url_parsed = urlparse(connection_url)
        username = connection_url_parsed.username
        password = connection_url_parsed.password
        database = connection_url_parsed.path[1:]
        hostname = connection_url_parsed.hostname
        port = connection_url_parsed.port

        pgbench_command = []
        pgbench_command.append(self.pg_bench)

        # -r activates the per query latency
        pgbench_command.append('-r')

        # Don't execute a vacuum on the non existing default tables
        pgbench_command.append('--no-vacuum')

        # Executions
        pgbench_command.append('-t')
        pgbench_command.append(str(executions))

        if username:
            pgbench_command.append('-U')
            pgbench_command.append(username)

        if hostname:
            pgbench_command.append('-h')
            pgbench_command.append(hostname)

        if port:
            pgbench_command.append('-p')
            pgbench_command.append(str(port))

        pgbench_command.append('-f')
        pgbench_command.append(pgbench_file)
        pgbench_command.append(database)

        logger.debug("PGBench command is %s", pgbench_command)

        # Execute pgbench directly or pass the password to stdin of command
        result = None
        with subprocess.Popen(
            pgbench_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE) as process:
            input_data = ''
            if password:
                input_data = password
            std_out, std_err = process.communicate(input=input_data.encode())
            process.wait()

            if process.returncode != 0:
                logger.error("Pgbench returned not zero return code %s",
                            process.returncode)
                logger.error("Stdout %s", std_out.strip())
                logger.error("Stderr %s", std_err.strip())
                sys.exit(1)

            result = std_out.strip().decode("utf-8")

        print("Result: " + result)
        return result

    @staticmethod
    def locate_pg_bench() -> str:
        pg_bench = distutils.spawn.find_executable("pgbench")
        logger.debug("Using pgbench binary: %s", pg_bench)
        return pg_bench


    def execute_benchmarks_on_connections(self,
                                        connections: list[str]) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:

            # Generate benchmark data (the first connection is used
            # to generate the benchmark data)
            for benchmark_spec in self.benchmark_specs:
                self.generate_benchmark_data(benchmark_spec, connections[0], tmpdir)

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

                for benchmark_spec in self.benchmark_specs:
                    self.execute_benchmark(benchmark_spec, connection, tmpdir)

        print("*** All benchmarks are executed - done")
        self.result_reporter.finish()

    def build_and_execute_commit(self, commit: str, pg_source: str, pg_path: str, tmpdir: str) -> None:

        user = os.getlogin()
        database = "benchmarkdb"

        logger.debug("Build commit %s", commit)
        git_command = distutils.spawn.find_executable("git")

        # Configure commit
        print("*** Configure commit %s", commit)
        repo_dir = os.path.join(tmpdir, 'timescaledb')
        os.chdir(repo_dir)
        checkout_command = [git_command, 'checkout', commit]
        subprocess.run(checkout_command, check=True)

        # Build commit
        configure_command = ['./bootstrap', ' -DCMAKE_BUILD_TYPE=Debug',
                            '-DPG_SOURCE_DIR=' + pg_source, '-DPG_PATH=' + pg_path,
                            '-DREQUIRE_ALL_TESTS=ON', '-DLINTER_STRICT=ON', '-DASSERTIONS=ON',
                            '-DCMAKE_EXPORT_COMPILE_COMMANDS=YES', '-DSEND_TELEMETRY_DEFAULT=NO']

        # Ensure old data is overwritten
        print("*** Building commit %s", commit)
        my_env = os.environ.copy()
        my_env["BUILD_FORCE_REMOVE"] = "true"
        subprocess.run(configure_command, check=True, env=my_env)
        os.chdir("build")
        subprocess.run("make", check=True)

        with tempfile.TemporaryDirectory() as pg_data:

            # Start PostgreSQL for commit
            print("*** Starting Postgres for commit %s", commit)
            initdb_path = os.path.join(pg_path, 'bin', 'initdb')
            init_command = [initdb_path, '-D', pg_data]
            subprocess.run(init_command, check=True)

            createdb_path = os.path.join(pg_path, 'bin', 'createdb')
            createdb_command = [createdb_path, database]
            subprocess.run(createdb_command, check=True)

            pgctl_path = os.path.join(pg_path, 'bin', 'pg_ctl')
            start_command = [pgctl_path, '-D', pg_data,
                            '-l', os.path.join(pg_data, "log"), 'start']
            subprocess.run(start_command, check=True)

            # Process benchmarks for commit
            print("*** Executing benchmarks for commit %s", commit)
            connection = "pgsql://" + user + "@localhost/" + database
            for benchmark_file in self.benchmark_specs:
                self.execute_benchmark(benchmark_file, connection, tmpdir)

            # Stop Postgres for commit
            print("*** Stopping postgres for commit %s", commit)
            stop_command = [pgctl_path, '-D', pg_data,
                            '-l', os.path.join(pg_data, "log"), 'stop']
            subprocess.run(stop_command, check=True)


    def execute_benchmarks_by_commits(self, commit1: str, commit2: str, pg_source: str,
            pg_path: str, repository: str) -> None:

        logger.debug("Performing benchmark of commit %s and %s of repository %s with PG %s %s",
                    commit1, commit2, repository, pg_source, pg_path)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Checkout repository
            print("*** Cloning repository %s", repository)
            os.chdir(tmpdir)
            git_command = distutils.spawn.find_executable("git")
            clone_command = [git_command, 'clone', repository, 'timescaledb']
            logger.debug("Executing clone command %s", clone_command)
            subprocess.run(clone_command, check=True)

            self.build_and_execute_commit(commit1, pg_source, pg_path, tmpdir)
            self.build_and_execute_commit(commit2, pg_source, pg_path, tmpdir)

        print("*** All benchmarks are executed - done")
        self.result_reporter.finish()
