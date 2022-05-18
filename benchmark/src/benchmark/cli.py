import sys
import logging

from typing import Any
from argparse import ArgumentParser
from .benchmark_executor import execute_benchmarks_on_connections
from .benchmark_executor import execute_benchmarks_by_commits

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def set_log_level(args: str) -> None:
    if not args.verbose:
        logging.root.setLevel(logging.WARN)
    elif args.verbose == 1:
        logging.root.setLevel(logging.INFO)
    else:
        logging.root.setLevel(logging.DEBUG)


def run() -> None:
    parser = ArgumentParser(
        description="Benchmark a TimescaleDB installation",
        prog="benchmark"
    )

    # Benchmark type
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--with-connection",
        dest="with_connection",
        action="append",
        default=[],
        type=str,
        help="Perform the benchmark by using connections URLs. Use the " +
        "placeholder reuse-and-pause to reuse a connection.",
    )

    group.add_argument(
        "--with-commit-ids",
        dest="with_commit_ids",
        default=None,
        help="Perform the benchmark by using two commit ids commitid1..commitid2"
    )

    parser.add_argument('-v', '--verbose', action="count",
                        help="Increase verbosity.")
    parser.add_argument(
        "--benchmark",
        dest="benchmarks",
        action="append",
        default=[],
        type=str,
        help="The benchmarks that should be included in this execution (default: *)",
    )

    parser.add_argument(
        "--pg",
        dest="pg",
        default=None,
        help="The PostgreSQL installation to use"
    )

    parser.add_argument(
        "--repository",
        dest="repository",
        default="https://github.com/timescale/timescaledb.git",
        help="The Git repository to use"
    )

    args = parser.parse_args()
    set_log_level(args)

    if args.with_connection:
        execute_benchmarks_with_connections(args)
    elif args.with_commit_ids:
        execute_benchmarks_with_commits(args)
    else:
        raise ValueError('Unknown benchmark type')


def execute_benchmarks_with_connections(args: Any) -> None:
    if len(args.with_connection) < 2:
        print("You must at least specify two connections", file=sys.stderr)
        sys.exit(1)
    execute_benchmarks_on_connections(args.benchmarks, args.with_connection)


def execute_benchmarks_with_commits(args: Any) -> None:
    commits = args.with_commit_ids.split('..')
    if args.with_commit_ids.count('.') != 2 or len(commits) != 2:
        print("Please provide the commits in the format commit1..commit2",
              file=sys.stderr)
        sys.exit(1)
    execute_benchmarks_by_commits(
        args.benchmarks, commits[0], commits[1], args.pg, args.repository)
