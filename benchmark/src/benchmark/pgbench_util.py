import re
import logging

logger = logging.getLogger(__name__)


def parse_pgbench_result(pg_result: str) -> list[float]:

    if pg_result is None:
        raise Exception("parse data is none")

    lines = pg_result.split("\n")
    query_results_found = False
    result_times = []

    for line in lines:
        if "statement latencies in milliseconds" in line:
            query_results_found = True
            continue

        if query_results_found is False:
            continue

        found_numbers = re.findall(r"[-+]?(?:\d*\.\d+|\d+)", line)

        if len(found_numbers) < 1:
            raise Exception("Unable to find time in line: " + line)

        execution_time = float(found_numbers[0])
        result_times.append(execution_time)

    if query_results_found is False:
        raise Exception("No query results found")

    return result_times
