from abc import ABC

import logging

logger = logging.getLogger(__name__)

class AbstractResultReporter(ABC):
    def add_result(self, experiment : str, run: int, query: int, result: float):
        pass

    def finish(self):
        pass

class ConsoleResultReporter(AbstractResultReporter):

    def __init__(self):
        self.results = {}

    def add_result(self, experiment : str, run: int, query: int, result: float):
        if experiment not in self.results:
            self.results[experiment] = {}

        self.results[experiment][run] = {}
        self.results[experiment][run][query] = result

        logger.debug("Got result %s %d %d", experiment, query, result)

    def finish(self):
        print(self.results)
