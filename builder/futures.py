import concurrent.futures
import sys
import traceback

import logging
LOG = logging.getLogger("builder.exceptions")

def print_exception(exception):
    for line in exception:
        line = line.rstrip()
        for sub_line in line.split('\n'):
            LOG.warning("{}".format(sub_line).rstrip())

class ThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    def submit(self, fn, *args, **kwargs):
        submit_traceback = traceback.format_stack()
        return super(ThreadPoolExecutor, self).submit(
                self._fn_wrapper, fn, submit_traceback, *args, **kwargs)

    def _fn_wrapper(self, fn, submit_traceback, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            LOG.warning("SUBMIT TRACEBACK")
            print_exception(submit_traceback)
            LOG.warning("TRACEBACK")
            print_exception(sys.exc_info()[0](traceback.format_exc()))
            raise e
