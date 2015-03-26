
import builder.build
import threading
import time
import subprocess

class ExecutionThread(threading.Thread):

    def __init__(self, execution_manager):
        self.execution_manager = execution_manager
        super(ExecutionThread, self).__init__()

    def run(self):
        time.sleep(1)
        print "Sleeping"

class Executor(object):

    def execute(self, job):
        raise NotImplementedError()

class LocalExecutor(Executor):

    def execute(self, job):
        command = job.get_command()
        return subprocess.check_call(command, shell=True)

class ExecutionManager(object):

    def __init__(self, build_manager, executor):
        self.build_manager = build_manager
        self.build = build_manager.make_build()
        self.executor = executor
        self._build_lock = threading.RLock()

    def submit(self, job, build_context, direction=None, depth=None, force=False):
        """
        Submit the provided job to be built
        """
        def update_build_graph():
            self.build.add_job(job, build_context, direction=direction, depth=depth, force=force)
        self._update_build(update_build_graph)

    def start_execution(self):
        """
        Begin executing jobs
        """
        pass

    def get_next_jobs(self):
        def get_next_jobs():
            return self.build.get_starting_jobs()
        return self._update_build(get_next_jobs)

    def get_build_graph(self):
        return self.build

    def _update_build(self, f):
        with self._build_lock:
            return f()