
import builder.build
import threading
import time
import subprocess
import deepy.log
import Queue

class ExecutionThread(threading.Thread):

    def __init__(self, execution_manager):
        self.execution_manager = execution_manager
        super(ExecutionThread, self).__init__()

    def run(self):
        time.sleep(1)
        print "Sleeping"

class Executor(object):

    def execute(self, job):
        if job.is_running:
            raise SystemError("Job {} is already running".format(job))
        job = self.prepare_job_for_execution(job)
        try:
            status, log = self.do_execute(job)
        finally:
            self.finish_job(job, status, log)

    def do_execute(self, job):
        raise NotImplementedError()

    def prepare_job_for_execution(self, job):
        job.is_running = True
        return job

    def finish_job(self, job, status, log):
        job.is_running = False
        deepy.log.info("Job {} complete. Status: {}".format(job.unexpaded_id, status))
        deepy.log.debug(log)

class LocalExecutor(Executor):

    def do_execute(self, job, build_graph):
        command = job.get_command(build_graph)
        return subprocess.check_call(command, shell=True), 'Log'

class PrintExecutor(Executor):

    def do_execute(self, job, build_graph):
        command = job.get_command(build_graph)
        print command
        return True, 'Log'

class ExecutionManager(object):

    def __init__(self, build_manager, executor):
        self.build_manager = build_manager
        self.build = build_manager.make_build()
        self.executor = executor
        self._build_lock = threading.RLock()

    def submit(self, job, build_context, **kwargs):
        """
        Submit the provided job to be built
        """
        def update_build_graph():
            self.build.add_job(job, build_context, **kwargs)
        self._update_build(update_build_graph)

    def start_execution(self, inline=True):
        """
        Begin executing jobs
        """
        work_queue = Queue.Queue()
        if inline:
            next_jobs = self.get_jobs_to_run()
            map(work_queue.put, next_jobs)
            while not work_queue.empty():
                job = work_queue.get()
                success, log = self.executor.execute(job, self.build)
                self._update_build(lambda: self.build.finish_job(job, success=success, log=log))
                next_job_ids = self.build.get_next_jobs_to_run(job.get_id())
                next_jobs = map(lambda x: self.build.get_job(x), next_job_ids)
                map(work_queue.put, next_jobs)
        else:
            # Daemon
            raise NotImplementedError()

    def get_jobs_to_run(self):
        def get_next_jobs():
            return self.build.get_starting_jobs()
        return self._update_build(get_next_jobs)

    def get_build_graph(self):
        return self.build

    def _update_build(self, f):
        with self._build_lock:
            return f()