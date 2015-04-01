
import builder.build
import threading
import time
import subprocess
import deepy.log
import Queue
import arrow

class ExecutionThread(threading.Thread):

    def __init__(self, execution_manager):
        self.execution_manager = execution_manager
        super(ExecutionThread, self).__init__()

    def run(self):
        time.sleep(1)
        print "Sleeping"

class Executor(object):

    # Should be False if this executor will handle updating the job state
    should_update_build_graph = True

    def execute(self, job, build_graph):
        if job.is_running:
            raise SystemError("Job {} is already running".format(job))
        job = self.prepare_job_for_execution(job)

        status = False
        try:
            status, log = self.do_execute(job, build_graph)
        except Exception as e:
            log = unicode(e)
        finally:
            self.finish_job(job, status, log, build_graph)

        return status, log

    def do_execute(self, job):
        raise NotImplementedError()

    def prepare_job_for_execution(self, job):
        job.is_running = True
        return job

    def finish_job(self, job, status, log, build_graph):
        job.is_running = False
        deepy.log.info("Job {} complete. Status: {}".format(job.get_id(), status))
        deepy.log.debug(log)

class LocalExecutor(Executor):

    def do_execute(self, job, build_graph):
        command = job.get_command(build_graph)
        return subprocess.check_call(command, shell=True), 'Log'

class PrintExecutor(Executor):
    """ "Executes" by printing and marking targets as available
    """

    should_update_build_graph = False

    def do_execute(self, job, build_graph):
        command = job.get_command(build_graph)
        print command
        target_ids = build_graph.get_targets(job.get_id())
        for target_id in target_ids:
            target = build_graph.get_target(target_id)
            target.exists = True
            target.mtime = arrow.get()

        return True, 'Log'

    def finish_job(self, job, status, log, build_graph):
        super(PrintExecutor, self).finish_job(job, status, log, build_graph)
        build_graph.finish_job(job, status, log, update_job_cache=False)

class ExecutionManager(object):

    def __init__(self, build_manager, executor, max_retries=5):
        self.build_manager = build_manager
        self.build = build_manager.make_build()
        self.executor = executor
        self.max_retries = max_retries
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

        if inline:
            self._execute_inline()
        else:
            self._execute_daemon()


    def _execute_inline(self):
        work_queue = Queue.Queue()
        next_jobs = self.get_jobs_to_run()
        map(work_queue.put, next_jobs)
        while not work_queue.empty():
            job = work_queue.get()

            # Don't run a job more than the configured max number of retries
            if job.retries >= self.max_retries:
                job.should_run = False
                job.force = False
                deepy.log.error("Maximum number of retries reached for {}".format(job))
                continue

            # Execute job
            success, log = self.executor.execute(job, self.build)

            # Update job state
            if self.executor.should_update_build_graph:
                self._update_build(lambda: self.build.finish_job(job, success=success, log=log))

            # Get next jobs to execute
            next_job_ids = self.build.get_next_jobs_to_run(job.get_id())
            next_jobs = map(lambda x: self.build.get_job_state(x), next_job_ids)
            map(work_queue.put, next_jobs)

    def _execute_daemon(self):
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