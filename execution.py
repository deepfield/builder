import threading
import time
import signal
import subprocess
import deepy.log
import Queue
import arrow
import collections
import shlex
import concurrent.futures
import json

from tornado import gen
from tornado import ioloop
from tornado.web import asynchronous, RequestHandler, Application

import deepy.log as LOG

class ExecutionResult(object):
    def __init__(self, is_async, status=None, stdout=None, stderr=None):
        self._is_async = is_async
        self.status = status
        self.stdout = stdout
        self.stderr = stderr

    def finish(self, status, stdout, stderr):
        self.status = status
        self.stdout = stdout
        self.stderr = stderr

    def is_finished(self):
        return self.status is not None

    def is_async(self):
        return self._is_async


class Executor(object):

    # Should be False if this executor will handle updating the job state
    should_update_build_graph = True

    def __init__(self, execution_manager):
        self._build_graph = execution_manager.get_build()
        self._execution_manager = execution_manager

    def execute(self, job):
        """Execute the specified job.
        Returns None if the job does not execute because it is already running or because its get_should_run method returns False.
        Otherwise, returns an appropriate ExecutionResult object.
        """
        if job.is_running:
            return None
        if not job.get_should_run():
            return None

        job = self.prepare_job_for_execution(job)

        result = None
        try:
            result = self.do_execute(job)
        finally:
            if result is not None and not isinstance(result, concurrent.futures.Future):
                LOG.debug("Finishing job {}".format(job.get_id()))
                self.get_execution_manager()._update_build(lambda: self.finish_job(job, result, self.should_update_build_graph))

        return result

    def do_execute(self, job):
        raise NotImplementedError()

    def get_build_graph(self):
        return self._build_graph


    def get_execution_manager(self):
        return self._execution_manager


    def prepare_job_for_execution(self, job):
        job.is_running = True
        return job


    def finish_job(self, job, result, update_job_cache=True):
        LOG.info("Job {} complete. Status: {}".format(job.get_id(), result.status))
        LOG.debug("{}(stdout): {}".format(job.get_id(), result.stdout))
        LOG.debug("{}(stderr): {}".format(job.get_id(), result.stderr))

        # Mark this job as finished running
        job.last_run = arrow.now()
        job.retries += 1
        job.is_running = False
        if update_job_cache:
            job.invalidate()

            # updat all of it's targets
            target_ids = self.get_build_graph().get_target_ids(job.get_id())
            self.update_targets(target_ids)
            # update all of it's dependents
            for target_id in target_ids:
                dependent_ids = self.get_build_graph().get_dependent_ids(target_id)
                for dependent_id in dependent_ids:
                    dependent = self.get_build_graph().get_job(dependent_id)
                    dependent.invalidate()

            # check if it succeeded and set retries to 0
            if not job.get_should_run_immediate():
                job.force = False
                job.retries = 0
        self.get_execution_manager().add_to_complete_queue(job.get_id())

    def update_targets(self, target_ids):
        """Takes in a list of target ids and updates all of their needed
        values
        """
        update_function_list = collections.defaultdict(list)
        for target_id in target_ids:
            target = self.get_build_graph().get_target(target_id)
            func = target.get_bulk_exists_mtime
            update_function_list[func].append(target)

        for update_function, targets in update_function_list.iteritems():
            update_function(targets)


class LocalExecutor(Executor):

    def do_execute(self, job):
        command = job.get_command()
        command_list = shlex.split(command)
        LOG.info("Executing '{}'".format(command))
        proc = subprocess.Popen(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = proc.communicate()
        LOG.info("{} STDOUT: {}".format(command, stdout))
        LOG.info("{} STDERR: {}".format(command, stderr))

        return ExecutionResult(is_async=False, status=proc.returncode == 0, stdout=stdout, stderr=stderr)


class PrintExecutor(Executor):
    """ "Executes" by printing and marking targets as available
    """

    should_update_build_graph = False

    def do_execute(self, job):
        build_graph = self.get_build_graph()
        command = job.get_command()
        job.set_should_run(False)

        print command
        target_ids = build_graph.get_target_ids(job.get_id())
        for target_id in target_ids:
            target = build_graph.get_target(target_id)
            target.exists = True
            target.mtime = arrow.get()
            for dependent_job_id in build_graph.get_dependent_ids(target_id):
                dependent_job = build_graph.get_job(dependent_job_id)
                dependent_job.invalidate()
                dependent_job.set_should_run(True)

        return ExecutionResult(is_async=False, status=True, stdout='', stderr='')


class ExecutionManager(object):

    def __init__(self, build_manager, executor_factory, max_retries=5):
        self.build_manager = build_manager
        self.build = build_manager.make_build()
        self.max_retries = max_retries
        self._build_lock = threading.RLock()
        self._work_queue = Queue.Queue()
        self._complete_queue = Queue.Queue()
        self.executor = executor_factory(self)
        self.running = False


    def _recursive_invalidate_job(self, job_id):
        job = self.build.get_job(job_id)
        job.invalidate()

        target_ids = self.build.get_target_ids(job_id)
        for target_id in target_ids:
            self._recursive_invalidate_target(target_id)

    def _recursive_invalidate_target(self, target_id):
        target = self.build.get_target(target_id)
        target.invalidate()
        job_ids = self.build.get_dependent_ids(target_id)
        for job_id in job_ids:
            self._recursive_invalidate_job(job_id)

    def submit(self, job_definition_id, build_context, **kwargs):
        """
        Submit the provided job to be built
        """
        def update_build_graph():
            # Add the job
            new_jobs, created_nodes = self.build.add_job(job_definition_id, build_context, **kwargs)

            # Invalidate the build graph for all child nodes
            for job_id in new_jobs:
                self._recursive_invalidate_job(job_id)

            # Refresh all uncached existences
            self.build.bulk_refresh_targets()

        self._update_build(update_build_graph)

    def add_to_work_queue(self, job_id):

        self._work_queue.put(job_id)
        LOG.debug("Adding {} to ExecutionManager's work queue. There are now approximately {} jobs in the queue.".format(job_id, self._work_queue.qsize()))


    def add_to_complete_queue(self, job_id):
        LOG.debug("Adding {} to ExecutionManager's complete queue".format(job_id))
        self._complete_queue.put(job_id)

    def start_execution(self, inline=True):
        """
        Begin executing jobs
        """
        LOG.info("Starting execution")
        self.running = True

        # Seed initial jobs
        work_queue = self._work_queue
        next_jobs = self.get_jobs_to_run()
        map(work_queue.put, next_jobs)

        # Start completed jobs consumer if not inline
        executor = None
        if not inline:
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            executor.submit(self._consume_completed_jobs, block=True)

        jobs_executed = 0
        ONEYEAR = 365 * 24 * 60 * 60
        while (not work_queue.empty() or not inline) and self.running:
            LOG.debug("EXECUTION_LOOP => Getting job from the work queue")

            try:
                job_id = work_queue.get(True, timeout=1)
            except Queue.Empty:
                continue

            LOG.debug("EXECUTION_LOOP => Got job {} from work queue".format(job_id))
            result = self.execute(job_id)
            #LOG.debug("EXECUTION_LOOP => Finished job {} from work queue".format(job_id))
            jobs_executed += 1
            if not isinstance(result, concurrent.futures.Future) and inline:
                self._consume_completed_jobs(block=False)
            elif inline:
                LOG.debug("EXECUTION_LOOP => Waiting on execution to complete")
                result.result() # Wait for job to complete
                self._consume_completed_jobs(block=False)
            LOG.debug("EXECUTION_LOOP => Finished consuming completed jobs for {}".format(job_id))
            #else: It is an asynchronous result and we're running asynchronously, so let the _consume_completed_jobs
            # thread add new jobs
            LOG.debug("EXECUTION_LOOP => Executed {} jobs".format(jobs_executed))

        LOG.debug("EXECUTION_LOOP => Execution is exiting")
        if executor is not None:
            executor.shutdown(wait=True)

    def stop_execution(self):
        LOG.info("Stopping execution")
        self.running = False


    def _consume_completed_jobs(self, block=False):

        LOG.debug("EXECUTION_LOOP => Consuming completed jobs")
        complete_queue = self._complete_queue
        while (not complete_queue.empty() or block) and self.running:
            try:
                job_id = complete_queue.get(True, timeout=1)
            except Queue.Empty:
                continue

            LOG.debug("COMPLETION_LOOP =>  Completed job {}".format(job_id))
            next_jobs = self.get_next_jobs_to_run(job_id)
            next_jobs = filter(lambda job_id: not self.build.get_job(job_id).is_running, next_jobs)
            LOG.debug("COMPLETION_LOOP => Received completed job {}. Next jobs are {}".format(job_id, next_jobs))
            map(self.add_to_work_queue, next_jobs)
        LOG.debug("COMPLETION_LOOP => Done consuming completed jobs")


    def get_next_jobs_to_run(self, job_id, update_set=None):
        """Returns the jobs that are below job_id that need to run"""
        if update_set is None:
            update_set = set([])

        if job_id in update_set:
            return []

        next_jobs_list = []

        job = self.build.get_job(job_id)
        if job.get_should_run():
            next_jobs_list.append(job_id)
            update_set.add(job_id)
            return next_jobs_list


        target_ids = self.build.get_target_ids(job_id)
        for target_id in target_ids:
            dependent_jobs = self.build.get_dependent_ids(target_id)
            for dependent_job in dependent_jobs:
                job = self.build.get_job(dependent_job)
                should_run = job.get_should_run()
                if should_run:
                    next_jobs_list.append(dependent_job)

        update_set.add(job_id)

        return next_jobs_list


    def execute(self, job_id):
        # Don't run a job more than the configured max number of retries
        LOG.debug("ExecutionManager.execute({})".format(job_id))
        job = self.build.get_job(job_id)
        if job.retries >= self.max_retries:
            job.set_failed(True)
            LOG.error("Maximum number of retries reached for {}".format(job))
            return

        # Execute job
        result = self._execute(job)

        return result

    def _execute(self, job):
        if callable(self.executor):
            return self.executor(job)
        else:
            return self.executor.execute(job)


    def get_jobs_to_run(self):
        """Used to return a list of jobs to run"""
        should_run_list = []
        for job_id, job in self.build.job_iter():
            if job.get_should_run():
                should_run_list.append(job_id)
        return should_run_list


    def get_build(self):
        return self.build

    def get_build_manager(self):
        return self.build_manager

    def _update_build(self, f):
        with self._build_lock:
            return f()


def _submit_from_json(execution_manager, json_body):
    payload = json.loads(json_body)
    LOG.debug("Submitting job {}".format(payload))

    # Clean up the payload a bit
    build_context = payload.get('build_context', {})
    for k in ('start_time', 'end_time'):
        if k in build_context:
            build_context[k] = arrow.get(build_context[k])

    execution_manager.submit(**payload)
    jobs_to_run = execution_manager.get_jobs_to_run()

    map(execution_manager.add_to_work_queue, jobs_to_run)
    LOG.debug("After submitting job, the following jobs should run: {}".format(jobs_to_run))

class SubmitHandler(RequestHandler):
    def initialize(self, execution_manager):
        self.execution_manager = execution_manager

    def post(self):
        _submit_from_json(self.execution_manager, self.request.body)

class ExecutionDaemon(object):

    def __init__(self, execution_manager, port=7001):
        self.execution_manager = execution_manager
        self.application = Application([
            (r"/submit", SubmitHandler, {"execution_manager" : self.execution_manager}),
        ])
        self.port = port
        self.is_closing = False

    def signal_handler(self, signum, frame):
            LOG.info('exiting...')
            self.is_closing = True

    def try_exit(self):
        if self.is_closing:
            # clean up here
            ioloop.IOLoop.instance().stop()
            LOG.info('exit success')

    def start(self):
        is_closing = False

        signal.signal(signal.SIGINT, self.signal_handler)
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        executor.submit(self.execution_manager.start_execution, inline=False)
        self.application.listen(self.port)
        LOG.info("Starting job listener")
        ioloop.PeriodicCallback(self.try_exit, 500).start()
        ioloop.IOLoop.instance().start()
        LOG.info("Shutting down")
        self.execution_manager.stop_execution()
        executor.shutdown()