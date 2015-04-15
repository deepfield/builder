import threading
import time
import subprocess
import deepy.log
import Queue
import arrow
import collections

from celery import Celery

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

    def __init__(self, execution_manager):
        self._build_graph = execution_manager.get_build()
        self._execution_manager = execution_manager

    def execute(self, job):
        if job.is_running:
            raise SystemError("Job {} is already running".format(job))
        job = self.prepare_job_for_execution(job)

        result = None
        try:
            result = self.do_execute(job)
        finally:
            if result is not None and not result.is_async():
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
        deepy.log.info("Job {} complete. Status: {}".format(job.get_id(), result.status))
        deepy.log.debug("{}(stdout): {}".format(job.get_id(), result.stdout))
        deepy.log.debug("{}(stderr): {}".format(job.get_id(), result.stderr))

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
        next_jobs = self.get_execution_manager().get_next_jobs_to_run(job.get_id())
        map(self.get_execution_manager().add_to_work_queue, next_jobs)

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

    def update_target_cache(self, target_id):
        """Updates the cache due to a target finishing"""
        target = self.get_build_graph().get_target(target_id)
        target.invalidate()
        target.get_mtime()

        dependent_ids = self.get_build_graph().get_dependent_ids(target_id)
        for dependent_id in dependent_ids:
            dependent = self.get_build_graph().get_job(dependent_id)
            dependent.invalidate()
            dependent.get_stale()
            dependent.get_buildable()
            dependent.update_lower_nodes_should_run()

    def update(self, target_id):
        """Checks what should happen now that there is new information
        on a target
        """
        self.update_target_cache(target_id)
        creator_ids = self.get_build_graph().get_creator_ids(target_id)
        creators_exist = False
        for creator_id in creator_ids:
            creators_exist = True
            next_jobs = self.get_next_jobs_to_run(creator_id)
            for next_job in next_jobs:
                self.run(next_job)
        if creators_exist == False:
            for dependent_id in self.get_build_graph().get_dependent_ids(target_id):
                next_jobs = self.get_next_jobs_to_run(dependent_id)
                for next_job in next_jobs:
                    self.run(next_job)



class LocalExecutor(Executor):

    def do_execute(self, job):
        command = job.get_command()
        proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = proc.communicate()
        deepy.log.info("{} STDOUT: {}".format(command, stdout))
        deepy.log.info("{} STDERR: {}".format(command, stderr))

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


    # def finish_job(self, job, result):
    #     super(PrintExecutor, self).finish_job(job, result)
    #     self.finish_job(job, result)




class ExecutionManager(object):

    def __init__(self, build_manager, executor_factory, max_retries=5):
        self.build_manager = build_manager
        self.build = build_manager.make_build()
        self.max_retries = max_retries
        self._build_lock = threading.RLock()
        self._work_queue = Queue.Queue()
        self.executor = executor_factory(self)


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

    def start_execution(self, inline=True):
        """
        Begin executing jobs
        """

        if inline:
            self._execute_inline()
        else:
            self._execute_daemon()

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

    def _execute_inline(self):
        work_queue = self._work_queue
        next_jobs = self.get_jobs_to_run()
        map(work_queue.put, next_jobs)
        while not work_queue.empty():
            job_id = work_queue.get()
            self.execute(job_id)

    def execute(self, job_id):
        # Don't run a job more than the configured max number of retries
        job = self.build.get_job(job_id)
        if job.retries >= self.max_retries:
            job.set_failed(True)
            deepy.log.error("Maximum number of retries reached for {}".format(job))
            return

        # Execute job
        result = self._execute(job)

        return result

    def _execute_daemon(self):
        raise NotImplementedError()

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
