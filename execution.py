import threading
import time
import subprocess
import deepy.log
import Queue
import arrow
import collections

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

    def execute(self, job):
        if job.is_running:
            raise SystemError("Job {} is already running".format(job))
        job = self.prepare_job_for_execution(job)

        status = False
        try:
            status, log = self.do_execute(job)
        except Exception as e:
            log = unicode(e)
        finally:
            self.finish_job(job, status, log)

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

    def do_execute(self, job):
        command = job.get_command()
        proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = proc.communicate()
        deepy.log.info("{} STDOUT: {}".format(command, stdout))
        deepy.log.info("{} STDERR: {}".format(command, stderr))
        return proc.returncode, '===='.join((stdout, stderr))

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

    def submit(self, job_definition_id, build_context, **kwargs):
        """
        Submit the provided job to be built
        """
        def update_build_graph():
            self.build.add_job(job_definition_id, build_context, **kwargs)
        self._update_build(update_build_graph)

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
                job.invalidate()
                should_run = job.get_should_run()
                if should_run:
                    next_jobs_list.append(dependent_job)

        update_set.add(job_id)

        return next_jobs_list

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
            success, log = self._execute(job, self.build)

            # Update job state
            if self.executor.should_update_build_graph:
                self._update_build(lambda: self.finish_job(job, success=success, log=log))

            # Get next jobs to execute
            next_job_ids = self.get_next_jobs_to_run(job.get_id())
            next_jobs = map(lambda x: self.build.get_job(x), next_job_ids)
            map(work_queue.put, next_jobs)

    def _execute_daemon(self):
        raise NotImplementedError()

    def _execute(self, job, build_graph):
        if callable(self.executor):
            return self.executor(job, build_graph)
        else:
            return self.executor.execute(job, build_graph)

    def get_jobs_to_run(self):
        def get_next_jobs():
            return self.build.get_starting_jobs()
        return self._update_build(get_next_jobs)

    def finish_job(self, job, success, log, update_job_cache=True):
        job.last_run = arrow.now()
        job.retries += 1
        if success:
            job.should_run = False
            job.force = False
            if update_job_cache:
                self.update_job_cache(job.get_id())

    def update_job_cache(self, job_id):
        """Updates the cache due to a job finishing"""
        target_ids = self.build.get_target_ids(job_id)
        self.update_targets(target_ids)

        job = self.build.get_job(job_id)
        job.invalidate()
        job.get_stale()

        for target_id in target_ids:
            dependent_ids = self.build.get_dependent_ids(target_id)
            for dependent_id in dependent_ids:
                dependent = self.build.get_job(dependent_id)
                dependent.invalidate()
                dependent.get_buildable()
                dependent.get_stale()

        job.update_lower_nodes_should_run()

    def update_targets(self, target_ids):
        """Takes in a list of target ids and updates all of their needed
        values
        """
        update_function_list = collections.defaultdict(list)
        for target_id in target_ids:
            target = self.build.get_target(target_id)
            func = target.get_bulk_exists_mtime
            update_function_list[func].append(target)

        for update_function, targets in update_function_list.iteritems():
            update_function(targets)

    def update_target_cache(self, target_id):
        """Updates the cache due to a target finishing"""
        target = self.build.get_target(target_id)
        target.invalidate()
        target.get_mtime()

        dependent_ids = self.build.get_dependent_ids(target_id)
        for dependent_id in dependent_ids:
            dependent = self.build.get_job(dependent_id)
            dependent.invalidate()
            dependent.get_stale()
            dependent.get_buildable()
            dependent.update_lower_nodes_should_run()

    def update(self, target_id):
        """Checks what should happen now that there is new information
        on a target
        """
        self.update_target_cache(target_id)
        creator_ids = self.build.get_creator_ids(target_id)
        creators_exist = False
        for creator_id in creator_ids:
            creators_exist = True
            next_jobs = self.get_next_jobs_to_run(creator_id)
            for next_job in next_jobs:
                self.run(next_job)
        if creators_exist == False:
            for dependent_id in self.build.get_dependent_ids(target_id):
                next_jobs = self.get_next_jobs_to_run(dependent_id)
                for next_job in next_jobs:
                    self.run(next_job)


    def get_build(self):
        return self.build

    def get_build_manager(self):
        return self.build_manager

    def _update_build(self, f):
        with self._build_lock:
            return f()
