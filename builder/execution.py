import threading
import time
import logging
import signal
import subprocess
import Queue
import collections
import concurrent.futures
import shlex
import json
import tempfile
import os
import re

import builder.futures
from builder.util import arrow_factory as arrow
import builder.build as build

import networkx as nx
from tornado import gen
from tornado import ioloop
from tornado.web import asynchronous, RequestHandler, Application, StaticFileHandler
from tornado.template import Loader, Template

LOG = logging.getLogger(__name__)
PROCESSING_LOG = logging.getLogger("builder.execution.processing")
TRANSITION_LOG = logging.getLogger("builder.execution.transition")

def _interruptable_sleep(seconds):
    # Loop so it can be interrupted quickly (sleep does not pay attention to interrupt)
    for i in xrange(min(int(seconds), 1)):
        time.sleep(1)

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

    def __init__(self, execution_manager, config=None):
        self._build_graph = execution_manager.get_build()
        self._execution_manager = execution_manager
        self._initialized = False

    def initialize(self):
        """
        Put any expensive operations here that should only happen right before execution starts
        """
        pass

    def execute(self, job):
        """Execute the specified job.
        Returns None if the job does not execute because it is already running or because its get_should_run method returns False.
        Otherwise, returns an appropriate ExecutionResult object.
        """
        job = self.prepare_job_for_execution(job)

        result = None
        try:
            result = self.do_execute(job)
        finally:
            if result is not None and not isinstance(result, concurrent.futures.Future):
                if not result.is_async():
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
        return job

    def finish_job(self, job, result, update_job_cache=True):
        LOG.info("Job {} complete. Status: {}".format(job.get_id(), result.status))
        LOG.debug("{}(stdout): {}".format(job.get_id(), result.stdout))
        LOG.debug("{}(stderr): {}".format(job.get_id(), result.stderr))

        # Mark this job as finished running
        job.last_run = arrow.now()
        job.retries += 1
        job.is_running = False
        job.force = False
        if update_job_cache:
            target_ids = self.get_build_graph().get_target_ids(job.get_id())
            self._execution_manager.update_targets(target_ids)

            job_id = job.unique_id
            self.get_execution_manager().update_parents_should_run(job_id)

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
            else:
                if job.retries >= self.get_execution_manager().max_retries:
                    job.set_failed(True)
                    job.invalidate()
                    LOG.error("Maximum number of retries reached for {}".format(job))
        self.get_execution_manager().add_to_complete_queue(job.get_id())


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
        job.set_stale(False)

        print "Simulation:", command
        target_relationships = build_graph.get_target_relationships(job.get_id())
        produced_targets = {}
        for target_type, target_group in target_relationships.iteritems():
            if target_type == "alternates":
                continue
            produced_targets.update(target_group)
        for target_id in produced_targets:
            target = build_graph.get_target(target_id)
            target.exists = True
            target.mtime = arrow.get().timestamp
            print "Simulation: Built target {}".format(target.get_id())
            for dependent_job_id in build_graph.get_dependent_ids(target_id):
                dependent_job = build_graph.get_job(dependent_job_id)
                dependent_job.invalidate()
                # dependent_job.set_should_run(True)

        return ExecutionResult(is_async=False, status=True, stdout='', stderr='')


class ExecutionManager(object):

    def __init__(self, build_manager, executor_factory, max_retries=5, job_timeout=30*60, config=None):
        self.build_manager = build_manager
        self.build = build_manager.make_build()
        self.max_retries = max_retries
        self.config = config
        self._build_lock = threading.RLock()
        self._work_queue = Queue.Queue()
        self._complete_queue = Queue.Queue()
        self.executor = executor_factory(self, config=self.config)
        self.execution_times = {}
        self.submitted_jobs = 0
        self.completed_jobs = 0
        self.start_time = None
        self.last_job_executed_on = None
        self.last_job_completed_on = None
        self.last_job_submitted_on = None
        self.last_job_worked_on = None
        self.job_timeout = job_timeout

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

    def submit(self, job_definition_id, build_context, update_topmost=False, update_all=False, **kwargs):
        """
        Submit the provided job to be built
        """
        if not self.running:
            raise RuntimeError("Cannot submit to a execution manager that "
                               "isn't running")
        def update_build_graph():
            # Add the job
            LOG.debug("SUBMISSION => Expanding build graph for submission {} {}".format(job_definition_id, build_context))
            if self.build.rule_dependency_graph.is_job_definition(job_definition_id):
                build_update = self.build.add_job(job_definition_id, build_context, **kwargs)
            else:
                build_update = self.build.add_meta(job_definition_id, build_context, **kwargs)

            update_nodes = set()
            if update_topmost or update_all:
                for node_id in build_update.targets:
                    if self.build.in_degree(node_id) == 0 or update_all:
                        if self.build.is_target(node_id):
                            update_nodes.add(node_id)

                LOG.debug("UPDATE_SUBMITTED => {}".format(update_nodes))
                self.external_update_targets(update_nodes)


            LOG.debug("SUBMISSION => Build graph expansion complete")

            # Refresh all uncached existences
            LOG.debug("updating {} targets".format(len(build_update.new_targets)))
            self.update_targets(build_update.new_targets)

            # Invalidate the build graph for all child nodes
            newly_invalidated_job_ids = build_update.new_jobs | build_update.newly_forced
            LOG.debug("SUBMISSION => Newly invlidated job ids: {}".format(newly_invalidated_job_ids))
            LOG.debug("SUBMISSION => Jobs expanded: {}".format(build_update.jobs))
            for newly_invalidated_job_id in newly_invalidated_job_ids:
                self.update_parents_should_run(newly_invalidated_job_id)
                next_job_to_run_ids = self.get_next_jobs_to_run(
                        newly_invalidated_job_id)
                LOG.debug("SUBMISSION => Next jobs {} -> {}".format(newly_invalidated_job_id, next_job_to_run_ids))
                for next_job_to_run_id in next_job_to_run_ids:
                    self.add_to_work_queue(next_job_to_run_id)
            self.last_job_submitted_on = arrow.now()
            self.submitted_jobs += 1

        self._update_build(update_build_graph)

    def _update_parents_should_not_run_recurse(self, job_id):
        build_graph = self.build
        job = build_graph.get_job(job_id)

        if not job.get_parents_should_run or job.should_ignore_parents():
            return

        job.invalidate()

        if job.get_parents_should_run() or job.get_should_run():
            return

        target_ids = build_graph.get_target_ids(job_id)
        for target_id in target_ids:
            dependent_ids = build_graph.get_dependent_ids(target_id)
            for dependent_id in dependent_ids:
                self._update_parents_should_not_run_recurse(dependent_id)

    def _update_parents_should_run_recurse(self, job_id):
        build_graph = self.build
        job = build_graph.get_job(job_id)

        if job.get_parents_should_run() or job.should_ignore_parents():
            return

        job.invalidate()

        target_ids = build_graph.get_target_ids(job_id)
        for target_id in target_ids:
            dependent_ids = build_graph.get_dependent_ids(target_id)
            for dependent_id in dependent_ids:
                self._update_parents_should_run_recurse(dependent_id)

    def update_parents_should_run(self, job_id):
        build_graph = self.build
        job = build_graph.get_job(job_id)
        job.invalidate()

        target_ids = build_graph.get_target_ids(job_id)
        dependent_ids = []
        for target_id in target_ids:
            dependent_ids = (dependent_ids +
                             build_graph.get_dependent_ids(target_id))

        if job.get_should_run() or job.get_parents_should_run():
            for dependent_id in dependent_ids:
                self._update_parents_should_run_recurse(dependent_id)
        else:
            for dependent_id in dependent_ids:
                self._update_parents_should_not_run_recurse(dependent_id)

    def external_update_targets(self, target_ids):
        """Updates the state of a single target and updates everything below
        it
        """
        build_graph = self.build
        update_job_ids = set()
        self.update_targets(target_ids)
        for target_id in target_ids:
            add_ids = build_graph.get_creator_ids(target_id)
            if not add_ids:
                add_ids = build_graph.get_dependent_ids(target_id)
            for add_id in add_ids:
                update_job_ids.add(add_id)

        LOG.debug("after updating targets, {} jobs are being updated".format(len(update_job_ids)))
        for update_job_id in update_job_ids:
            self.update_parents_should_run(update_job_id)

        # Update the upper jobs that states depend on the target
        for update_job_id in update_job_ids:
            next_job_to_run_ids = self.get_next_jobs_to_run(update_job_id)
            update_job = self.build.get_job(update_job_id)
            for next_job_to_run_id in next_job_to_run_ids:
                self.add_to_work_queue(next_job_to_run_id)

    def update_top_most(self):
        top_most = []
        for node_id in self.build:
            if self.build.in_degree(node_id) == 0:
                if self.build.is_target(node_id):
                    top_most.append(node_id)
        LOG.debug("TOP_MOST_JOBS => {}".format(top_most))
        self.external_update_targets(top_most)

    def update_targets(self, target_ids):
        """Takes in a list of target ids and updates all of their needed
        values
        """
        LOG.debug("updating {} targets".format(len(target_ids)))
        update_function_list = collections.defaultdict(list)
        for target_id in target_ids:
            target = self.build.get_target(target_id)
            func = target.get_bulk_exists_mtime
            update_function_list[func].append(target)

        for update_function, targets in update_function_list.iteritems():
            update_function(targets)

    def add_to_work_queue(self, job_id):
        job = self.build.get_job(job_id)
        if job.is_running:
            return
        job.is_running = True
        self._work_queue.put(job_id)
        LOG.info("Adding {} to ExecutionManager's work queue. There are now approximately {} jobs in the queue.".format(job_id, self._work_queue.qsize()))


    def add_to_complete_queue(self, job_id):
        LOG.info("Adding {} to ExecutionManager's complete queue".format(job_id))
        self._complete_queue.put(job_id)

    def start_execution(self, inline=True):
        """
        Begin executing jobs
        """
        LOG.info("Starting execution")
        self.running = True
        self.start_time = arrow.now()
        self.executor.initialize()
        # Seed initial jobs
        work_queue = self._work_queue
        next_jobs = self.get_jobs_to_run()
        map(self.add_to_work_queue, next_jobs)

        # Start completed jobs consumer if not inline
        executor = None
        if not inline:
            executor = builder.futures.ThreadPoolExecutor(max_workers=3)
            executor.submit(self._consume_completed_jobs, block=True)
            executor.submit(self._check_for_timeouts)
            executor.submit(self._check_for_passed_curfews)

        jobs_executed = 0
        ONEYEAR = 365 * 24 * 60 * 60
        tick = 0
        while (not work_queue.empty() or not inline) and self.running:
            PROCESSING_LOG.debug("EXECUTION_LOOP => Getting job from the work queue. Tick {}".format(tick))
            tick += 1

            try:
                job_id = work_queue.get(True, timeout=1)
            except Queue.Empty:
                continue
            self.last_job_worked_on = arrow.now()

            TRANSITION_LOG.debug("EXECUTION_LOOP => Got job {} from work queue".format(job_id))
            result = self.execute(job_id)
            #LOG.debug("EXECUTION_LOOP => Finished job {} from work queue".format(job_id))
            jobs_executed += 1
            if not isinstance(result, concurrent.futures.Future) and inline:
                if result.is_async():
                    raise NotImplementedError("Cannot run an async executor inline")
                self._consume_completed_jobs(block=False)
            elif inline:
                TRANSITION_LOG.debug("EXECUTION_LOOP => Waiting on execution to complete")
                result.result() # Wait for job to complete
                self._consume_completed_jobs(block=False)
            TRANSITION_LOG.debug("EXECUTION_LOOP => Finished consuming completed jobs for {}".format(job_id))
            #else: It is an asynchronous result and we're running asynchronously, so let the _consume_completed_jobs
            # thread add new jobs
            LOG.debug("EXECUTION_LOOP => Executed {} jobs".format(jobs_executed))

        LOG.debug("EXECUTION_LOOP[work_queue] => Execution is exiting")
        if executor is not None:
            executor.shutdown(wait=True)

    def stop_execution(self):
        LOG.info("Stopping execution")
        self.running = False


    def _consume_completed_jobs(self, block=False):

        LOG.info("COMPLETION_LOOP => Consuming completed jobs")
        complete_queue = self._complete_queue
        tick = 0
        while (not complete_queue.empty() or block) and self.running:
            PROCESSING_LOG.debug("COMPLETION_LOOP => Getting job from the work queue. Tick {}".format(tick))
            tick += 1
            try:
                job_id = complete_queue.get(True, timeout=1)
            except Queue.Empty:
                continue
            self.last_job_completed_on = arrow.now()
            self.completed_jobs += 1

            try:
                job = self.build.get_job(job_id)
                del self.execution_times[job]
            except KeyError:
                pass


            TRANSITION_LOG.debug("COMPLETION_LOOP =>  Completed job {}".format(job_id))
            next_jobs = self.get_next_jobs_to_run(job_id)
            next_jobs = filter(lambda job_id: not self.build.get_job(job_id).is_running, next_jobs)
            TRANSITION_LOG.debug("COMPLETION_LOOP => Received completed job {}. Next jobs are {}".format(job_id, next_jobs))
            map(self.add_to_work_queue, next_jobs)
        LOG.debug("COMPLETION_LOOP => Done consuming completed jobs")

    def _check_for_timeouts(self):

        while self.running:
            PROCESSING_LOG.debug("TIMEOUTS => Checking for timeouts")
            timed_out_jobs = []
            now = arrow.get()
            for job, timestamp in self.execution_times.iteritems():
                if (now - timestamp).total_seconds() > self.job_timeout:
                    timed_out_jobs.append(job)
            for job in timed_out_jobs:
                self.execution_times.pop(job)
                self.executor.finish_job(job, ExecutionResult(is_async=False, status=False))

            _interruptable_sleep(10)

    def _check_for_passed_curfews(self):

        while self.running:
            PROCESSING_LOG.debug("CURFEWS => Checking for stale jobs past curfew")
            stale_jobs_past_curfew = []
            for node_id in self.build.node.keys():

                if (not node_id in self.build.node) or (not self.build.is_job(node_id)):
                    continue
                job = self.build.get_job(node_id)
                if job.past_curfew() and job.get_stale() and job.get_buildable():
                    job.invalidate()
                    if job.get_should_run():
                        stale_jobs_past_curfew.append(job)
                        self._work_queue.put(job.get_id())
            PROCESSING_LOG.debug("CURFEWS => These jobs were stale, past curfew, and should run: {}".format(
                stale_jobs_past_curfew))
            _interruptable_sleep(60)

    def get_next_jobs_to_run_recurse(self, job_id):
        next_job_ids = set()
        job = self.build.get_job(job_id)
        if job.get_should_run():
            next_job_ids.add(job_id)
        else:
            target_ids = self.build.get_target_ids(job_id)
            for target_id in target_ids:
                dependent_ids = self.build.get_dependent_ids(target_id)
                for dependent_id in dependent_ids:
                    next_job_ids |= self.get_next_jobs_to_run_recurse(
                            dependent_id)
        return next_job_ids

    def get_next_jobs_to_run(self, job_id):
        """Returns the jobs that are below job_id that need to run"""
        next_jobs = self.get_next_jobs_to_run_recurse(job_id)
        return next_jobs


    def execute(self, job_id):
        # Don't run a job more than the configured max number of retries
        self.last_job_executed_on = arrow.get()
        job = self.build.get_job(job_id)
        TRANSITION_LOG.info("EXECUTION => Executing {} ({})".format(job_id, job.get_command()))

        # Execute job
        result = self._execute(job)

        return result

    def _execute(self, job):
        self.execution_times[job] = arrow.get()
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

    def get_running_jobs(self):
        running_jobs = []
        for job, timestamp in self.execution_times.iteritems():
            running_jobs.append((job.get_id(), timestamp))

        return running_jobs


def _submit_from_json(execution_manager, json_body):
    payload = json.loads(json_body)
    LOG.debug("Submitting job {}".format(payload))

    # Clean up the payload a bit
    build_context = payload.get('build_context', {})
    for k in ('start_time', 'end_time'):
        if k in build_context:
            LOG.debug("converting {}".format(k))
            build_context[k] = arrow.get(build_context[k])
    LOG.debug("build_context is {}".format(build_context))

    execution_manager.submit(**payload)

def _update_from_json(execution_manager, json_body):
    payload = json.loads(json_body)
    LOG.debug("Updating target(s) {}".format(payload))

    execution_manager.update_targets(payload["target_ids"])


class SubmitHandler(RequestHandler):
    def initialize(self, execution_manager, work_queue):
        self.execution_manager = execution_manager
        self.work_queue = work_queue

    def post(self):
        LOG.debug("{}".format(self.request.body))
        self.work_queue.submit(_submit_from_json, self.execution_manager, self.request.body)

class UpdateHandler(RequestHandler):
    def initialize(self, execution_manager, work_queue):
        self.execution_manager = execution_manager
        self.work_queue = work_queue

    def post(self):
        LOG.debug("{}".format(self.request.body))
        self.work_queue.submit(_update_from_json, self.execution_manager, self.request.body)

class UpdateTopMostHandler(RequestHandler):
    def initialize(self, execution_manager, work_queue):
        self.execution_manager = execution_manager
        self.updating = False
        self.executor = work_queue

    def post(self):
        if self.updating:
            LOG.debug("Not updating; Update already in progress")
            self.write({"status": False, "message": "Update in progress"})
        self.updating = True
        LOG.debug("Updating topmost")
        self.executor.submit(self.update)
        self.write({"status": True, "message": "Updating"})

    def update(self):
        self.execution_manager.update_top_most()
        LOG.debug("Updating topmost complete")
        self.updating = False

class StatusHandler(RequestHandler):
    def initialize(self, execution_manager):
        self.execution_manager = execution_manager

    def get(self):
        status = {
            'n_submitted_jobs': self.execution_manager.submitted_jobs,
            'n_completed_jobs': self.execution_manager.completed_jobs,
            'running_jobs': [(k, v.isoformat()) for k, v in self.execution_manager.get_running_jobs()],
            'last_job_executed_on': unicode(self.execution_manager.last_job_executed_on),
            'last_job_submitted_on': unicode(self.execution_manager.last_job_submitted_on),
            'last_job_completed_on': unicode(self.execution_manager.last_job_completed_on),
            'last_job_worked_on': unicode(self.execution_manager.last_job_worked_on),
            'n_build_graph_nodes': len(self.execution_manager.get_build().node),
            'n_rdg_nodes': len(self.execution_manager.get_build_manager().get_rule_dependency_graph().node)
        }

        self.write(status)

class RDGHandler(RequestHandler):
    def initialize(self, execution_manager):
        self.execution_manager = execution_manager
        self.build_manager = self.execution_manager.get_build_manager()

    def get(self):
        LOG.info("Getting RDG")
        rdg = self.build_manager.get_rule_dependency_graph()
        data = nx.to_agraph(rdg).string()
        self.write(data)



class BuildGraphHandler(RequestHandler):
    ALL = object()

    def initialize(self, execution_manager):
        self.execution_manager = execution_manager
        self.build_manager = self.execution_manager.get_build_manager()


    def get(self, format='json'):
        LOG.info("Getting build graph as dot format")
        build_graph = self.execution_manager.get_build()

        LOG.debug("Updating graph display status")

        # Update colors based on existence
        include_edges = self.get_argument("edges", default=None)
        query = {
            'job_definition_ids': self.get_arguments('job-definition'),
            'expander_ids': self.get_arguments('expander'),
            'includes': self.get_arguments('include'),
            'excludes': self.get_arguments('exclude'),
            'include_neighbors': self.get_argument('neighbors', default=None)
        }
        transformer = builder.build.BuildGraphTransformer(build_graph)
        #data = self._convert_graph_to_json_and_update(build_graph, include_edges, query)

        LOG.debug("Finished updating graph display status")
        if format == 'dot':
            data = transformer.to_dot(query)
            self.write(data)
        elif format == 'html':
            self.render('build-graph.html')
        elif format in {'pdf', 'png', 'jpg'}:
            dot_file = tempfile.NamedTemporaryFile()
            transformer.write_dot(dot_file.name, query)
            with open(dot_file.name) as f:
                length = len(f.read())
                if length > 4e6:
                    self.write("Error: Graph is too big, not converting")
                    return
            pdf_file = tempfile.NamedTemporaryFile()
            subprocess.call(['/usr/bin/dot', '-T'+format, dot_file.name, '-o', pdf_file.name])
            pdf_file.seek(0)
            self.write(pdf_file.read())
            if format == 'pdf':
                mime_type = 'application/pdf'
            elif format == 'png':
                mime_type = 'image/png'
            elif format == 'jpg':
                mime_type = 'image/jpeg'
            self.set_header('Content-Type', mime_type)
        else:
            data = transformer.to_json(include_edges, query)
            self.write(data)



class ExecutionDaemon(object):

    def __init__(self, execution_manager, port=20345, debug=False):
        work_queue = builder.futures.ThreadPoolExecutor(max_workers=2)
        self.execution_manager = execution_manager
        self.application = Application([
            (r"/submit", SubmitHandler, {"execution_manager" : self.execution_manager, "work_queue": work_queue}),
            (r"/update", UpdateHandler, {"execution_manager" : self.execution_manager, "work_queue": work_queue}),
            (r"/update_top_most", UpdateTopMostHandler, {"execution_manager" : self.execution_manager,
                                                         "work_queue": work_queue}),
            (r"/status", StatusHandler, {"execution_manager" : self.execution_manager}),
            (r"/rdg", RDGHandler, {"execution_manager" : self.execution_manager}),
            (r"/build-graph\.?(?P<format>[^\/]+)?", BuildGraphHandler, {"execution_manager" : self.execution_manager}),
            (r'/static/(.*)', StaticFileHandler, {'path': os.path.join(os.path.dirname(__file__), 'static')}),
        ], template_path=os.path.join(os.path.dirname(__file__), 'static'), debug=debug)
        self.port = port
        self.is_closing = False
        self.debug = debug


    def signal_handler(self, signum, frame):
            LOG.info('exiting...')
            self.is_closing = True


    def try_exit(self):
        if self.is_closing:
            # clean up here
            ioloop.IOLoop.instance().stop()
            LOG.info('exit success')


    def start(self):
        self.is_closing = False

        signal.signal(signal.SIGINT, self.signal_handler)
        executor = builder.futures.ThreadPoolExecutor(max_workers=1)
        executor.submit(self.execution_manager.start_execution, inline=False)
        self.application.listen(self.port)
        LOG.info("Starting job listener")
        ioloop.PeriodicCallback(self.try_exit, 500).start()
        ioloop.IOLoop.instance().start()
        LOG.info("Shutting down")
        self.execution_manager.stop_execution()
        executor.shutdown()
        LOG.info("Shutting down")
