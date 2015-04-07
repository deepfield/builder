
import mock
import unittest

import arrow
import funcy

import builder.build
import builder.execution
from builder.tests.tests_jobs import *

from testing import unit


class ExecutionManagerTests(unittest.TestCase):

    def _get_execution_manager(self, jobs, executor=None):
        build_manager = builder.build.BuildManager(jobs=jobs, metas=[])

        if executor is None:
            executor = mock.Mock()
            executor.execute = mock.Mock(return_value=(True, ''))
        execution_manager = builder.execution.ExecutionManager(build_manager, executor)

        return execution_manager

    @unit
    def test_submit(self):
        # Given
        execution_manager = self._get_execution_manager([BuildableJobTester()])
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.submit('buildable_job', build_context)

        # Then
        self.assertIn('buildable_job_2015-01-01-00-00-00', execution_manager.get_build_graph())

    @unit
    def test_get_jobs_to_run(self):
        # Given
        jobs = [
            ShouldRunRecurseJob('should_run_recurse_job_01',
                depends=['should_run_recurse_target_00'],
                targets=['should_run_recurse_target_01']),
            ShouldRunRecurseJob('should_run_recurse_job_02',
                depends=['should_run_recurse_target_01'],
                targets=['should_run_recurse_target_02']),
            ShouldRunRecurseJob('should_run_recurse_job_03',
                depends=['should_run_recurse_target_02'],
                targets=['should_run_recurse_target_03'])
        ]
        jobs[0].should_run_immediate = False
        jobs[1].should_run_immediate = True
        jobs[2].should_run_immediate = False

        execution_manager = self._get_execution_manager(jobs)
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.submit('should_run_recurse_job_03', build_context)
        next_jobs = execution_manager.get_jobs_to_run()

        # Then
        self.assertEquals(set(map(lambda x: x.unique_id, next_jobs)), {'should_run_recurse_job_02'})


    @unit
    def test_start_excution_run_to_completion(self):
        # Given
        execution_manager = self._get_execution_manager([BuildableJobTester()])
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.submit('buildable_job', build_context, force=True)
        execution_manager.start_execution(inline=True)

        # Then
        self.assertTrue(execution_manager.executor.execute.called)

    @unit
    def test_inline_execution_simple_plan(self):
        # Given
        jobs = [
            SimpleTestJob('A', targets=['target-A']),
            SimpleTestJob('B', depends=['target-A'], targets=['target-B1', 'target-B2'])
        ]
        executor = mock.Mock()
        execution_manager = self._get_execution_manager(jobs)
        def update_job(job, build):
            job.should_run = False
            target_ids = build.get_target_ids(job.get_id())
            for target_id in target_ids:
                target = build.get_target(target_id)
                target.exists = True
                target.mtime = arrow.get()
                for dependent_id in build.get_dependent_ids(target_id):
                    dependent = build.get_job_state(dependent_id)
                    dependent.should_run = True
            return True, ''

        executor.execute = mock.Mock(side_effect=update_job)
        execution_manager.executor = executor
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.submit('B', build_context)
        execution_manager.start_execution(inline=True)

        # Then
        self.assertEquals(executor.execute.call_count, 2)

    @unit
    def test_inline_execution_retries(self):
        # Given
        jobs = [
            SimpleTestJob('A', targets=['target-A']),
        ]
        executor = mock.Mock()
        execution_manager = self._get_execution_manager(jobs)
        def update_job(job, build):
            job.should_run = True
            return False, ''

        executor.execute = mock.Mock(side_effect=update_job)
        execution_manager.executor = executor
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.submit('A', build_context)
        execution_manager.start_execution(inline=True)

        # Then
        self.assertEquals(executor.execute.call_count, 5)
