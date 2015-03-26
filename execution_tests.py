
import mock
import unittest

import arrow

import builder.build
import builder.execution
import builder.tests_jobs
from testing import unit


class ExecutionManagerTests(unittest.TestCase):

    def _get_execution_manager(self, jobs):
        build_manager = builder.build.BuildManager(jobs=jobs, metas=[])
        executor = mock.Mock()
        execution_manager = builder.execution.ExecutionManager(build_manager, executor)

        return execution_manager

    @unit
    def test_submit(self):
        # Given
        execution_manager = self._get_execution_manager([builder.tests_jobs.BuildableJobTester()])
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.submit('buildable_job', build_context)

        # Then
        self.assertIn('buildable_job_2015-01-01-00-00-00', execution_manager.get_build_graph())

    @unit
    def test_get_next_jobs(self):
        # Given
        jobs = [
            builder.tests_jobs.ShouldRunRecurseJob01Tester(),
            builder.tests_jobs.ShouldRunRecurseJob02Tester(),
            builder.tests_jobs.ShouldRunRecurseJob03Tester(),
            builder.tests_jobs.ShouldRunRecurseJob04Tester(),
            builder.tests_jobs.ShouldRunRecurseJob05Tester(),
            builder.tests_jobs.ShouldRunRecurseJob06Tester(),
            builder.tests_jobs.ShouldRunRecurseJob07Tester(),
            builder.tests_jobs.ShouldRunRecurseJob08Tester(),
            builder.tests_jobs.ShouldRunRecurseJob09Tester(),
            builder.tests_jobs.ShouldRunRecurseJob10Tester(),
        ]
        execution_manager = self._get_execution_manager(jobs)
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.submit('should_run_recurse_job_10', build_context)
        next_jobs = execution_manager.get_next_jobs()

        # Then
        self.assertEquals(set(map(lambda x: x.unique_id, next_jobs)), {'should_run_recurse_job_02',
            'should_run_recurse_job_06', 'should_run_recurse_job_10'})

    