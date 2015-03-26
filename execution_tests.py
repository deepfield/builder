
import mock
import unittest

import arrow

import builder.build
import builder.execution
import builder.tests_jobs


class ExecutionManagerTests(unittest.TestCase):

    def _get_execution_manager(self):
        build_manager = builder.build.BuildManager(jobs=[builder.tests_jobs.BuildableJobTester()], metas=[])
        executor = mock.Mock()
        execution_manager = builder.execution.ExecutionManager(build_manager, executor)

        return execution_manager

    def test_submit(self):
        # Given
        execution_manager = self._get_execution_manager()
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.submit('buildable_job', build_context)

        # Then
        self.assertIn('buildable_job_2015-01-01-00-00-00', execution_manager.get_build_graph())

    def test_get_next_jobs(self):
        # Given
        execution_manager = self._get_execution_manager()
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.submit('buildable_job', build_context, force=True)
        next_jobs = execution_manager.get_next_jobs()

        # Then
        self.assertEquals(len(next_jobs), 1)