
import mock
import unittest

import arrow
import funcy

import builder.build
import builder.execution
import builder.tests_jobs
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
        execution_manager = self._get_execution_manager([builder.tests_jobs.BuildableJobTester()])
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
        next_jobs = execution_manager.get_jobs_to_run()

        # Then
        self.assertEquals(set(map(lambda x: x.unique_id, next_jobs)), {'should_run_recurse_job_02',
            'should_run_recurse_job_06', 'should_run_recurse_job_10', 'should_run_recurse_job_08'})


    @unit
    def test_start_excution_run_to_completion(self):
        # Given
        execution_manager = self._get_execution_manager([builder.tests_jobs.BuildableJobTester()])
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
            builder.tests_jobs.SimpleTestJob('A', targets=['target-A']),
            builder.tests_jobs.SimpleTestJob('B', depends=['target-A'], targets=['target-B1', 'target-B2'])
        ]
        executor = mock.Mock()
        execution_manager = self._get_execution_manager(jobs)
        def update_job(job, build):
            job.should_run = False
            target_ids = build.get_targets(job.get_id())
            for target_id in target_ids:
                target = build.get_target(target_id)
                target.exists = True
                target.mtime = arrow.get()
                for dependant_id in build.get_dependants(target_id):
                    dependant = build.get_job_state(dependant_id)
                    dependant.should_run = True
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