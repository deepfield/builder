
import mock
import unittest

import arrow
import funcy

import builder.build
import builder.execution
from builder.tests.tests_jobs import *

from testing import unit, mock_mtime_generator


class ExecutionManagerTests(unittest.TestCase):

    def _get_execution_manager(self, jobs, executor=None):
        build_manager = builder.build.BuildManager(jobs=jobs, metas=[])

        if executor is None:
            executor = mock.Mock(return_value=(True, ''))
        execution_manager = builder.execution.ExecutionManager(build_manager, executor)

        return execution_manager

    @unit
    def test_submit(self):
        # Given
        execution_manager = self._get_execution_manager([self._get_buildable_job()])
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.submit('buildable_job', build_context)

        # Then
        self.assertIn('buildable_job_2015-01-01-00-00-00', execution_manager.get_build())

    @unit
    def test_get_jobs_to_run(self):
        # Given
        jobs = [
            ShouldRunRecurseJobDefinition('should_run_recurse_job_01',
                depends=['should_run_recurse_target_00'],
                targets=['should_run_recurse_target_01']),
            ShouldRunRecurseJobDefinition('should_run_recurse_job_02',
                depends=['should_run_recurse_target_01'],
                targets=['should_run_recurse_target_02']),
            ShouldRunRecurseJobDefinition('should_run_recurse_job_03',
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

    def _get_buildable_job(self):
        return SimpleTimestampExpandedTestJob("buildable_job", file_step="15min",
                depends=[{"unexpanded_id": "buildable_15_minute_target_01-%Y-%m-%d-%H-%M", "file_step": "15min"},
                    {"unexpanded_id": "buildable_5_minute_target_01-%Y-%m-%d-%H-%M", "file_step": "5min"},
                    {"unexpanded_id": "buildable_15_minute_target_02-%Y-%m-%d-%H-%M", "file_step": "15min",
                        "type": "depends_one_or_more"},
                    {"unexpanded_id": "buildable_5_minute_target_02-%Y-%m-%d-%H-%M", "file_step": "5min",
                        "type": "depends_one_or_more"}])

    @unit
    def test_start_excution_run_to_completion(self):
        # Given
        execution_manager = self._get_execution_manager([self._get_buildable_job()])
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.submit('buildable_job', build_context, force=True)
        execution_manager.start_execution(inline=True)

        # Then
        self.assertTrue(execution_manager.executor.called)

    @unit
    def test_inline_execution_simple_plan(self):
        # Given
        jobs = [
            SimpleTestJobDefinition('A', targets=['target-A']),
            SimpleTestJobDefinition('B', depends=['target-A'], targets=['target-B1', 'target-B2'])
        ]
        executor = mock.Mock()
        execution_manager = self._get_execution_manager(jobs)
        def update_job(job, build):
            job.should_run = False
            target_ids = build.get_target_ids(job.get_id())
            for target_id in target_ids:
                target = build.get_target(target_id)
                target.cached_mtime = True
                target.mtime = arrow.get()
                for dependent_id in build.get_dependent_ids(target_id):
                    dependent = build.get_job(dependent_id)
                    dependent.should_run = True
            return True, ''

        executor = mock.Mock(side_effect=update_job)
        execution_manager.executor = executor
        build_context = {
            'start_time': arrow.get('2015-01-01')
        }

        # When
        execution_manager.submit('B', build_context)
        execution_manager.start_execution(inline=True)

        # Then
        self.assertEquals(executor.call_count, 2)

    @unit
    def test_inline_execution_retries(self):
        # Given
        jobs = [
            SimpleTestJobDefinition('A', targets=['target-A']),
        ]
        executor = mock.Mock(return_value=(True, ""))
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
        self.assertEquals(executor.call_count, 5)

    @unit
    def test_update_targets(self):
        build_manager = builder.build.BuildManager([], [])
        execution_manager = builder.execution.ExecutionManager(build_manager, mock.Mock(return_value=(True, '')))
        build = execution_manager.build

        target1 = builder.targets.LocalFileSystemTarget("", "target1", {})
        target2 = builder.targets.LocalFileSystemTarget("", "target2", {})
        target3 = builder.targets.LocalFileSystemTarget("", "target3", {})
        target4 = builder.targets.LocalFileSystemTarget("", "target4", {})
        target5 = builder.targets.LocalFileSystemTarget("", "target5", {})
        build.add_node(target1)
        build.add_node(target2)
        build.add_node(target3)
        build.add_node(target4)
        build.add_node(target5)

        id_list = ["target1", "target2", "target3", "target4", "target5"]

        mock_mtime = mock_mtime_generator({
            "target1": 100,
            "target3": 500,
            "target4": 600,

        })

        with mock.patch("os.stat", mock_mtime):
            execution_manager.update_targets(id_list)

        self.assertTrue(build.node["target1"]["object"].get_exists())
        self.assertFalse(build.node["target2"]["object"].get_exists())
        self.assertTrue(build.node["target3"]["object"].get_exists())
        self.assertTrue(build.node["target4"]["object"].get_exists())
        self.assertFalse(build.node["target5"]["object"].get_exists())

        self.assertEqual(build.node["target1"]["object"].mtime, 100)
        self.assertEqual(build.node["target2"]["object"].mtime, None)
        self.assertEqual(build.node["target3"]["object"].mtime, 500)
        self.assertEqual(build.node["target4"]["object"].mtime, 600)
        self.assertEqual(build.node["target5"]["object"].mtime, None)


    @unit
    def test_update_job_cache(self):
        # Given
        jobs = [
            SimpleTestJobDefinition("update_job_cache_top",
                target_type=builder.targets.LocalFileSystemTarget,
                targets=["update_job_cache_top_01_target",
                         "update_job_cache_top_02_target", "update_job_cache_top_03_target"],
                depends=["update_job_cache_highest_target"]),
            SimpleTestJobDefinition("update_job_cache_middle_01",
                targets=["update_job_cache_middle_01_target"],
                depends=["update_job_cache_top_01_target"]),
            SimpleTestJobDefinition("update_job_cache_middle_02",
                targets=["update_job_cache_middle_02_target"],
                depends=["update_job_cache_top_02_target"]),
            SimpleTestJobDefinition("update_job_cache_middle_03",
                targets=["update_job_cache_middle_03_target"],
                depends=["update_job_cache_top_03_target"]),
            SimpleTestJobDefinition("update_job_cache_bottom",
                targets=["update_job_cache_bottom_target"],
                depends=["update_job_cache_middle_01_target",
                         "update_job_cache_middle_02_target", "update_job_cache_middle_03_target"]),
        ]

        build_manager = builder.build.BuildManager(jobs, [])
        execution_manager = builder.execution.ExecutionManager(build_manager, mock.Mock())
        build = execution_manager.build

        build_context = {
        }

        build.add_job("update_job_cache_bottom", build_context)

        mtime_dict = {
                "update_job_cache_top_01_target": None,
                "update_job_cache_top_02_target": 100,
                "update_job_cache_top_03_target": 100,
        }

        (build.node
                ["update_job_cache_highest_target"]
                ["object"].mtime) = 100
        (build.node
                ["update_job_cache_highest_target"]
                ["object"].cached_mtime) = True
        (build.node
                ["update_job_cache_top_01_target"]
                ["object"].mtime) = None
        (build.node
                ["update_job_cache_top_01_target"]
                ["object"].cached_mtime) = True
        (build.node
                ["update_job_cache_top_02_target"]
                ["object"].mtime) = None
        (build.node
                ["update_job_cache_top_02_target"]
                ["object"].cached_mtime) = True
        (build.node
                ["update_job_cache_top_03_target"]
                ["object"].mtime) = 100
        (build.node
                ["update_job_cache_top_03_target"]
                ["object"].cached_mtime) = True
        (build.node
                ["update_job_cache_middle_02_target"]
                ["object"].mtime) = 50
        (build.node
                ["update_job_cache_middle_02_target"]
                ["object"].cached_mtime) = True
        (build.node
                ["update_job_cache_middle_03_target"]
                ["object"].mtime) = 150
        (build.node
                ["update_job_cache_middle_03_target"]
                ["object"].cached_mtime) = True

        mock_mtime = mock_mtime_generator(mtime_dict)

        expected_mtime_old1 = None
        expected_mtime_old2 = None
        expected_mtime_old3 = 100
        expected_mtime_new1 = None
        expected_mtime_new2 = 100
        expected_mtime_new3 = 100

        expected_stale_old1 = True
        expected_stale_old2 = False
        expected_stale_old3 = False
        expected_stale_new1 = True
        expected_stale_new2 = True
        expected_stale_new3 = False

        expected_buildable_old1 = False
        expected_buildable_old2 = False
        expected_buildable_old3 = True
        expected_buildable_new1 = False
        expected_buildable_new2 = True
        expected_buildable_new3 = True

        expected_should_run_old1 = False
        expected_should_run_old2 = False
        expected_should_run_old3 = False
        expected_should_run_new1 = False
        expected_should_run_new2 = True
        expected_should_run_new3 = False

        # When
        mtime_old1 = (build.node
                ["update_job_cache_top_01_target"]
                ["object"].get_mtime())
        mtime_old2 = (build.node
                ["update_job_cache_top_02_target"]
                ["object"].get_mtime())
        mtime_old3 = (build.node
                ["update_job_cache_top_03_target"]
                ["object"].get_mtime())
        stale_old1 = (build.node
                ["update_job_cache_middle_01"]
                ["object"].get_stale())
        stale_old2 = (build.node
                ["update_job_cache_middle_02"]
                ["object"].get_stale())
        stale_old3 = (build.node
                ["update_job_cache_middle_03"]
                ["object"].get_stale())
        buildable_old1 = (build.node
                ["update_job_cache_middle_01"]
                ["object"].get_buildable())
        buildable_old2 = (build.node
                ["update_job_cache_middle_02"]
                ["object"].get_buildable())
        buildable_old3 = (build.node
                ["update_job_cache_middle_03"]
                ["object"].get_buildable())
        should_run_old1 = (build.node
                ["update_job_cache_middle_01"]
                ["object"].get_should_run())
        should_run_old2 = (build.node
                ["update_job_cache_middle_02"]
                ["object"].get_should_run())
        should_run_old3 = (build.node
                ["update_job_cache_middle_03"]
                ["object"].get_should_run())

        with mock.patch("os.stat", mock_mtime):
            execution_manager.update_job_cache("update_job_cache_top")

        build.node["update_job_cache_middle_01"]["object"].parents_should_run = False
        build.node["update_job_cache_middle_02"]["object"].parents_should_run = False
        build.node["update_job_cache_middle_03"]["object"].parents_should_run = False
        mtime_new1 = (build.node
                ["update_job_cache_top_01_target"]
                ["object"].get_mtime())
        mtime_new2 = (build.node
                ["update_job_cache_top_02_target"]
                ["object"].get_mtime())
        mtime_new3 = (build.node
                ["update_job_cache_top_03_target"]
                ["object"].get_mtime())
        stale_new1 = (build.node
                ["update_job_cache_middle_01"]
                ["object"].get_stale())
        stale_new2 = (build.node
                ["update_job_cache_middle_02"]
                ["object"].get_stale())
        stale_new3 = (build.node
                ["update_job_cache_middle_03"]
                ["object"].get_stale())
        buildable_new1 = (build.node
                ["update_job_cache_middle_01"]
                ["object"].get_buildable())
        buildable_new2 = (build.node
                ["update_job_cache_middle_02"]
                ["object"].get_buildable())
        buildable_new3 = (build.node
                ["update_job_cache_middle_03"]
                ["object"].get_buildable())
        should_run_new1 = (build.node
                ["update_job_cache_middle_01"]
                ["object"].get_should_run())
        should_run_new2 = (build.node
                ["update_job_cache_middle_02"]
                ["object"].get_should_run())
        should_run_new3 = (build.node
                ["update_job_cache_middle_03"]
                ["object"].get_should_run())

        # Then
        self.assertEqual(mtime_old1, expected_mtime_old1)
        self.assertEqual(mtime_old2, expected_mtime_old2)
        self.assertEqual(mtime_old3, expected_mtime_old3)
        self.assertEqual(stale_old1, expected_stale_old1)
        self.assertEqual(stale_old2, expected_stale_old2)
        self.assertEqual(stale_old3, expected_stale_old3)
        self.assertEqual(buildable_old1, expected_buildable_old1)
        self.assertEqual(buildable_old2, expected_buildable_old2)
        self.assertEqual(buildable_old3, expected_buildable_old3)
        self.assertEqual(should_run_old1, expected_should_run_old1)
        self.assertEqual(should_run_old2, expected_should_run_old2)
        self.assertEqual(should_run_old3, expected_should_run_old3)
        self.assertEqual(mtime_new1, expected_mtime_new1)
        self.assertEqual(mtime_new2, expected_mtime_new2)
        self.assertEqual(mtime_new3, expected_mtime_new3)
        self.assertEqual(stale_new1, expected_stale_new1)
        self.assertEqual(stale_new2, expected_stale_new2)
        self.assertEqual(stale_new3, expected_stale_new3)
        self.assertEqual(buildable_new1, expected_buildable_new1)
        self.assertEqual(buildable_new2, expected_buildable_new2)
        self.assertEqual(buildable_new3, expected_buildable_new3)
        self.assertEqual(should_run_new1, expected_should_run_new1)
        self.assertEqual(should_run_new2, expected_should_run_new2)
        self.assertEqual(should_run_new3, expected_should_run_new3)

    @unit
    def test_update_target_cache(self):
        # Given
        jobs = [
            SimpleTestJobDefinition("update_target_cache_middle_01",
                targets=["update_target_cache_middle_01_target"],
                depends=["update_target_cache_top_01_target"]),

            SimpleTestJobDefinition("update_target_cache_middle_02",
                targets=["update_target_cache_middle_02_target"],
                depends=["update_target_cache_top_02_target"]),

            SimpleTestJobDefinition("update_target_cache_middle_03",
                targets=["update_target_cache_middle_03_target"],
                depends=["update_target_cache_top_03_target"]),

            SimpleTestJobDefinition("update_target_cache_bottom",
                targets=["update_target_cache_bottom_target"],
                depends=["update_target_cache_middle_01_target",
                    "update_target_cache_middle_02_target",
                    "update_target_cache_middle_03_target"]),

            SimpleTestJobDefinition("update_target_cache_top",
                depends=["update_target_cache_highest_target"],
                targets=["update_target_cache_top_01_target",
                    "update_target_cache_top_02_target",
                    "update_target_cache_top_03_target"],
                target_type=builder.targets.LocalFileSystemTarget)
        ]

        build_manager = builder.build.BuildManager(jobs, [])
        execution_manager = builder.execution.ExecutionManager(build_manager, mock.Mock())
        build = execution_manager.build

        build_context = {
        }

        build.add_job("update_target_cache_bottom", build_context)

        mtime_dict = {
                "update_target_cache_top_01_target": None,
                "update_target_cache_top_02_target": 100,
                "update_target_cache_top_03_target": 100,
        }

        (build.node
                ["update_target_cache_top_01_target"]
                ["object"].mtime) = None
        (build.node
                ["update_target_cache_top_01_target"]
                ["object"].cached_mtime) = True
        (build.node
                ["update_target_cache_top_02_target"]
                ["object"].mtime) = None
        (build.node
                ["update_target_cache_top_02_target"]
                ["object"].cached_mtime) = True
        (build.node
                ["update_target_cache_top_03_target"]
                ["object"].mtime) = 100
        (build.node
                ["update_target_cache_top_03_target"]
                ["object"].cached_mtime) = True
        (build.node
                ["update_target_cache_middle_02_target"]
                ["object"].mtime) = 50
        (build.node
                ["update_target_cache_middle_02_target"]
                ["object"].cached_mtime) = True
        (build.node
                ["update_target_cache_middle_03_target"]
                ["object"].mtime) = 150
        (build.node
                ["update_target_cache_middle_03_target"]
                ["object"].cached_mtime) = True

        mock_mtime = mock_mtime_generator(mtime_dict)

        expected_mtime_old1 = None
        expected_mtime_old2 = None
        expected_mtime_old3 = 100
        expected_mtime_new1 = None
        expected_mtime_new2 = 100
        expected_mtime_new3 = 100

        expected_stale_old1 = True
        expected_stale_old2 = False
        expected_stale_old3 = False
        expected_stale_new1 = True
        expected_stale_new2 = True
        expected_stale_new3 = False

        expected_buildable_old1 = False
        expected_buildable_old2 = False
        expected_buildable_old3 = True
        expected_buildable_new1 = False
        expected_buildable_new2 = True
        expected_buildable_new3 = True

        expected_should_run_old1 = False
        expected_should_run_old2 = False
        expected_should_run_old3 = False
        expected_should_run_new1 = False
        expected_should_run_new2 = True
        expected_should_run_new3 = False

        # When
        mtime_old1 = (build.node
                ["update_target_cache_top_01_target"]
                ["object"].get_mtime())
        mtime_old2 = (build.node
                ["update_target_cache_top_02_target"]
                ["object"].get_mtime())
        mtime_old3 = (build.node
                ["update_target_cache_top_03_target"]
                ["object"].get_mtime())
        stale_old1 = (build.node
                ["update_target_cache_middle_01"]
                ["object"].get_stale())
        stale_old2 = (build.node
                ["update_target_cache_middle_02"]
                ["object"].get_stale())
        stale_old3 = (build.node
                ["update_target_cache_middle_03"]
                ["object"].get_stale())
        buildable_old1 = (build.node
                ["update_target_cache_middle_01"]
                ["object"].get_buildable())
        buildable_old2 = (build.node
                ["update_target_cache_middle_02"]
                ["object"].get_buildable())
        buildable_old3 = (build.node
                ["update_target_cache_middle_03"]
                ["object"].get_buildable())
        should_run_old1 = (build.node
                ["update_target_cache_middle_01"]
                ["object"].get_should_run())
        should_run_old2 = (build.node
                ["update_target_cache_middle_02"]
                ["object"].get_should_run())
        should_run_old3 = (build.node
                ["update_target_cache_middle_03"]
                ["object"].get_should_run())

        with mock.patch("os.stat", mock_mtime):
            execution_manager.update_target_cache("update_target_cache_top_01_target")
            execution_manager.update_target_cache("update_target_cache_top_02_target")
            execution_manager.update_target_cache("update_target_cache_top_03_target")

        mtime_new1 = (build.node
                ["update_target_cache_top_01_target"]
                ["object"].get_mtime())
        mtime_new2 = (build.node
                ["update_target_cache_top_02_target"]
                ["object"].get_mtime())
        mtime_new3 = (build.node
                ["update_target_cache_top_03_target"]
                ["object"].get_mtime())
        stale_new1 = (build.node
                ["update_target_cache_middle_01"]
                ["object"].get_stale())
        stale_new2 = (build.node
                ["update_target_cache_middle_02"]
                ["object"].get_stale())
        stale_new3 = (build.node
                ["update_target_cache_middle_03"]
                ["object"].get_stale())
        buildable_new1 = (build.node
                ["update_target_cache_middle_01"]
                ["object"].get_buildable())
        buildable_new2 = (build.node
                ["update_target_cache_middle_02"]
                ["object"].get_buildable())
        buildable_new3 = (build.node
                ["update_target_cache_middle_03"]
                ["object"].get_buildable())
        should_run_new1 = (build.node
                ["update_target_cache_middle_01"]
                ["object"].get_should_run())
        should_run_new2 = (build.node
                ["update_target_cache_middle_02"]
                ["object"].get_should_run())
        should_run_new3 = (build.node
                ["update_target_cache_middle_03"]
                ["object"].get_should_run())

        # Then
        self.assertEqual(mtime_old1, expected_mtime_old1)
        self.assertEqual(mtime_old2, expected_mtime_old2)
        self.assertEqual(mtime_old3, expected_mtime_old3)
        self.assertEqual(stale_old1, expected_stale_old1)
        self.assertEqual(stale_old2, expected_stale_old2)
        self.assertEqual(stale_old3, expected_stale_old3)
        self.assertEqual(buildable_old1, expected_buildable_old1)
        self.assertEqual(buildable_old2, expected_buildable_old2)
        self.assertEqual(buildable_old3, expected_buildable_old3)
        self.assertEqual(should_run_old1, expected_should_run_old1)
        self.assertEqual(should_run_old2, expected_should_run_old2)
        self.assertEqual(should_run_old3, expected_should_run_old3)
        self.assertEqual(mtime_new1, expected_mtime_new1)
        self.assertEqual(mtime_new2, expected_mtime_new2)
        self.assertEqual(mtime_new3, expected_mtime_new3)
        self.assertEqual(stale_new1, expected_stale_new1)
        self.assertEqual(stale_new2, expected_stale_new2)
        self.assertEqual(stale_new3, expected_stale_new3)
        self.assertEqual(buildable_new1, expected_buildable_new1)
        self.assertEqual(buildable_new2, expected_buildable_new2)
        self.assertEqual(buildable_new3, expected_buildable_new3)
        self.assertEqual(should_run_new1, expected_should_run_new1)
        self.assertEqual(should_run_new2, expected_should_run_new2)
        self.assertEqual(should_run_new3, expected_should_run_new3)

    @unit
    def test_no_depends_next_jobs(self):
        """tests_no_depends_next_jobs
        tests a situation where nothing depends on the job. When the job
        finishes, nothing should be returned as the next job to run
        """

    @unit
    def test_simple_get_next_jobs(self):
        """test_simple_get_next_jobs
        test a situation where a job depends on a target of another job. When
        the depended on job finishes, the other job should be the next job to
        run
        """

    @unit
    def test_simple_get_next_jobs_failed(self):
        """test_simple_get_next_jobs_failed
        TODO: decide what should happen
        test a situation where a job depends on a target of another job. When
        the depended on job finishes, but fails and does not reach it's max
        fail count, either the failed job should be the next job to run or
        nothing should be the next job to run.
        """

    @unit
    def test_simple_get_next_jobs_failed_max(self):
        """test_simple_get_next_jobs_failed_max
        test a situation where a job depends on a target of another job.
        When the dpended on job finishes, but fails and reaches it's max fail
        count, return nothing as the next job to run.
        """

    @unit
    def test_multiple_get_next_jobs(self):
        """test_multiple_get_next_jobs
        test a situation where a job creates multiple targets where individual
        jobs depend on individual targets. When the depended on job finishes, all
        of the lower jobs should be the next job to run.
        """

    @unit
    def test_multiple_get_next_jobs_failed(self):
        """test_multiple_get_next_jobs_failed
        test a situation where a job creates multiple targets where individual
        jobs depend on individual targets. When the depended on job finishes,
        but fails and does not reach it's max fail count, either the failed job
        should be the next job to run or nothing should be the next job to run.
        When the depended on job finishes it should make some of it's targets.
        This tests to make sure that when the job fails, the job's that now have
        their dependencies don't run. This is not covered by should run as there
        is a possibility that the lower nodes are check for should run before
        the parent job is invalidated.
        """

    @unit
    def test_multiple_get_next_jobs_failed_max(self):
        """test_multiple_get_next_jobs_failed_max
        test a situation where a job creates multiple targets where individual
        jobs depend on individual targets. When the depended on job finishes,
        but fails, reaches it's max fail count, and some targets are created,
        all the jobs below with dependencies that exist should be the next jobs
        to run.
        """

    @unit
    def test_depends_one_or_more_next_jobs(self):
        """test_depends_one_or_more_next_jobs
        test a situation where a job has a depends one or more dependency. It is
        not past it's curfew so it needs all of the dependencies to run.
        Complete each of it's dependencies individually. Each one should return
        nothing until the last one.
        """

    @unit
    def test_depends_one_or_more_next_jobs_failed_max_lower(self):
        """test_depends_one_or_more_next_jobs_failed
        test a situation where a job hads a depends one or more dependency. It
        is not past it's curfew so it needs all of the dependencies to run.
        Each of the dependencies should also depend on a single job so there are
        a total of three layers of jobs. Complete each of the jobs in the first
        two rows except the last job. The last job in the first row should fail
        and reach it's max fail count. It's next job should be the job in the
        bottom row as all of it's buildable dependencies are built and all of
        the non buildable dependencies are due to a failure.
        """

    @unit
    def test_upper_update(self):
        """tests situations where a job starts running and while it is running a
        job above it should run again, possibly due to a target being deleted
        or a force. When the running job finishes, none of it's lower jobs
        should run.
        """
