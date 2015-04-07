
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
            executor = mock.Mock()
            executor.execute = mock.Mock(return_value=(True, ''))
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
        self.assertIn('buildable_job_2015-01-01-00-00-00', execution_manager.get_build_graph())

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
        self.assertTrue(execution_manager.executor.execute.called)

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
                target.exists = True
                target.mtime = arrow.get()
                for dependent_id in build.get_dependent_ids(target_id):
                    dependent = build.get_job(dependent_id)
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
            SimpleTestJobDefinition('A', targets=['target-A']),
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

    @unit
    def test_update_targets(self):
        build_manager = builder.build.BuildManager([], [])
        execution_manager = builder.execution.ExecutionManager(build_manager, mock.Mock())
        build = execution_manager.build

        target1 = builder.targets.LocalFileSystemTarget("", "target1", {})
        target2 = builder.targets.LocalFileSystemTarget("", "target2", {})
        target3 = builder.targets.S3BackedLocalFileSystemTarget("", "target3", {})
        target4 = builder.targets.S3BackedLocalFileSystemTarget("", "target4", {})
        target5 = builder.targets.S3BackedLocalFileSystemTarget("", "target5", {})
        build.add_node(target1)
        build.add_node(target2)
        build.add_node(target3)
        build.add_node(target4)
        build.add_node(target5)

        id_list = ["target1", "target2", "target3", "target4", "target5"]

        mock_mtime = mock_mtime_generator({
            "target1": 100,
            "target3": 500,
        })

        s3_mtimes = {
            "target4": 600,
        }

        def mock_s3_list(targets):
            return s3_mtimes

        with mock.patch("deepy.store.list_files_remote", mock_s3_list), \
             mock.patch("os.stat", mock_mtime):
            execution_manager.update_targets(id_list)

        self.assertTrue(build.node["target1"]["object"].exists)
        self.assertFalse(build.node["target2"]["object"].exists)
        self.assertTrue(build.node["target3"]["object"].exists)
        self.assertTrue(build.node["target4"]["object"].exists)
        self.assertFalse(build.node["target5"]["object"].exists)

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

        build.add_job_definition("update_job_cache_bottom", build_context)

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
                ["object"].exists) = False
        (build.node
                ["update_job_cache_top_01_target"]
                ["object"].mtime) = None
        (build.node
                ["update_job_cache_top_01_target"]
                ["object"].exists) = False
        (build.node
                ["update_job_cache_top_02_target"]
                ["object"].mtime) = None
        (build.node
                ["update_job_cache_top_02_target"]
                ["object"].exists) = False
        (build.node
                ["update_job_cache_top_03_target"]
                ["object"].mtime) = 100
        (build.node
                ["update_job_cache_top_03_target"]
                ["object"].exists) = True
        (build.node
                ["update_job_cache_middle_02_target"]
                ["object"].mtime) = 50
        (build.node
                ["update_job_cache_middle_02_target"]
                ["object"].exists) = True
        (build.node
                ["update_job_cache_middle_03_target"]
                ["object"].mtime) = 150
        (build.node
                ["update_job_cache_middle_03_target"]
                ["object"].exists) = True

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
                ["object"].get_stale(build))
        stale_old2 = (build.node
                ["update_job_cache_middle_02"]
                ["object"].get_stale(build))
        stale_old3 = (build.node
                ["update_job_cache_middle_03"]
                ["object"].get_stale(build))
        buildable_old1 = (build.node
                ["update_job_cache_middle_01"]
                ["object"].get_buildable(build))
        buildable_old2 = (build.node
                ["update_job_cache_middle_02"]
                ["object"].get_buildable(build))
        buildable_old3 = (build.node
                ["update_job_cache_middle_03"]
                ["object"].get_buildable(build))
        should_run_old1 = (build.node
                ["update_job_cache_middle_01"]
                ["object"].get_should_run(build))
        should_run_old2 = (build.node
                ["update_job_cache_middle_02"]
                ["object"].get_should_run(build))
        should_run_old3 = (build.node
                ["update_job_cache_middle_03"]
                ["object"].get_should_run(build))

        with mock.patch("os.stat", mock_mtime):
            execution_manager.update_job_cache("update_job_cache_top")

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
                ["object"].get_stale(build))
        stale_new2 = (build.node
                ["update_job_cache_middle_02"]
                ["object"].get_stale(build))
        stale_new3 = (build.node
                ["update_job_cache_middle_03"]
                ["object"].get_stale(build))
        buildable_new1 = (build.node
                ["update_job_cache_middle_01"]
                ["object"].get_buildable(build))
        buildable_new2 = (build.node
                ["update_job_cache_middle_02"]
                ["object"].get_buildable(build))
        buildable_new3 = (build.node
                ["update_job_cache_middle_03"]
                ["object"].get_buildable(build))
        should_run_new1 = (build.node
                ["update_job_cache_middle_01"]
                ["object"].get_should_run(build))
        should_run_new2 = (build.node
                ["update_job_cache_middle_02"]
                ["object"].get_should_run(build))
        should_run_new3 = (build.node
                ["update_job_cache_middle_03"]
                ["object"].get_should_run(build))

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

        build.add_job_definition("update_target_cache_bottom", build_context)

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
                ["object"].exists) = False
        (build.node
                ["update_target_cache_top_02_target"]
                ["object"].mtime) = None
        (build.node
                ["update_target_cache_top_02_target"]
                ["object"].exists) = False
        (build.node
                ["update_target_cache_top_03_target"]
                ["object"].mtime) = 100
        (build.node
                ["update_target_cache_top_03_target"]
                ["object"].exists) = True
        (build.node
                ["update_target_cache_middle_02_target"]
                ["object"].mtime) = 50
        (build.node
                ["update_target_cache_middle_02_target"]
                ["object"].exists) = True
        (build.node
                ["update_target_cache_middle_03_target"]
                ["object"].mtime) = 150
        (build.node
                ["update_target_cache_middle_03_target"]
                ["object"].exists) = True

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
                ["object"].get_stale(build))
        stale_old2 = (build.node
                ["update_target_cache_middle_02"]
                ["object"].get_stale(build))
        stale_old3 = (build.node
                ["update_target_cache_middle_03"]
                ["object"].get_stale(build))
        buildable_old1 = (build.node
                ["update_target_cache_middle_01"]
                ["object"].get_buildable(build))
        buildable_old2 = (build.node
                ["update_target_cache_middle_02"]
                ["object"].get_buildable(build))
        buildable_old3 = (build.node
                ["update_target_cache_middle_03"]
                ["object"].get_buildable(build))
        should_run_old1 = (build.node
                ["update_target_cache_middle_01"]
                ["object"].get_should_run(build))
        should_run_old2 = (build.node
                ["update_target_cache_middle_02"]
                ["object"].get_should_run(build))
        should_run_old3 = (build.node
                ["update_target_cache_middle_03"]
                ["object"].get_should_run(build))

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
                ["object"].get_stale(build))
        stale_new2 = (build.node
                ["update_target_cache_middle_02"]
                ["object"].get_stale(build))
        stale_new3 = (build.node
                ["update_target_cache_middle_03"]
                ["object"].get_stale(build))
        buildable_new1 = (build.node
                ["update_target_cache_middle_01"]
                ["object"].get_buildable(build))
        buildable_new2 = (build.node
                ["update_target_cache_middle_02"]
                ["object"].get_buildable(build))
        buildable_new3 = (build.node
                ["update_target_cache_middle_03"]
                ["object"].get_buildable(build))
        should_run_new1 = (build.node
                ["update_target_cache_middle_01"]
                ["object"].get_should_run(build))
        should_run_new2 = (build.node
                ["update_target_cache_middle_02"]
                ["object"].get_should_run(build))
        should_run_new3 = (build.node
                ["update_target_cache_middle_03"]
                ["object"].get_should_run(build))

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
