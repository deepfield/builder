
import mock
import numbers
import unittest

import funcy

import builder.build
import builder.execution
from builder.tests.tests_jobs import *
from builder.build import BuildManager
from builder.execution import Executor, ExecutionManager
from builder.expanders import TimestampExpander

import deepy.timerange
arrow = deepy.timerange.arrow_factory

from testing import unit, mock_mtime_generator

class MockExecutor(Executor):
    """ "Executes" by getting the job's command, logging it, and setting the target as available
    """
    should_update_build_graph = False

    def __init__(self, mtime=None):
        if mtime is None:
            mtime = arrow.get().timestamp
        self.mtime = mtime


    def do_execute(self, job):
        build_graph = job.build_graph
        command = job.get_command()
        target_ids = build_graph.get_target_ids(job.get_id())
        for target_id in target_ids:
            target = build_graph.get_target(target_id)
            target.do_get_mtime = mock.Mock(return_value=self.mtime)
        job.invalidate()
        return True, command


class ExtendedMockExecutor(Executor):
    """"Executes" by running the jobs effect. An effect is a dictionary of
    things to do. Here is an example effect
    { }
    This effect updates non of the targets

    Here is another effect
    {
        "A-target": 500
    }
    This effect set's A's target's do_get_mtime to a mock thar returns 500
    """
    should_update_build_graph = True

    def do_execute(self, job):
        build_graph = job.build_graph
        command = job.get_command()
        effect = job.get_effect()
        if isinstance(effect, numbers.Number):
            success = True
        else:
            success = effect.get('success') or True

        target_ids = build_graph.get_target_ids(job.get_id())
        for target_id in target_ids:
            target = build_graph.get_target(target_id)
            if isinstance(effect, numbers.Number):
                target.do_get_mtime = mock.Mock(return_value=effect)
            elif target_id not in effect:
                continue
            else:
                target.do_get_mtime = mock.Mock(return_value=effect[target_id])

        return success, command


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


class ExecutionManagerTests(unittest.TestCase):

    def _get_execution_manager(self, jobs):
        build_manager = BuildManager(jobs, metas=[])
        execution_manager = ExecutionManager(build_manager, MockExecutor(mtime=arrow.get('2015-01-01').timestamp))
        return execution_manager

    def _get_execution_manager_with_effects(self, jobs):
        build_manager = BuildManager(jobs, metas=[])
        execution_manager = ExecutionManager(build_manager, ExtendedMockExecutor())
        return execution_manager

    @unit
    def test_no_depends_next_jobs(self):
        """tests_no_depends_next_jobs
        tests a situation where nothing depends on the job. When the job
        finishes, nothing should be returned as the next job to run
        """

        # Given
        jobs = [SimpleTestJobDefinition("A",
            depends=None,
            targets=["A-target"])]
        execution_manager = self._get_execution_manager(jobs)


        # When
        execution_manager.submit("A", {})
        execution_manager.execute("A")

        # Then
        self.assertEquals([], execution_manager.get_next_jobs_to_run("A"))


    @unit
    def test_simple_get_next_jobs(self):
        """test_simple_get_next_jobs
        test a situation where a job depends on a target of another job. When
        the depended on job finishes, the other job should be the next job to
        run
        """
        # Given
        jobs = [
            SimpleTestJobDefinition("A",
                depends=None,targets=["A-target"]),
            SimpleTestJobDefinition("B",
                depends=['A-target'], targets=["B-target"])
        ]
        execution_manager = self._get_execution_manager(jobs)

        # When
        execution_manager.submit("B", {})
        execution_manager.execute("A")

        # Then
        self.assertEquals(["B"], execution_manager.get_next_jobs_to_run("A"))

    @unit
    def test_simple_get_next_jobs_failed_but_creates_targets(self):
        """test_simple_get_next_jobs_failed
        test a situation where a job depends on a target of another job. When
        the depended on job finishes, but fails, does not reach it's max
        fail count, and creates targets, the dependent should be next job to run
        """
        # Given
        jobs = [
            SimpleTestJobDefinition("A",
                depends=None,targets=["A-target"]),
            SimpleTestJobDefinition("B",
                depends=['A-target'], targets=["B-target"])
        ]
        execution_manager = self._get_execution_manager(jobs)
        do_execute = execution_manager.executor.do_execute
        def mock_do_execute(job):
            do_execute(job)
            return False, ''

        execution_manager.executor.execute = mock.Mock(side_effect=mock_do_execute)

        # When
        execution_manager.submit("B", {})
        execution_manager.execute("A")

        # The
        self.assertEquals(["B"], execution_manager.get_next_jobs_to_run("A"))

    @unit
    def test_simple_get_next_jobs_failed_but_no_targets(self):
        """test_simple_get_next_jobs_failed
        test a situation where a job depends on a target of another job. When
        the depended on job finishes, but fails and does not reach it's max
        fail count, and does not create targets, the job should run again
        """
        # Given
        jobs = [
            SimpleTestJobDefinition("A",
                depends=None,targets=["A-target"]),
            SimpleTestJobDefinition("B",
                depends=['A-target'], targets=["B-target"])
        ]
        execution_manager = self._get_execution_manager(jobs)
        execution_manager.executor.execute = mock.Mock(return_value=(False, ''))

        # When
        execution_manager.submit("B", {})
        execution_manager.execute("A")

        # The
        self.assertEquals(["A"], execution_manager.get_next_jobs_to_run("A"))

    @unit
    def test_simple_get_next_jobs_failed_max(self):
        """test_simple_get_next_jobs_failed_max
        test a situation where a job depends on a target of another job.
        When the depended on job finishes, but fails and reaches it's max fail
        count, return nothing as the next job to run.
        """
        # Given
        jobs = [
            SimpleTestJobDefinition("A",
                depends=None,targets=["A-target"]),
            SimpleTestJobDefinition("B",
                depends=['A-target'], targets=["B-target"])
        ]
        execution_manager = self._get_execution_manager(jobs)
        execution_manager.executor.execute = mock.Mock(return_value=(False, ''))

        # When
        execution_manager.submit("B", {})
        for i in xrange(6):
            execution_manager.execute("A")

        # The
        self.assertEquals([], execution_manager.get_next_jobs_to_run("A"))

    @unit
    def test_multiple_get_next_jobs(self):
        """test_multiple_get_next_jobs
        test a situation where a job creates multiple targets where individual
        jobs depend on individual targets. When the depended on job finishes, all
        of the lower jobs should be the next job to run.
        """
        # Given
        jobs = [
            SimpleTestJobDefinition("A",
                depends=None,targets=["A-target"]),
            SimpleTestJobDefinition("B",
                depends=['A-target'], targets=["B-target"]),
            SimpleTestJobDefinition("C",
                depends=['A-target'], targets=["C-target"]),
            SimpleTestJobDefinition("D",
                depends=['A-target'], targets=["D-target"]),
        ]
        execution_manager = self._get_execution_manager(jobs)

        # When
        execution_manager.submit("A", {}, direction={"up", "down"})
        execution_manager.execute("A")

        # Then
        self.assertEquals({"B", "C", "D"}, set(execution_manager.get_next_jobs_to_run("A")))

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
        # Given
        jobs = [
            SimpleTestJobDefinition("A",
                depends=None,targets=["A1-target", "A2-target", "A3-target"]),
            SimpleTestJobDefinition("B",
                depends=['A1-target'], targets=["B-target"]),
            SimpleTestJobDefinition("C",
                depends=['A2-target'], targets=["C-target"]),
            SimpleTestJobDefinition("D",
                depends=['A3-target'], targets=["D-target"]),
        ]
        execution_manager = self._get_execution_manager(jobs)
        execution_manager.executor.execute = mock.Mock(return_value=(False, ''))
        execution_manager.submit("A", {}, direction={"up", "down"})
        execution_manager.build.get_target('A1-target').do_get_mtime = mock.Mock(return_value=None)

        # When
        execution_manager.execute("A")

        # Then
        self.assertEquals({"A"}, set(execution_manager.get_next_jobs_to_run("A")))

    @unit
    def test_multiple_get_next_jobs_failed_max(self):
        """test_multiple_get_next_jobs_failed_max
        test a situation where a job creates multiple targets where individual
        jobs depend on individual targets. When the depended on job finishes,
        but fails, reaches it's max fail count, and some targets are created,
        all the jobs below with dependencies that exist should be the next jobs
        to run.
        """
        # Given
        jobs = [
            SimpleTestJobDefinition("A",
                depends=None,targets=["A1-target", "A2-target", "A3-target"]),
            SimpleTestJobDefinition("B",
                depends=['A1-target'], targets=["B-target"]),
            SimpleTestJobDefinition("C",
                depends=['A2-target'], targets=["C-target"]),
            SimpleTestJobDefinition("D",
                depends=['A3-target'], targets=["D-target"]),
        ]
        execution_manager = self._get_execution_manager(jobs)
        execute = execution_manager.executor.execute
        def mock_execute(job):
            execute(job)
            return False, ''
        execution_manager.executor.execute = mock.Mock(side_effect=mock_execute)
        execution_manager.submit("A", {}, direction={"up", "down"})

        # When
        for i in xrange(6):
            execution_manager.execute("A")
        execution_manager.build.get_target('A1-target').do_get_mtime = mock.Mock(return_value=None)

        # Then
        self.assertEquals({"C", "D"}, set(execution_manager.get_next_jobs_to_run("A")))
        self.assertEquals(execution_manager.get_build().get_job("C").get_should_run(), True)
        self.assertEquals(execution_manager.get_build().get_job("D").get_should_run(), True)

    @unit
    def test_depends_one_or_more_next_jobs(self):
        """test_depends_one_or_more_next_jobs
        test a situation where a job has a depends one or more dependency. It is
        not past it's curfew so it needs all of the dependencies to run.
        Complete each of it's dependencies individually. Each one should return
        nothing until the last one.
        """
        # Given
        jobs = [
            EffectJobDefinition("A1",
                depends=None, targets=["A1-target"],
                effect={"A1-target": 1}),
            EffectJobDefinition("A2",
                depends=None, targets=["A2-target"],
                effect=[{"A2-target": None}, {"A2-target": 1}]),
            EffectJobDefinition("B",
                depends=[
                    {"unexpanded_id": "A1-target", "type": "depends_one_or_more"},
                    {"unexpanded_id": "A2-target", "type": "depends_one_or_more"}],
                targets=["B-target"])
        ]
        execution_manager = self._get_execution_manager_with_effects(jobs)
        build_context = {"start_time": arrow.get("2015-01-01-00-00"), "end_time": arrow.get("2015-01-01-00-10")}
        execution_manager.submit("B", build_context)

        # When
        execution_manager.execute("A1")
        execution_manager.execute("A2")

        # Then
        self.assertEquals(set(), set(execution_manager.get_next_jobs_to_run("A1")))
        self.assertEquals({"A2"}, set(execution_manager.get_next_jobs_to_run("A2")))
        self.assertEquals(execution_manager.get_build().get_job("B").get_should_run(), False)

        # On rerun, A2 complete successfully and therefore B should run
        execution_manager.execute("A2")
        self.assertEquals({"B"}, set(execution_manager.get_next_jobs_to_run("A1")))
        self.assertEquals({"B"}, set(execution_manager.get_next_jobs_to_run("A2")))
        self.assertEquals(execution_manager.get_build().get_job("B").get_should_run(), True)


    @unit
    def test_depends_one_or_more_next_jobs_failed_max_lower(self):
        """test_depends_one_or_more_next_jobs_failed
        test a situation where a job has a depends one or more dependency. It
        is not past it's curfew so it needs all of the dependencies to run.
        Each of the dependencies should also depend on a single job so there are
        a total of three layers of jobs. Complete each of the jobs in the first
        two rows except the last job. The last job in the first row should fail
        and reach it's max fail count. It's next job should be the job in the
        bottom row as all of it's buildable dependencies are built and all of
        the non buildable dependencies are due to a failure.
        """
        jobs = [
            EffectTimestampExpandedJobDefinition("A", file_step="5min",
                depends=None,
                targets=[{"unexpanded_id": "A-target-%Y-%m-%d-%H-%M", "file_step": "5min"}]),
            EffectTimestampExpandedJobDefinition("B", file_step="5min",
                depends=None,
                targets=[{"unexpanded_id": "B-target-%Y-%m-%d-%H-%M", "file_step": "5min"}],
                effect=[{"B-target-2015-01-01-00-00": 1, "B-target-2015-01-01-00-05": None, "success": False}]),
            EffectJobDefinition("C", expander_type=TimestampExpander,
                depends=[
                    {"unexpanded_id": "A-target-%Y-%m-%d-%H-%M", "file_step": "5min", "type": "depends_one_or_more"},
                    {"unexpanded_id": "B-target-%Y-%m-%d-%H-%M", "file_step": "5min", "type": "depends_one_or_more"}],
                targets=[{"unexpanded_id": "C-target-%Y-%m-%d-%H-%M", "file_step": "5min"}])
        ]
        execution_manager = self._get_execution_manager_with_effects(jobs)
        build_context = {"start_time": arrow.get("2015-01-01-00-00"), "end_time": arrow.get("2015-01-01-00-10")}
        execution_manager.submit("C", build_context)

        # When
        executions = ["A_2015-01-01-00-05-00", "A_2015-01-01-00-00-00", "B_2015-01-01-00-00-00"] + ["B_2015-01-01-00-05-00"]*6
        for execution in executions:
            execution_manager.execute(execution)

        # Then
        self.assertEquals(execution_manager.get_build().get_job("B_2015-01-01-00-05-00").get_should_run(), False)
        self.assertEquals(execution_manager.get_build().get_job("B_2015-01-01-00-05-00").get_stale(), True)
        for job_id in ("A_2015-01-01-00-05-00", "A_2015-01-01-00-00-00", "B_2015-01-01-00-00-00", "B_2015-01-01-00-05-00"):
            self.assertEquals({"C"}, set(execution_manager.get_next_jobs_to_run(job_id)))
        self.assertEquals(execution_manager.get_build().get_job("C").get_should_run(), True)


    @unit
    def test_upper_update(self):
        """tests situations where a job starts running and while it is running a
        job above it should run again, possibly due to a target being deleted
        or a force. When the running job finishes, none of it's lower jobs
        should run.
        """
        # Given
        jobs = [
            EffectJobDefinition("A",
                depends=None, targets=["A-target"],
                effect=[1,100]),
            EffectJobDefinition("B",
                depends=["A-target"], targets=["B-target"], effect=1),
            EffectJobDefinition("C",
                depends=["B-target"], targets=["C-target"], effect=1),
        ]
        execution_manager = self._get_execution_manager_with_effects(jobs)
        build_context = {}
        execution_manager.submit("C", build_context)

        # When
        execution_manager.execute("A")
        execution_manager.execute("B")
        execution_manager.submit("A", {}, force=True)

        # Then
        self.assertEquals(set(), set(execution_manager.get_next_jobs_to_run("B")))

    @unit
    def test_multiple_targets_one_exists(self):
        """tests situations where a job has multiple targets. There are
        individual jobs that depend on individual targets. There is also another
        row of jobs below that one. There is a total of three job rows. The top
        job has all of it's targets completed and all of the second row jobs
        have their targets completed. The top job has a target deleted. The top
        job runs again and updates that target but doesn't overwrite the other
        targets. Obviouslly the job that had one of it's dependencies updated
        should be a next job, but so should the third row jobs that no longer
        have a parent that should run. The event won't trickle down to them
        because their parents are not stale and won't run again.
        """

        # Given
        jobs = [
            EffectJobDefinition("A",
                depends=None, targets=["A1-target", "A2-target"],
                effect=[{"A1-target": 1, "A2-target": None}, {"A1-target": 1, "A2-target": None}, {"A1-target": 1, "A2-target": 4}]),
            EffectJobDefinition("B",
                depends=["A1-target"], targets=["B-target"], effect=2),
            EffectJobDefinition("C",
                depends=["A2-target"], targets=["C-target"], effect=[2, 5]),
            EffectJobDefinition("D",
                depends=["B-target"], targets=["D-target"], effect=3),
            EffectJobDefinition("E",
                depends=["C-target"], targets=["E-target"], effect=[3, 6]),
        ]
        execution_manager = self._get_execution_manager_with_effects(jobs)
        build_context = {}
        execution_manager.submit("A", build_context, direction={"down", "up"})

        # When
        for execution in ("A", "B", "C", "D", "E", "A", "A"):
            execution_manager.execute(execution)

        # Then
        self.assertEquals({"C"}, set(execution_manager.get_next_jobs_to_run("A")))
        self.assertEquals(execution_manager.get_build().get_job("C").get_should_run(), True)
        execution_manager.execute("C")
        self.assertEquals({"E"}, set(execution_manager.get_next_jobs_to_run("C")))
        self.assertEquals(execution_manager.get_build().get_job("E").get_should_run(), True)


    @unit
    def test_effect_job(self):
        """test_effect_job
        tests a situtation where a job starts running and then updates it's
        targets and then the next job will run
        """
        # Given
        jobs = [
            EffectJobDefinition("A", targets=["A-target"]),
            EffectJobDefinition("B", depends=["A-target"], targets=["B-target"]),
        ]
        execution_manager = self._get_execution_manager_with_effects(jobs)

        # When
        execution_manager.submit("B", {})
        execution_manager.build.write_dot("graph.dot")
        execution_manager.start_execution(inline=True)

        # Then
        job_A = execution_manager.build.get_job("A")
        job_B = execution_manager.build.get_job("B")

        self.assertEqual(job_A.count, 1)
        self.assertEqual(job_B.count, 1)
