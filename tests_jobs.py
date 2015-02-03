"""Used to test the job api functions"""
import unittest

import arrow
import networkx

import builder
from builder.jobs import Job, TimestampExpandedJob
import testing
import builder.jobs
import builder.targets
import builder.expanders



class JobTest(unittest.TestCase):
    @testing.unit
    def test_get_command(self):
        # given
        timestamp1 = arrow.get("2014-12-05T20:35")

        build_context1 = {
            "start_time": timestamp1,
        }

        job1 = TimestampExpandedJobTester()
        job1_state = job1.expand({
            "start_time": arrow.get("2014-12-05T20:35"),
            "end_time": arrow.get("2014-12-05T20:35")
        })[0]
        build_graph = networkx.DiGraph()

        expected_command1 = ("timestamp expanded job tester command "
                             "%Y-%m-%d-%H-%M")

        # when
        command1 = job1_state.get_command(build_graph)

        # then
        self.assertEqual(command1, expected_command1)

    @testing.unit
    def test_expand(self):
        # given
        timestamp1 = arrow.get("2014-12-05T20:35")

        job_type1 = TimestampExpandedJobTester()

        build_context1 = {
            "start_time": timestamp1,
            "end_time": arrow.get("2014-12-05T20:40"),
        }

        expected_unique_id1 = "timestamp_expanded_job_2014-12-05-20-35"

        expected_command1 = ("timestamp expanded job tester command "
                             "%Y-%m-%d-%H-%M")

        # when
        expanded_nodes1 = job_type1.expand(build_context1)
        unique_id1 = expanded_nodes1[0].unique_id
        command1 = expanded_nodes1[0].get_command("blank command")

        # then
        self.assertEqual(len(expanded_nodes1), 1)
        self.assertEqual(unique_id1, expected_unique_id1)
        self.assertEqual(command1, expected_command1)


class JobDependsPast(Job):
    """A job that depends on a file from the past"""
    unexpanded_id = "job_depends_past"

    def get_dependencies(self, build_context=None):
        depends_dict = {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "standard_depends_target"),
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "standard_depends_past",
                    "5min",
                    past=3)
            ],
        }
        return depends_dict


class JobBackboneDependantTester(Job):
    """A job that has targets that depend on wether or not backbone is
    enabled
    """
    unexpanded_id = "job_backbone_dependant"

    def get_targets(self, build_context=None):
        targets = {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "non_backbone_target")
            ]
        }

        if self.config.get("has_backbone", False):
            targets["produces"].append(
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "backbone_target"))

        return targets


class JobAlternateTester(Job):
    """A job that has an alternate target"""
    unexpanded_id = "job_alternate"

    def get_targets(self, build_context=None):
        if build_context is None:
            build_context = {}

        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "non_backbone_target")
            ],
            "alternates": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "alternate_target")
            ],
        }


class UpdateTargetCacheBottom(Job):
    """The job at the bottom to easily build out the graph"""
    unexpanded_id = "update_target_cache_bottom"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_target_cache_bottom_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_target_cache_middle_01_target"),
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_target_cache_middle_02_target"),
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_target_cache_middle_03_target")
            ]
        }


class UpdateTargetCacheMiddle03(Job):
    """The job in the middle that is buildable and not stale and will not
    be updated
    """
    unexpanded_id = "update_target_cache_middle_03"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_target_cache_middle_03_target"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_target_cache_top_03_target")
            ]
        }


class UpdateTargetCacheMiddle02(Job):
    """The job in the middle that is not stale not buildable will be
    updated
    """
    unexpanded_id = "update_target_cache_middle_02"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_target_cache_middle_02_target"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_target_cache_top_02_target")
            ]
        }


class UpdateTargetCacheMiddle01(Job):
    """The job in the middle that will never be buildable but has it's"""
    unexpanded_id = "update_target_cache_middle_01"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_target_cache_middle_01_target"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_target_cache_top_01_target")
            ]
        }


class UpdateTargetCacheTop(Job):
    """The job at the top whose mtimes will be updated"""
    unexpanded_id = "update_target_cache_top"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_target_cache_top_01_target"),
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_target_cache_top_02_target"),
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_target_cache_top_03_target"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_target_cache_highest_target")
            ]
        }


class ForceBuildBottom(TimestampExpandedJob):
    """The middle job to not force"""
    unexpanded_id = "force_build_bottom"
    file_step = "15min"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.Target,
                    "force_build_top_target_%Y-%m-%d-%H-%M",
                    "1min"),
                builder.expanders.TimestampExpander(
                    builder.targets.Target,
                    "force_build_middle_target_%Y-%m-%d-%H-%M",
                    "5min"),
            ]
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.Target,
                    "force_build_bottom_target_%Y-%m-%d-%H-%M",
                    "15min"),
            ]
        }


class ForceBuildMiddle(TimestampExpandedJob):
    """The middle job to not force"""
    unexpanded_id = "force_build_middle"
    file_step = "5min"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.Target,
                    "force_build_top_target_%Y-%m-%d-%H-%M",
                    "1min"),
            ]
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.Target,
                    "force_build_middle_target_%Y-%m-%d-%H-%M",
                    "5min"),
            ]
        }


class ForceBuildTop(TimestampExpandedJob):
    """The top job to be force"""
    unexpanded_id = "force_build_top"
    file_step = "1min"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.Target,
                    "force_build_top_target_%Y-%m-%d-%H-%M",
                    "1min")
            ]
        }


class ExpandExactBottom(Job):
    """The bottom job"""
    unexpanded_id = "test_expand_exact_bottom"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "test_expand_exact_bottom_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "test_expand_exact_middle_target")
            ]
        }


class ExpandExactMiddle(Job):
    """The middle job"""
    unexpanded_id = "test_expand_exact_middle"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "test_expand_exact_middle_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "test_expand_exact_top_target")
            ]
        }


class ExpandExactTop(Job):
    """The top job"""
    unexpanded_id = "test_expand_exact_top"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "test_expand_exact_top_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "test_expand_exact_highest_target")
            ]
        }


class IsJobImmediatlyRunnable5min(Job):
    """The job is a 5min job"""
    unexpanded_id = "is_job_immediatly_runnable"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "is_job_immediatly_runnable_5min_target"
                    "_%Y-%m-%d-%H-%M",
                    "5min")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "is_job_immediatly_runnable_5min_depends"
                    "_%Y-%m-%d-%H-%M",
                    "5min")
            ]
        }

    def get_command(self):
        return "command"


class UpdateJobCacheBottom(Job):
    """The job at the bottom to easily build out the graph"""
    unexpanded_id = "update_job_cache_bottom"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_job_cache_bottom_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_job_cache_middle_01_target"),
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_job_cache_middle_02_target"),
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_job_cache_middle_03_target")
            ]
        }


class UpdateJobCacheMiddle03(Job):
    """The job in the middle that is buildable and not stale and will not
    be updated
    """
    unexpanded_id = "update_job_cache_middle_03"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_job_cache_middle_03_target"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_job_cache_top_03_target")
            ]
        }


class UpdateJobCacheMiddle02(Job):
    """The job in the middle that is not stale not buildable will be
    updated
    """
    unexpanded_id = "update_job_cache_middle_02"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_job_cache_middle_02_target"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_job_cache_top_02_target")
            ]
        }


class UpdateJobCacheMiddle01(Job):
    """The job in the middle that will never be buildable but has it's"""
    unexpanded_id = "update_job_cache_middle_01"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_job_cache_middle_01_target"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_job_cache_top_01_target")
            ]
        }


class UpdateJobCacheTop(Job):
    """The job at the top whose mtimes will be updated"""
    unexpanded_id = "update_job_cache_top"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_job_cache_top_01_target"),
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_job_cache_top_02_target"),
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_job_cache_top_03_target"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "update_job_cache_highest_target")
            ]
        }


class GetNextJobsCounter(Job):
    """Used to count how many times the next job was looked for"""

    def __init__(self, config=None):
        super(GetNextJobsCounter, self).__init__(config=config)
        self.count = 0

    def get_next_jobs_to_run(self, job_id, update_set=None):
        """Counts the number of times it is called"""
        self.count += 1
        return super(GetNextJobsCounter, self).get_next_jobs_to_run(
            job_id, update_set=update_set)


class GetNextJobsToRunLowest(GetNextJobsCounter):
    """A job at the bottom of the graph"""
    unexpanded_id = "get_next_jobs_to_run_lowest"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "get_next_jobs_to_run_lowest_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "get_next_jobs_to_run_bottom_target"),
            ]
        }


class GetNextJobsToRunBottom(GetNextJobsCounter):
    """A job at the bottom of the diamond"""
    unexpanded_id = "get_next_jobs_to_run_bottom"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "get_next_jobs_to_run_bottom_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "get_next_jobs_to_run_middle_01_target"),
                builder.expanders.Expander(
                    builder.targets.Target,
                    "get_next_jobs_to_run_middle_02_target"),
            ]
        }


class GetNextJobsToRunMiddle02(GetNextJobsCounter):
    """A job in the middle of the diamond"""
    unexpanded_id = "get_next_jobs_to_run_middle_02"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "get_next_jobs_to_run_middle_02_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "get_next_jobs_to_run_top_target"),
            ]
        }


class GetNextJobsToRunMiddle01(GetNextJobsCounter):
    """A job in the middle of the diamond"""
    unexpanded_id = "get_next_jobs_to_run_middle_01"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "get_next_jobs_to_run_middle_01_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "get_next_jobs_to_run_top_target"),
            ]
        }


class GetNextJobsToRunTop(GetNextJobsCounter):
    """Top job of the graph"""
    unexpanded_id = "get_next_jobs_to_run_top"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "get_next_jobs_to_run_top_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "get_next_jobs_to_run_highest_target"),
            ]
        }


class GetShouldRunCounterState(builder.jobs.JobState):
    """Used to count how many times the should run is returned"""

    def __init__(self, unexpanded_id, unique_id, build_context, command,
            cache_time, config=None):
        super(GetShouldRunCounterState, self).__init__(
                unexpanded_id, unique_id, build_context, command, cache_time,
                config=config)
        self.count = 0

    def get_should_run(self, build_graph, cached=True, cache_set=None):
        """Counts the number of times it is called"""
        self.count += 1
        return super(GetShouldRunCounterState, self).get_should_run(
            build_graph, cached=cached, cache_set=cache_set)

class GetShouldRunCounterJob(Job):
    """Expands out the jobsattes into the GetShouldRunCounterState"""
    def expand(self, build_context):
        counting_nodes = []
        expanded_nodes = super(GetShouldRunCounterJob, self).expand(
                build_context)
        for expanded_node in expanded_nodes:
            counting_node = GetShouldRunCounterState(
                    expanded_node.unexpanded_id,
                    expanded_node.unique_id,
                    expanded_node.build_context,
                    expanded_node.command,
                    expanded_node.cache_time,
                    expanded_node.config)
            counting_nodes.append(counting_node)
        return counting_nodes


class UpdateLowerNodesShouldRunLowest(GetShouldRunCounterJob):
    """A job at the bottom of the graph"""
    unexpanded_id = "update_lower_nodes_should_run_lowest"
    cache_time = "10min"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "update_lower_nodes_should_run_lowest_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "update_lower_nodes_should_run_bottom_target"),
            ]
        }


class UpdateLowerNodesShouldRunBottom(GetShouldRunCounterJob):
    """A job at the bottom of the diamond"""
    unexpanded_id = "update_lower_nodes_should_run_bottom"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "update_lower_nodes_should_run_bottom_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "update_lower_nodes_should_run_middle"
                    "_01_target"),
                builder.expanders.Expander(
                    builder.targets.Target,
                    "update_lower_nodes_should_run_middle"
                    "_02_target")
            ]
        }


class UpdateLowerNodesShouldRunMiddle02(GetShouldRunCounterJob):
    """A job in the middle of the diamond"""
    unexpanded_id = "update_lower_nodes_should_run_middle_02"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "update_lower_nodes_should_run_middle"
                    "_02_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "update_lower_nodes_should_run_top_target")
            ]
        }



class UpdateLowerNodesShouldRunMiddle01(GetShouldRunCounterJob):
    """A job in the middle of the diamond"""
    unexpanded_id = "update_lower_nodes_should_run_middle_01"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "update_lower_nodes_should_run_"
                    "middle_01_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "update_lower_nodes_should_run_top_target")
            ]
        }


class UpdateLowerNodesShouldRunTop(GetShouldRunCounterJob):
    """Top most job"""
    unexpanded_id = "update_lower_nodes_should_run_top"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "update_lower_nodes_should_run_top_target")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "update_lower_nodes_should_run_highest_target")
            ]
        }


class GetStartingJobs04Tester(Job):
    """A job that will have it's should run values overwritten"""
    unexpanded_id = "get_starting_jobs_04"


class GetStartingJobs03Tester(Job):
    """A job that will have it's should run values overwritten"""
    unexpanded_id = "get_starting_jobs_03"


class GetStartingJobs02Tester(Job):
    """A job that will have it's should run values overwritten"""
    unexpanded_id = "get_starting_jobs_02"


class GetStartingJobs01Tester(Job):
    """A job that will have it's should run values overwritten"""
    unexpanded_id = "get_starting_jobs_01"


class ShouldRunRecurseJobState(builder.jobs.JobState):
    """Used to count how many times the should run is returned"""

    def __init__(self, unexpanded_id, unique_id, build_context, command,
            cache_time, should_run_immediate, config=None):
        super(ShouldRunRecurseJobState, self).__init__(
                unexpanded_id, unique_id, build_context, command, cache_time,
                config=config)
        self.should_run_immediate = should_run_immediate

    def get_should_run_immediate(self, build_graph, cached=True, cache_set=None):
        """Counts the number of times it is called"""
        return self.should_run_immediate


class ShouldRunRecurseJob(Job):
    def expand(self, build_context):
        counting_nodes = []
        expanded_nodes = super(ShouldRunRecurseJob, self).expand(
                build_context)
        for expanded_node in expanded_nodes:
            counting_node = ShouldRunRecurseJobState(
                    expanded_node.unexpanded_id,
                    expanded_node.unique_id,
                    expanded_node.build_context,
                    expanded_node.command,
                    expanded_node.cache_time,
                    self.should_run_immediate,
                    expanded_node.config,)
            counting_nodes.append(counting_node)
        return counting_nodes


class ShouldRunRecurseJob10Tester(ShouldRunRecurseJob):
    """Second top most job should not run"""
    unexpanded_id = "should_run_recurse_job_10"
    should_run_immediate = True

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_09",
                    "5min"),
            ]
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_10",
                    "5min")
            ]
        }


class ShouldRunRecurseJob09Tester(ShouldRunRecurseJob):
    """Second top most job should not run"""
    unexpanded_id = "should_run_recurse_job_09"
    should_run_immediate = False

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_08",
                    "5min"),
            ]
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_09",
                    "5min"),
            ]
        }


class ShouldRunRecurseJob08Tester(ShouldRunRecurseJob):
    """Second top most job should not run"""
    unexpanded_id = "should_run_recurse_job_08"
    cache_time = "10min"
    should_run_immediate = True

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_07",
                    "5min"),
            ]
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_08",
                    "5min"),
            ]
        }


class ShouldRunRecurseJob07Tester(ShouldRunRecurseJob):
    """Second top most job should not run"""
    unexpanded_id = "should_run_recurse_job_07"
    cache_time = "10min"
    should_run_immediate = False

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_06",
                    "5min"),
            ]
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_07",
                    "5min"),
            ]
        }


class ShouldRunRecurseJob06Tester(ShouldRunRecurseJob):
    """Second top most job should not run"""
    unexpanded_id = "should_run_recurse_job_06"
    cache_time = "10min"
    should_run_immediate = True

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_05",
                    "5min"),
            ]
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_06",
                    "5min"),
            ]
        }


class ShouldRunRecurseJob05Tester(ShouldRunRecurseJob):
    """Second top most job should not run"""
    unexpanded_id = "should_run_recurse_job_05"
    cache_time = "10min"
    should_run_immediate = False

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_04",
                    "5min"),
            ]
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_05",
                    "5min"),
            ]
        }


class ShouldRunRecurseJob04Tester(ShouldRunRecurseJob):
    """Second top most job should not run"""
    unexpanded_id = "should_run_recurse_job_04"
    should_run_immediate = True

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_03",
                    "5min"),
            ]
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_04",
                    "5min"),
            ]
        }


class ShouldRunRecurseJob03Tester(ShouldRunRecurseJob):
    """Second top most job should not run"""
    unexpanded_id = "should_run_recurse_job_03"
    should_run_immediate = False

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_02",
                    "5min"),
            ]
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_03",
                    "5min"),
            ]
        }


class ShouldRunRecurseJob02Tester(ShouldRunRecurseJob):
    """Second top most job should not run"""
    unexpanded_id = "should_run_recurse_job_02"
    should_run_immediate = True

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_01",
                    "5min"),
            ]
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_02",
                    "5min"),
            ]
        }


class ShouldRunRecurseJob01Tester(ShouldRunRecurseJob):
    """Top most job should not run"""
    unexpanded_id = "should_run_recurse_job_01"
    should_run_immediate = False

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_00",
                    "5min"),
            ]
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "should_run_recurse_target_01",
                    "5min"),
            ]
        }


class ShouldRunCacheLogicJobTester(Job):
    """Cached job"""
    unexpanded_id = "should_run_cache_logic"
    cache_time = "5min"


class ShouldRunLogicJobTester(Job):
    """Non cache Job"""
    unexpanded_id = "should_run_logic"


class PastCurfewTimestampJobTester(TimestampExpandedJob):
    """Timestamp job, returns True if curfew + endtime is past"""
    unexpanded_id = "past_curfew_timestamp_job"


class PastCurfewJobTester(Job):
    """Standard job, returns True"""
    unexpanded_id = "past_curfew_job"


class AllDependenciesJobTester(TimestampExpandedJob):
    """Only job"""
    file_step = "15min"
    unexpanded_id = "all_dependencies_job"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "all_dependencies_target_01-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "all_dependencies_target_02-%Y-%m-%d-%H-%M",
                    "5min"),
            ]
        }

    def get_targets(self, build_context=None):
        return {}


class PastCacheTimeJobTester(TimestampExpandedJob):
    """Job for testing cache time"""
    file_step = "15min"
    unexpanded_id = "past_cache_time_job"
    cache_time = "5min"

    def get_dependencies(self, build_context=None):
        return {}

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "past_cache_time_target-%Y-%m-%d-%H-%M",
                    "5min")
            ]
        }


class BuildableJobTester(TimestampExpandedJob):
    """Has multiple kinds of dependencies that will be tested"""
    file_step = "15min"
    unexpanded_id = "buildable_job"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "buildable_15_minute_target_01-%Y-%m-%d-%H-%M",
                    "15min"),
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "buildable_5_minute_target_01-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "buildable_15_minute_target_02-%Y-%m-%d-%H-%M",
                    "15min"),
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "buildable_5_minute_target_02-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {}


class StaleAlternateUpdateBottomJobTester(TimestampExpandedJob):
    """Outputs a target and that target has an alternate_update"""
    file_step = "15min"
    unexpanded_id = "stale_alternate_update_bottom_job"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_alternate_update_top_target-%Y-%m-%d-%H-%M",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_alternate_update_secondary_target"
                    "-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_alternate_update_bottom_target-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
        }


class StaleAlternateUpdateTopJobTester(TimestampExpandedJob):
    """Outputs a target and that target has an alternate_update"""
    file_step = "15min"
    unexpanded_id = "stale_alternate_update_top_job"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_alternate_update_highest_target-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_alternate_update_top_target-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
            "alternates": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_alternate_update_bottom_target-%Y-%m-%d-%H-%M",
                    "5min"),
            ]
        }


class StaleAlternateBottomJobTester(
    TimestampExpandedJob):
    """Outputs a target and that target has an alternate"""
    file_step = "15min"
    unexpanded_id = "stale_alternate_bottom_job"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_alternate_top_target-%Y-%m-%d-%H-%M",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_alternate_secondary_target-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_alternate_bottom_target-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
        }


class StaleAlternateTopJobTester(TimestampExpandedJob):
    """Outputs a target and that target has an alternate"""
    file_step = "15min"
    unexpanded_id = "stale_alternate_top_job"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_alternate_highest_target-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_alternate_top_target-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
            "alternates": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_alternate_bottom_target-%Y-%m-%d-%H-%M",
                    "5min"),
            ]
        }


class StaleIgnoreMtimeJobTester(TimestampExpandedJob):
    """Depends on a target that has an ignoredmtime"""
    file_step = "15min"
    unexpanded_id = "stale_ignore_mtime_job"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_ignore_mtime_input_target_01-%Y-%m-%d-%H-%M",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_ignore_mtime_input_target_02-%Y-%m-%d-%H-%M",
                    "5min", ignore_mtime=True),

            ]
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_ignore_mtime_output_target-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
        }


class StaleStandardJobTester(TimestampExpandedJob):
    """A target with dependencies and targets"""
    file_step = "15min"
    unexpanded_id = "stale_standard_job"
    cache_time = "5min"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_top_target-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "stale_standard_target-%Y-%m-%d-%H-%M",
                    "5min"),
            ]
        }


class DiamondRedundancyHighestJobTester(TimestampExpandedJob):
    """Highest job"""
    unexpanded_id = "diamond_redundant_highest_job"
    count = 0

    def get_targets(self, build_context=None):
        return {
            "produces": [
                (builder.expanders
                 .DiamondRedundancyHighestTargetCountingTimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "diamond_redundancy_highest_target",
                    "5min")),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                (builder.expanders
                 .DiamondRedundancySuperTargetCountingTimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "diamond_redundancy_super_target",
                    "5min")),
            ]
        }

    def expand(self, build_context):
        self.__class__.count += 1
        return super(DiamondRedundancyHighestJobTester, self).expand(
            build_context)


class DiamondRedundancyTopJobTester(TimestampExpandedJob):
    """Top job"""
    unexpanded_id = "diamond_redundant_top_job"
    count = 0

    def get_targets(self, build_context=None):
        return {
            "produces": [
                (builder.expanders
                 .DiamondRedundancyTopTargetCountingTimestampExpander(
                     builder.targets.LocalFileSystemTarget,
                     "diamond_redundancy_top_target",
                     "5min")),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                (builder.expanders
                 .DiamondRedundancyHighestTargetCountingTimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "diamond_redundancy_highest_target",
                    "5min")),
            ]
        }

    def expand(self, build_context):
        self.__class__.count += 1
        return super(DiamondRedundancyTopJobTester, self).expand(build_context)


class DiamondRedundancyMiddleJob02Tester(TimestampExpandedJob):
    """Middle job"""
    unexpanded_id = "diamond_redundant_middle_job_02"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "diamond_redundancy_middle_target_02",
                    "5min"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                (builder.expanders
                 .DiamondRedundancyTopTargetCountingTimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "diamond_redundancy_top_target",
                    "5min")),
            ]
        }


class DiamondRedundancyMiddleJob01Tester(TimestampExpandedJob):
    """Middle job"""
    unexpanded_id = "diamond_redundant_middle_job_01"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "diamond_redundancy_middle_target_01",
                    "5min"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                (builder.expanders
                 .DiamondRedundancyTopTargetCountingTimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "diamond_redundancy_top_target",
                    "5min")),
            ]
        }


class DiamondRedundancyBottomJobTester(Job):
    """Bottom job"""
    unexpanded_id = "diamond_redundant_bottom_job"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "diamond_redundancy_bottom_target",
                    "5min"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "diamond_redundancy_middle_target_01",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "diamond_redundancy_middle_target_02",
                    "5min"),
            ]
        }


class BackboneDependantTopJob02Tester(Job):
    """Top job"""
    unexpanded_id = "backbone_dependant_top_job_02"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "backbone_dependant_top_target_02",
                    "5min"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "backbone_dependant_highest_target_02",
                    "5min"),
            ]
        }


class BackboneDependantTopJob01Tester(Job):
    """Top job"""
    unexpanded_id = "backbone_dependant_top_job_01"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "backbone_dependant_top_target_01",
                    "5min"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "backbone_dependant_highest_target_01",
                    "5min"),
            ]
        }


class BackboneDependantMiddleJob02Tester(Job):
    """Middle job"""
    unexpanded_id = "backbone_dependant_middle_job_02"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "backbone_dependant_middle_target_02",
                    "5min"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "backbone_dependant_top_target_02",
                    "5min"),
            ]
        }


class BackboneDependantMiddleJob01Tester(Job):
    """Middle job"""
    unexpanded_id = "backbone_dependant_middle_job_01"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "backbone_dependant_middle_target_01",
                    "5min"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "backbone_dependant_top_target_01",
                    "5min"),
            ]
        }


class BackboneDependantBottomJobTester(Job):
    """Bottom job"""
    unexpanded_id = "backbone_dependant_bottom_job"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "backbone_dependant_bottom_target",
                    "5min"),
            ]
        }

    def get_dependencies(self, build_context=None):
        depends_dict = {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "backbone_dependant_middle_target_02",
                    "5min"),
            ]
        }
        if self.config.get("has_backbone", False):
            depends_dict["depends"].append(
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "backbone_dependant_middle_target_01",
                    "5min"),
            )
        return depends_dict


class BuildGraphConstructionJobBottom01Tester(TimestampExpandedJob):
    """bottom job"""
    file_step = "1h"
    unexpanded_id = "build_graph_construction_job_bottom_01"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "build_graph_construction_target_bottom_01"
                    "-%Y-%m-%d-%H-%M",
                    "5min"),
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "build_graph_construction_target_top_02-%Y-%m-%d-%H-%M",
                    "1min"),
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "build_graph_construction_target_top_03-%Y-%m-%d-%H-%M",
                    "5min", past=3),
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "build_graph_construction_target_top_04-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
        }


class BuildGraphConstructionJobTop02Tester(
    TimestampExpandedJob):
    """Top job"""
    unexpanded_id = "build_graph_construction_job_top_02"
    file_step = "5min"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "build_graph_construction_target_top_02-%Y-%m-%d-%H-%M",
                    "1min"),
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "build_graph_construction_target_top_03-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
            "alternates": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "build_graph_construction_target_top_04-%Y-%m-%d-%H-%M",
                    "5min"),
            ],
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "build_graph_construction_target_highest_02"
                    "-%Y-%m-%d-%H-%M",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "build_graph_construction_target_highest_03"
                    "-%Y-%m-%d-%H-%M",
                    "1min"),
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "build_graph_construction_target_highest_04"
                    "-%Y-%m-%d-%H-%M",
                    "1min"),
            ],
        }


class BuildGraphConstructionJobTop01Tester(TimestampExpandedJob):
    """Top job"""
    unexpanded_id = "build_graph_construction_job_top_01"
    file_step = "5min"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "build_graph_construction_target_top_01-%Y-%m-%d-%H-%M",
                    "5min")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "build_graph_construction_target_highest_01"
                    "-%Y-%m-%d-%H-%M",
                    "1min")
            ],
        }


class RuleDepConstructionJobBottom01Tester(Job):
    """bottom job"""
    unexpanded_id = "rule_dep_construction_job_bottom_01"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "rule_dep_construction_target_bottom_01",
                    "5min")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "rule_dep_construction_target_top_02",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "rule_dep_construction_target_top_03",
                    "5min",
                    past=3),
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "rule_dep_construction_target_top_04",
                    "5min"),
            ],
        }


class RuleDepConstructionJobTop02Tester(Job):
    """Top job"""
    unexpanded_id = "rule_dep_construction_job_top_02"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "rule_dep_construction_target_top_02",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "rule_dep_construction_target_top_03",
                    "5min"),
            ],
            "alternates": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "rule_dep_construction_target_top_04",
                    "5min"),
            ],
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "rule_dep_construction_target_highest_02",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "rule_dep_construction_target_highest_03",
                    "1min"),
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "rule_dep_construction_target_highest_04",
                    "5min"),
            ],
        }


class RuleDepConstructionJobTop01Tester(Job):
    """Top job"""
    unexpanded_id = "rule_dep_construction_job_top_01"

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "rule_dep_construction_top_01",
                    "5min")
            ]
        }

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "rule_dep_construction_target_highest_01",
                    "5min")
            ],
        }


class StandardDependsOneOrMoreTargetTester(Job):
    """Used to test a job in the depends section"""
    unexpanded_id = "standard_depends_target"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.targets.Target
            ],
            "depends_one_or_more": [
                builder.targets.Target,
                builder.targets.Target
            ],
        }


class StandardDependsTargetTester(Job):
    """Used to test a job in the depends section"""
    unexpanded_id = "standard_depends_target"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "standard_target")
            ]
        }


class TimestampExpandedJobTester(TimestampExpandedJob):
    """Used to test that jobs are expanded correctly and their commands
    are retrieved correctly
    """
    unexpanded_id = "timestamp_expanded_job"

    def get_command(self):
        command = "timestamp expanded job tester command %Y-%m-%d-%H-%M"
        return command


class JobBackboneDependantDependsOneOrMore(Job):
    """A job that has a depends_one_or_more that is inserted if backbone
    is true
    """
    unexpanded_id = "job_backbone_dependant_depends_one_or_more"

    def get_dependencies(self, build_context=None):
        depends_dict = {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.LocalFileSystemTarget,
                    "standard_depends_target")
            ],
            "depends_one_or_more": []
        }

        if self.config.get("has_backbone", False):
            depends_dict["depends_one_or_more"].append(
                builder.expanders.Expander(
                    builder.targets.Target,
                    "backbone_dependant_depends_one_or_more"))

        if not depends_dict["depends_one_or_more"]:
            del depends_dict["depends_one_or_more"]

        return depends_dict

class RangeJob(TimestampExpandedJob):
    """Used to test that the range value is followed"""
    unexpanded_id = "range_job"
    file_step = "5min"
