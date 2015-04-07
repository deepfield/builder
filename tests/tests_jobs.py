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


class FakeTarget(builder.targets.Target):

    @staticmethod
    def get_bulk_exists_mtime(targets):
        return {target.get_id(): {"exists": target.exists, "mtime": target.mtime} for target in targets}

class SimpleJobTestMixin(object):

    def setup_dependencies_and_targets(self, depends_dict, targets_dict, depends, targets):
        # Set up dependency dictionary
        depends_dict = depends_dict or {}
        depends_dict.setdefault('depends', [])
        depends_dict.setdefault('depends_one_or_more', [])
        if depends:
            for depend in depends:
                if isinstance(depend, dict):
                    depends_type = depend.pop('type', 'depends')
                    depends_dict[depends_type].append(
                        self.expander_type(
                            self.target_type,
                        **depend)
                    )
                elif isinstance(depend, basestring):
                    depends_dict['depends'].append(
                        self.expander_type(
                            self.target_type,
                        depend)
                    )
        self.dependencies = depends_dict

        # Set up target dictionary
        targets_dict = targets_dict or {}
        targets_dict.setdefault("produces", [])
        targets_dict.setdefault("alternates", [])
        if targets:
            for target in targets:
                if isinstance(target, dict):
                    target_type = target.pop('type', 'produces')
                    targets_dict[target_type].append(
                        self.expander_type(
                            self.target_type,
                            **target)
                     )
                elif isinstance(target, basestring):

                    targets_dict["produces"].append(
                        self.expander_type(
                            self.target_type,
                            target)
                     )
        self.targets = targets_dict


class SimpleTestJob(SimpleJobTestMixin, Job):
    """A simple API for creating a job through constructor args"""
    def __init__(self, unexpanded_id=None, targets=None, depends=None,
            config=None, should_run=False, parents_should_run=False,
            target_type=None, expander_type=None,
            depends_dict=None, targets_dict=None, **kwargs):
        super(SimpleTestJob, self).__init__(unexpanded_id, config=config, **kwargs)
        self.targets = targets

        self.should_run = should_run
        self.parents_should_run = parents_should_run
        self.target_type = target_type or FakeTarget
        self.expander_type = expander_type or builder.expanders.Expander

        self.setup_dependencies_and_targets(depends_dict, targets_dict, depends, targets)

class SimpleTimestampExpandedTestJob(SimpleJobTestMixin, TimestampExpandedJob):
    """A simple API for creating a job through constructor args"""
    def __init__(self, unexpanded_id=None, targets=None, depends=None,
            should_run=False, parents_should_run=False,
            target_type=None, expander_type=None,
            depends_dict=None, targets_dict=None, **kwargs):
        super(SimpleTimestampExpandedTestJob, self).__init__(unexpanded_id, **kwargs)
        self.targets = targets

        self.should_run = should_run
        self.parents_should_run = parents_should_run
        self.target_type = target_type or FakeTarget
        self.expander_type = expander_type or builder.expanders.Expander

        self.setup_dependencies_and_targets(depends_dict, targets_dict, depends, targets)


class GetNextJobsCounter(Job):
    """Used to count how many times the next job was looked for"""

    def __init__(self, unexpanded_id="get_next_jobs_counter", config=None):
        super(GetNextJobsCounter, self).__init__(unexpanded_id=unexpanded_id,
                                                 config=config)
        self.count = 0

    def get_next_jobs_to_run(self, job_id, update_set=None):
        """Counts the number of times it is called"""
        self.count += 1
        return super(GetNextJobsCounter, self).get_next_jobs_to_run(
            job_id, update_set=update_set)


class GetNextJobsToRunLowest(GetNextJobsCounter):
    """A job at the bottom of the graph"""
    def __init__(self, unexpanded_id="get_next_jobs_to_run_lowest"):
        super(GetNextJobsToRunLowest, self).__init__(
                unexpanded_id=unexpanded_id)

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
    def __init__(self, unexpanded_id="get_next_jobs_to_run_bottom"):
        super(GetNextJobsToRunBottom, self).__init__(
                unexpanded_id=unexpanded_id)

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
    def __init__(self, unexpanded_id="get_next_jobs_to_run_middle_02"):
        super(GetNextJobsToRunMiddle02, self).__init__(
                unexpanded_id=unexpanded_id)

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
    def __init__(self, unexpanded_id="get_next_jobs_to_run_middle_01"):
        super(GetNextJobsToRunMiddle01, self).__init__(
                unexpanded_id=unexpanded_id)

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
    def __init__(self, unexpanded_id="get_next_jobs_to_run_top"):
        super(GetNextJobsToRunTop, self).__init__(
                unexpanded_id=unexpanded_id)

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


class ShouldRunRecurseJobState(builder.jobs.JobState):
    """Used to count how many times the should run is returned"""

    def __init__(self, job, unique_id, build_context, cache_time,
            should_run_immediate, config=None):
        super(ShouldRunRecurseJobState, self).__init__(job,
                unique_id, build_context, cache_time,
                config=config)
        self.should_run_immediate = should_run_immediate

    def get_should_run_immediate(self, build_graph, cached=True, cache_set=None):
        return self.should_run_immediate


class ShouldRunRecurseJob(SimpleTestJob):
    def expand(self, build_context):
        print self.should_run_immediate, self.unexpanded_id
        counting_nodes = []
        expanded_nodes = super(ShouldRunRecurseJob, self).expand(
                build_context)
        for expanded_node in expanded_nodes:
            counting_node = ShouldRunRecurseJobState(
                    expanded_node,
                    expanded_node.unique_id,
                    expanded_node.build_context,
                    expanded_node.cache_time,
                    self.should_run_immediate,
                    expanded_node.config,)
            counting_nodes.append(counting_node)
        return counting_nodes


class ShouldRunCacheLogicJobTester(Job):
    """Cached job"""
    def __init__(self, unexpanded_id="should_run_cache_logic",
                 cache_time="5min", targets=None, dependencies=None,
                 config=None):
        super(ShouldRunCacheLogicJobTester, self).__init__(
                unexpanded_id=unexpanded_id, cache_time=cache_time)


class ShouldRunLogicJobTester(Job):
    """Non cache Job"""
    def __init__(self, unexpanded_id="should_run_logic", config=None):
        super(ShouldRunLogicJobTester, self).__init__(
                unexpanded_id=unexpanded_id)


class PastCurfewTimestampJobTester(TimestampExpandedJob):
    """Timestamp job, returns True if curfew + endtime is past"""
    def __init__(self, unexpanded_id="past_curfew_timestamp_job", config=None):
        super(PastCurfewTimestampJobTester, self).__init__(
                unexpanded_id=unexpanded_id)


class PastCurfewJobTester(Job):
    """Standard job, returns True"""
    def __init__(self, unexpanded_id="past_curfew_job", config=None):
        super(PastCurfewJobTester, self).__init__(
                unexpanded_id=unexpanded_id)


class AllDependenciesJobTester(TimestampExpandedJob):
    """Only job"""
    def __init__(self, unexpanded_id="all_dependencies_job", file_step="15min",
                 config=None):
        super(AllDependenciesJobTester, self).__init__(
                unexpanded_id=unexpanded_id, file_step=file_step)

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
    def __init__(self, unexpanded_id="past_cache_time_job", file_step="15min",
                 cache_time="5min", config=None):
        super(PastCacheTimeJobTester, self).__init__(
                unexpanded_id=unexpanded_id, file_step=file_step,
                cache_time=cache_time)

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
    def __init__(self, unexpanded_id="buildable_job", file_step="15min",
                 config=None):
        super(BuildableJobTester, self).__init__(
                unexpanded_id=unexpanded_id, file_step=file_step)

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
    def __init__(self, unexpanded_id="stale_alternate_update_bottom_job",
                 file_step="15min", config=None):
        super(StaleAlternateUpdateBottomJobTester, self).__init__(
                unexpanded_id=unexpanded_id, file_step=file_step)

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
    def __init__(self, unexpanded_id="stale_alternate_update_top_job",
                 file_step="15min", config=None):
        super(StaleAlternateUpdateTopJobTester, self).__init__(
                unexpanded_id=unexpanded_id, file_step=file_step)

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
    def __init__(self, unexpanded_id="stale_alternate_bottom_job",
                 file_step="15min", config=None):
        super(StaleAlternateBottomJobTester, self).__init__(
                unexpanded_id=unexpanded_id, file_step=file_step)

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
    def __init__(self, unexpanded_id="stale_alternate_top_job",
                 file_step="15min", config=None):
        super(StaleAlternateTopJobTester, self).__init__(
                unexpanded_id=unexpanded_id, file_step=file_step)

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


class DiamondRedundancyHighestJobTester(TimestampExpandedJob):
    """Highest job"""
    count = 0
    def __init__(self, unexpanded_id="diamond_redundant_highest_job",
                 config=None):
        super(DiamondRedundancyHighestJobTester, self).__init__(
                unexpanded_id=unexpanded_id)

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
    count = 0
    def __init__(self, unexpanded_id="diamond_redundant_top_job",
                 config=None):
        super(DiamondRedundancyTopJobTester, self).__init__(
                unexpanded_id=unexpanded_id)

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
    def __init__(self, unexpanded_id="diamond_redundant_middle_job_02",
                 config=None):
        super(DiamondRedundancyMiddleJob02Tester, self).__init__(
                unexpanded_id=unexpanded_id)

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
    def __init__(self, unexpanded_id="diamond_redundant_middle_job_01",
                 config=None):
        super(DiamondRedundancyMiddleJob01Tester, self).__init__(
                unexpanded_id=unexpanded_id)

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
    def __init__(self, unexpanded_id="diamond_redundant_bottom_job",
                 config=None):
        super(DiamondRedundancyBottomJobTester, self).__init__(
                unexpanded_id=unexpanded_id)

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


class StandardDependsOneOrMoreTargetTester(Job):
    """Used to test a job in the depends section"""
    unexpanded_id = "standard_depends_target"
    def __init__(self, unexpanded_id="standard_depends_target",
                 config=None):
        super(StandardDependsOneOrMoreTargetTester, self).__init__(
                unexpanded_id=unexpanded_id)

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
    def __init__(self, unexpanded_id="standard_depends_target",
                 config=None):
        super(StandardDependsTargetTester, self).__init__(
                unexpanded_id=unexpanded_id)

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.targets.Target,
                    "standard_target")
            ]
        }


class ExpandCounter(Job):
    def __init__(self, unexpanded_id=None, cache_time=None, targets=None,
                 dependencies=None, config=None):
        self.count = 0
        super(ExpandCounter, self).__init__(unexpanded_id=unexpanded_id,
                                            cache_time=cache_time,
                                            targets=targets,
                                            dependencies=dependencies,
                                            config=config)

    def expand(self, build_context):
        self.count = self.count + 1
        return super(ExpandCounter, self).expand(build_context)
