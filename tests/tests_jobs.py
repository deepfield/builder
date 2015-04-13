"""Used to test the job api functions"""
import unittest

import arrow
import networkx
import numbers

import builder
from builder.jobs import JobDefinition, TimestampExpandedJobDefinition, Job
import testing
import builder.jobs
import builder.targets
import builder.expanders


class FakeTarget(builder.targets.Target):

    @staticmethod
    def get_bulk_exists_mtime(targets):
        return {target.get_id(): {"exists": target.mtime is not None, "mtime": target.mtime} for target in targets}

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


class SimpleTestJobDefinition(SimpleJobTestMixin, JobDefinition):
    """A simple API for creating a job through constructor args"""
    def __init__(self, unexpanded_id=None, targets=None, depends=None,
            config=None, should_run=False, parents_should_run=False,
            target_type=None, expander_type=None,
            depends_dict=None, targets_dict=None, **kwargs):
        super(SimpleTestJobDefinition, self).__init__(unexpanded_id, config=config, **kwargs)
        self.targets = targets

        self.should_run = should_run
        self.parents_should_run = parents_should_run
        self.target_type = target_type or FakeTarget
        self.expander_type = expander_type or builder.expanders.Expander

        self.setup_dependencies_and_targets(depends_dict, targets_dict, depends, targets)

class SimpleTimestampExpandedTestJob(SimpleJobTestMixin, TimestampExpandedJobDefinition):
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
        self.expander_type = expander_type or builder.expanders.TimestampExpander

        self.setup_dependencies_and_targets(depends_dict, targets_dict, depends, targets)


class EffectJobDefinition(SimpleTestJobDefinition):
    def __init__(self, unexpanded_id=None, effect=None, **kwargs):

        if effect is None:
            effect = [1]
        if not isinstance(effect, list):
            effect = [effect]
        self.effect = effect
        super(EffectJobDefinition, self).__init__(unexpanded_id=unexpanded_id,
                target_type=builder.targets.Target, **kwargs)

    def get_effect(self):
        return self.effect

    def construct_job(self, expanded_id, build_graph, build_context):
        return EffectJob(job=self, unique_id=expanded_id, build_graph=build_graph, build_context=build_context,
            effect=self.effect)


class EffectTimestampExpandedJobDefinition(SimpleTimestampExpandedTestJob):
    def __init__(self, unexpanded_id=None, effect=None, **kwargs):

        if effect is None:
            effect = [1]
        if not isinstance(effect, list):
            effect = [effect]
        self.effect = effect
        super(EffectTimestampExpandedJobDefinition, self).__init__(unexpanded_id=unexpanded_id,
                target_type=builder.targets.Target, **kwargs)

    def get_effect(self):
        return self.effect

    def construct_job(self, expanded_id, build_graph, build_context):
        return EffectJob(job=self, unique_id=expanded_id, build_graph=build_graph, build_context=build_context,
            effect=self.effect)

class ShouldRunRecurseJob(builder.jobs.Job):
    """Used to count how many times the should run is returned"""

    def __init__(self, job, unique_id, build_graph, build_context,
            should_run_immediate):
        super(ShouldRunRecurseJob, self).__init__(job,
                unique_id, build_graph, build_context)
        self.should_run_immediate = should_run_immediate

    def get_should_run_immediate(self):
        return self.should_run_immediate


class ShouldRunRecurseJobDefinition(SimpleTestJobDefinition):
    def expand(self, build_graph, build_context):
        counting_nodes = []
        expanded_nodes = super(ShouldRunRecurseJobDefinition, self).expand(build_graph,
                build_context)
        for expanded_node in expanded_nodes:
            counting_node = ShouldRunRecurseJob(
                    expanded_node,
                    expanded_node.unique_id,
                    build_graph,
                    expanded_node.build_context,
                    self.should_run_immediate)
            counting_nodes.append(counting_node)
        return counting_nodes




class EffectJob(Job):
    def __init__(self, effect=None, *args, **kwargs):
        super(EffectJob, self).__init__(*args, **kwargs)
        self.effect = effect
        self.count = 0

    def get_effect(self):
        self.count = self.count + 1
        min_count = min(self.count, len(self.effect))
        return self.effect[min_count - 1]
