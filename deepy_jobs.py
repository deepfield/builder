import collections
import copy
import datetime
import inspect
import string
import sys
import json

import dateutil

import builder.deepy_targets
from builder.deepy_util import basic_command_substitution, deepy_command_substitution
import builder.jobs
import deepy.cfg
import deepy.log


class DeepyJobState(builder.jobs.JobState):
    """A wrapper for the job state, might never be used"""


class DeepyTimestampExpandedJobState(
        builder.jobs.TimestampExpandedJobState, DeepyJobState):
    """A wrapper that give job states the ability to do a command
    replacement
    """
    def __init__(self, unexpanded_id, unique_id, build_context, command,
            cache_time, curfew, rule, config=None):
        super(DeepyTimestampExpandedJobState, self).__init__(unexpanded_id,
                unique_id, build_context, command, cache_time, curfew,
                config=config)
        if not config:
            self.config = deepy.cfg
        self.rule = rule
        self.dimensions = rule.get('dimensions') or []

    def _replace_command(self, command, build_graph):
        """Used to replace all of the formatting on the string used for
        recipes
        """
        got_format_args = False

        if "$@" in command:
            targets = build_graph.neighbors(self.unique_id)
            targets = " ".join(targets)
            command = command.replace("$@", targets)

        dependencies = []
        for depends_node in build_graph.predecessors(self.unique_id):
            for dependency_id in build_graph.predecessors(depends_node):
                dependency_id = dependencies.append(dependency_id)

        while True:
            new_command = basic_command_substitution(
                command, self.build_context["start_time"])
            new_command = deepy_command_substitution(new_command, config=deepy.cfg)

            try:
                new_command = new_command.format()
                command = new_command
                break
            except:
                if got_format_args == False:
                    format_args, new_command = self._get_format_args(
                            dependencies, new_command, build_graph)
                    got_format_args = True
                new_command = new_command.format(**format_args)
                command = new_command

        if "$^" in command:
            prerequisites_string = " ".join(dependencies)
            command = command.replace("$^", prerequisites_string)
        if "$D" in command:
            dimensions = self._get_list_merge("dimensions")
            dimensions = ",".join(dimensions)
            command = command.replace("$D", dimensions)
        if "$A" in command:
            user_args = self.build_context.get('user_args') or []
            user_args = " ".join(user_args)
            command = command.replace("$A", user_args)

        command = str(command)
        return command

    def _get_list_merge(self, key):
        """Merges in the extend and remove dictionarys"""
        val = self.rule.get(key, [])
        # Copy the list so we don't modify the original.
        val = val[:]
        ext = self.rule.get(key + '.extend')
        rem = self.rule.get(key + '.remove')
        if ext is not None:
            # preserve order and make unique
            val += [x for x in ext if x not in val]
        if rem is not None:
            val = [x for x in val if x not in rem]
        return val

    def _get_format_args(self, dependencies, command, build_graph):
        """Returns a dict of the form
        {"class_id": [dependencies, ...], ...}
        """
        string_formatter = string.Formatter()
        object_dict = {}
        dependency_dict = collections.defaultdict(list)
        for item in string_formatter.parse(command):
            job_id = item[1]
            actual_object = build_graph.get_job(job_id)
            unexpanded_id = getattr(actual_object, "unexpanded_id")
            replace_string = "{" + job_id + "}"
            replacement_string = "{" + unexpanded_id + "}"
            command = command.replace(replace_string, replacement_string)
            targets = actual_object.get_targets()
            for target_set in targets["produces"]:
                for expanded_target in target_set.expand(self.build_context):
                    target_unexpanded_id = expanded_target.unexpanded_id
                    object_dict[target_unexpanded_id] = unexpanded_id
                    dependency_dict[unexpanded_id] = []

        for dependency_id in dependencies:
            dependency = build_graph.node[dependency_id]["object"]
            if not dependency.get_exists():
                continue
            dependency_dict[object_dict[dependency.unexpanded_id]].append(
                dependency_id)
        for dependency_id, dependency_list in dependency_dict.iteritems():
            dependency_dict[dependency_id] = " ".join(dependency_list)

        return dependency_dict, command

    def local_only(self, build_context=None):
        """Returns whether or not the job can only be run on the master box"""
        if build_context is None:
            build_context = {}

        return False

    def get_dimensions(self, build_context=None):
        """Returns all the dimensions the job has in list form"""
        return self.dimensions

    def get_command(self, build_graph):
        return self._replace_command(self.command, build_graph)

class DeepyMetaJobState(builder.jobs.MetaJobState, DeepyTimestampExpandedJobState):
    """A wrapper for deepy meta jobs states"""

class BundleState(DeepyTimestampExpandedJobState):
    """A state that should always run because bundles should pretty much always
    run
    """
    def get_should_run(self, build_garph, cached=True, cache_set=None):
        return True

    def get_should_run_immediate(self, build_graph, cached=True):
        return True

    def get_parents_should_not_run(self, build_graph, cache_time, cached=True,
                                   cache_set=None):
        return True

class DeepyJob(builder.jobs.Job):
    """Used to give the jobs the command replacement"""
    meta = {}

    def get_state_type(self):
        return DeepyJobState

    def get_dimensions(self, build_context=None):
        """Returns all the dimensions the job has in list form"""
        if build_context is None:
            build_context = {}

        return []


class DeepyTimestampExpandedJob(DeepyJob, builder.jobs.TimestampExpandedJob):
    """A timestamp expanded form of deepy job"""
    def get_state_type(self):
        return DeepyTimestampExpandedJobState

    def expand(self, build_context, log_it=False):
        job_type = self.get_state_type()
        expanded_contexts = (builder
                                  .expanders
                                  .TimestampExpander
                                  .expand_build_context(
                                         build_context, self.get_expandable_id(),
                                         self.file_step))
        expanded_nodes = []
        for expanded_id, build_context in expanded_contexts.iteritems():
            expanded_node = job_type(self.unexpanded_id,
                    expanded_id, build_context, self.get_command(),
                    self.cache_time, self.curfew, self.rules_db[self.rule_id],
                    config=self.config)
            expanded_nodes.append(expanded_node)
        return expanded_nodes

class DeepyDictJob(DeepyTimestampExpandedJob):

    def __init__(self, rule_id, rules_db, config=None, expander=builder.expanders.TimestampExpander):
        super(DeepyDictJob, self).__init__(config=config)
        self.unexpanded_id = rule_id
        self.rule_id = rule_id
        self.rules_db = rules_db
        self.expander = expander
        self.rule = self.rules_db[self.rule_id]


        # get the file step using a process that can handle many revisions of
        # file_step
        self.file_step = self.rule.get(
                "file_step", self.rule.get(
                    "make_time_step", self.rule.get(
                        "time_step", None)))

        base_datetime = datetime.datetime.now()
        max_relative_delta = dateutil.relativedelta.relativedelta(minutes=5)
        max_file_step = "5min"
        if self.file_step is None:
            for _, dependency_group in self.get_dependencies().iteritems():
                for dependency in dependency_group:
                    time_delta = deepy.timerange.convert_to_timedelta(
                            dependency.file_step)
                    relative_delta = time_delta
                    if isinstance(time_delta, datetime.timedelta):
                        relative_delta = dateutil.relativedelta.relativedelta(
                                seconds=time_delta.total_seconds())
                    if (relative_delta + base_datetime >
                            max_relative_delta + base_datetime):
                        max_relative_delta = relative_delta
                        max_file_step = dependency.file_step
            self.file_step = max_file_step

    def get_state_type(self):
        """Returns what type of job state the job should expand to"""
        if self.get_type() == "bundle":
            return BundleState
        elif "meta" in self.get_type():
            return DeepyMetaJobState
        return DeepyTimestampExpandedJobState

    def get_type(self):
        """Returns what type of job this is
        possible:
            job: A normal job
            meta: A job that is used for specifying multiple jobs
            target: A job that only specifies a target
        """
        if self.rule.get("type") != "bundle":
            if self.get_command() is not None:
                return "job"
            elif self.get_targets()["produces"]:
                return "target"
            else:
                return "meta"
        else:
            if self.get_command() is not None:
                return "bundle"
            else:
                return "bundle_meta"

    def get_command(self):
        return self.rule.get("recipe", [None])[0]

    def get_dependencies(self, build_context=None):
        dependencies = {}
        depends = []
        depends_one_or_more = []

        definition = self.rules_db[self.rule_id]
        for rule_id, time_step in definition.get('depends', {}).iteritems():
            dependency_definition = self.rules_db[rule_id]
            dependency = DeepyDictJob(rule_id, self.rules_db,
                                      config=self.config,
                                      expander=self.expander)
            targets = dependency.get_targets()
            depends = depends + targets["produces"]

        for rule_id, time_step in definition.get('depends_one_or_more', {}).iteritems():
            dependency_definition = self.rules_db[rule_id]
            dependency = DeepyDictJob(rule_id, self.rules_db,
                                      config=self.config,
                                      expander=self.expander)
            targets = dependency.get_targets()
            depends_one_or_more = depends_one_or_more + targets["produces"]

        if depends:
            dependencies['depends'] = depends

        if depends_one_or_more:
            dependencies['depends_one_or_more'] = depends_one_or_more

        return dependencies

    def get_targets(self, build_context=None):
        definition = self.rules_db[self.rule_id]
        target_id = definition.get('target')

        targets = {'produces':[]}
        if target_id:
            targets['produces'].append(self.expander(
                        builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                        target_id,
                        self.file_step)
            )

        # Get all of the back-referenced target-only nodes
        for rule_id, rule in self.rules_db.iteritems():
            if rule.get('depends', {}).get(self.rule_id) and rule.get('target') and (not rule.get('recipe')):
                # Add target
                targets['produces'].append(self.expander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    rule['target'],
                    rule['file_step']),)

        if not targets["produces"] and self.get_command() is not None:
            print deepy.log.warn("{} has a recipe but no target".format(self.rule_id))

        return targets

    def get_dimensions(self, build_context=None):
        return self.rule.get('dimensions') or []

    def __repr__(self):
        return str(json.dumps(self.rules_db[self.rule_id], indent=2))


class DeepyHourJob(DeepyJob):

    """Used to default to a 12 hour curfew"""
    curfew = "12h"


class DeepyCachedJob(DeepyJob):

    """Used to default to a 2 hour cache"""
    cache_time = "2h"

