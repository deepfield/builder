import collections
import copy
import datetime
import inspect
import string
import sys

import dateutil

import builder.deepy_targets
import builder.jobs
import deepy.make

class DeepyJobState(builder.jobs.JobState):
    """A wrapper for the job state, might never be used"""

class DeepyTimestampExpandedJobState(
        builder.jobs.TimestampExpandedJobState, DeepyJobState):
    """A wrapper that give job states the ability to do a command
    replacement
    """
    def __init__(self, unexpanded_id, unique_id, build_context, command,
            cache_time, curfew, dimensions, config=None):
        super(DeepyTimestampExpandedJobState, self).__init__(unexpanded_id,
                unique_id, build_context, command, cache_time, curfew,
                config=config)
        self.dimensions = dimensions

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
            new_command = deepy.make._basic_subst(
                command, self.build_context["start_time"])
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
            dimensions = self.get_dimensions()
            # dimensions = self._get_list_merge("dimensions")
            dimensions = ",".join(dimensions)
            command = command.replace("$D", dimensions)
        if "$A" in command:
            user_args = self.build_context.get('user_args') or []
            user_args = " ".join(user_args)
            command = command.replace("$A", user_args)
            # command = command.replace("$A", " ".join(user_args))

        command = str(command)
        return command

    def _get_list_merge(self, key):
        """Merges in the extend and remove dictionarys"""
        val = self.__dict__.get(key, [])
        # Copy the list so we don't modify the original.
        val = val[:]
        ext = self.__dict__.get(key + '.extend')
        rem = self.__dict__.get(key + '.remove')
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
                    self.cache_time, self.curfew, self.get_dimensions(),
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

        # Get all the query targets
        for _, query in self.rule.get("queries", {}).iteritems():
            target_id = query.get("target")
            target = self.expander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    target_id,
                    self.file_step)
            targets["produces"].append(target)

        return targets

    def get_dimensions(self, build_context=None):
        return self.rule.get('dimensions') or []


class DeepyHourJob(DeepyJob):

    """Used to default to a 12 hour curfew"""
    curfew = "12h"


class DeepyCachedJob(DeepyJob):

    """Used to default to a 2 hour cache"""
    cache_time = "2h"


# all of the following are meta's not sure what to do with them yet
class Cubes5Min(DeepyTimestampExpandedJob,
                builder.jobs.MetaJob, DeepyJob):

    """class for the job 5min cubes """
    unexpanded_id = "cubes_5min_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"

    def get_dependencies(self, build_context=None):
        depends_dict = {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/snmp/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill_small/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill_small_network/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

        if self.config.get("has_backbone", False):
            depends_dict["depends"] = (depends_dict["depends"] + [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_small/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_small_bgp/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ]
            )
        return depends_dict

    def get_targets(self, build_context=None):
        return {
            "produces": [
            ],
        }


class CubesHour(DeepyTimestampExpandedJob,
                builder.jobs.MetaJob):

    """class for the job hour cube_ops """
    unexpanded_id = "cubes_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"

    def get_dependencies(self, build_context=None):
        depends_dict = {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/interface/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/origin_asn.remote3/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/aspaths.remote3/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill_small/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill_small_network/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_ip_version_total/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/geo1/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_cloud/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_site/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_site_total/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_category_total/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_ip_version/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_cloud_total/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/cube_snmp/days/cube.%Y-%m-%d.h5",
                    "1d"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_category/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "cubes_hour_deployment missing",
                    "1h"),
            ],
            "depends_one_or_more": [
            ],
        }

        if self.config.get("has_backbone", False):
            depends_dict["depends"] = (depends_dict["depends"] +
                                       [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_small/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_small_bgp/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ]
            )

        return depends_dict

    def get_targets(self, build_context=None):
        return {
            "produces": [
            ],
        }


class CubesDay(DeepyTimestampExpandedJob,
               builder.jobs.MetaJob):

    """class for the job day cube_ops """
    unexpanded_id = "cubes_day_%Y-%m-%d-%H-%M"
    file_step = "5min (default)"
    time_step = "5min (default)"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/snmp/days/cube.%Y-%m-%d.h5",
                    "1d"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/geo1/days/cube.%Y-%m-%d.h5",
                    "1d"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/days/cube.%Y-%m-%d.h5",
                    "1d"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "cubes_day_deployment missing",
                    "1d"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
            ],
        }


class CubesMonth(DeepyTimestampExpandedJob,
                 builder.jobs.MetaJob):

    """class for the job month cube_ops """
    unexpanded_id = "cubes_month_%Y-%m-%d-%H-%M"
    file_step = "5min (default)"
    time_step = "5min (default)"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/origin_asn.remote2/months/cube.%Y-%m.h5",
                    "1d"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sites2/months/top_list.%Y-%m.json.gz",
                    "1d"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/origin_asn.remote2/months/top_list.%Y-%m.json.gz",
                    "1d"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/company2/months/top_list.%Y-%m.json.gz",
                    "1d"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sites2/months/cube.%Y-%m.h5",
                    "1d"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/aspaths.remote2/months/top_list.%Y-%m.json.gz",
                    "1d"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/company2/months/cube.%Y-%m.h5",
                    "1d"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/aspaths.remote2/months/cube.%Y-%m.h5",
                    "1d"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
            ],
        }


class Snmp5Min(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job snmp5min ***** craig new in sprint-14-snmp ****"""
    unexpanded_id = "snmp5min_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"
    meta = {'cube_id': 'snmp'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/snmp/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return self._replace_command("cube_from_snmp_new.py $A -t %Y-%m-%d-%H-%M", build)

    def get_enable(config=None):
        return False


class Snmp5MinDay(DeepyTimestampExpandedJob, DeepyCachedJob):

    """class for the job snmp5min_day ***** craig new in sprint-14-snmp ****"""
    unexpanded_id = "snmp5min_day_%Y-%m-%d-%H-%M"
    file_step = "1d"
    time_step = "5min"
    meta = {'cube_id': 'snmp'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/snmp/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/snmp/days/cube.%Y-%m-%d.h5",
                    "1d"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ $^"

    def get_enable(config=None):
        return False


class CubeSnmp(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job this hour data per snmp interface """
    unexpanded_id = "cube_snmp_%Y-%m-%d-%H-%M"
    file_step = "1d"
    time_step = "1h"
    meta = {'cube_id': 'cube_snmp'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(snmp_dir)/snmp/snmp.%Y-%m-%d-%H-%M.json.gz",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/cube_snmp/days/cube.%Y-%m-%d.h5",
                    "1d"),
            ],
        }

    def get_command(self):
        return "cube_from_snmp.py -t %Y-%m-%d"


class CubeHeartbeats(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job one set of all deployment heartbeats """
    unexpanded_id = "cube_heartbeats_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"
    meta = {'cube_id': 'cube_heartbeats'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/heartbeats/markers/%Y-%m-%d-%H-%M.marker",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/heartbeats/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "cube_from_heartbeats.py $A -t %Y-%m-%d-%H-%M"


class UiLogHour(DeepyTimestampExpandedJob, DeepyHourJob):

    """class for the job  """
    unexpanded_id = "ui_log_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'cube_ui_log'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/log_cubes/ui/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "cube_from_log.py"


class UiLogDay(DeepyTimestampExpandedJob, DeepyCachedJob):

    """class for the job  """
    unexpanded_id = "ui_log_day_%Y-%m-%d-%H-%M"
    file_step = "1d"
    time_step = "1d"
    meta = {'cube_id': 'cube_ui_log'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/log_cubes/ui/days/cube.%Y-%m-%d.h5",
                    "1d"),
            ],
        }

    def get_command(self):
        return "cube_from_log.py"


class H5Flow(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job flow to h5flow """
    unexpanded_id = "h5flow_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"

    def local_only(self, config=None, build_context=None):
        if build_context is None:
            build_context = {}

        return not self.config.get("archive_flow", False)

    def get_dependencies(self, build_context=None):
        depends_dict = {
            "depends": [
            ],
            "depends_one_or_more": [
            ],
        }

        target_type = builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget
        if self.local_only(config=self.config, build_context=build_context):
            target_type = builder.deepy_targets.DeepyLocalFileSystemTarget

        flowd_options = self.config.get("flowd_options", {})
        num_flowd = flowd_options.get("num", 1)
        if num_flowd == 1:
            depends_dict["depends"].append(
                builder.expanders.TimestampExpander(
                    target_type,
                    "$(flows_dir)/flow.%Y-%m-%d-%H-%M.pcap.gz",
                    "5min"))
        else:
            for index in range(num_flowd):
                depends_dict["depends"].append(
                    builder.expanders.TimestampExpander(
                        target_type,
                        "$(flows_dir)/flow.%Y-%m-%d-%H-%M.pcap.gz.{}".format(
                            index),
                        "5min"))
        return depends_dict

    def get_targets(self, build_context=None):
        target_type = builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget
        if not self.config.get("archive_h5flow", False):
            target_type = builder.deepy_targets.DeepyLocalFileSystemTarget

        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    target_type,
                    "$(h5flow_dir)/flow.%Y-%m-%d-%H-%M.h5.raw",
                    "5min"),
            ],
            "alternates": [
                builder.expanders.TimestampExpander(
                    target_type,
                    "$(h5flow_dir)/flow.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "flow.py $A -t %Y-%m-%d-%H-%M"


class H5Dns(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job dnspcap to h5dns """
    unexpanded_id = "h5dns_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"

    def local_only(self, config=None, build_context=None):
        if build_context is None:
            build_context = {}
        return not self.config.get("archive_dnsflow", False)

    def get_dependencies(self, build_context=None):
        depends_dict = {
            "depends": [
            ],
            "depends_one_or_more": [
            ],
        }

        if not self.config.get("dnsflow_enabled", False):
            return depends_dict

        target_type = builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget
        if self.local_only(config=self.config, build_context=build_context):
            target_type = builder.deepy_targets.DeepyLocalFileSystemTarget

        flowd_options = self.config.get("flowd_options", {})
        num_flowd = flowd_options.get("num", 1)

        if num_flowd == 1:
            depends_dict["depends"].append(
                builder.expanders.TimestampExpander(
                    target_type,
                    "$(dnsflows_dir)/dns.%Y-%m-%d-%H-%M.pcap.gz",
                    "5min"))
        else:
            for index in range(num_flowd):
                depends_dict["depends"].append(
                    builder.expanders.TimestampExpander(
                        target_type,
                        "$(dnsflows_dir)/dns.%Y-%m-%d-%H-%M.pcap.gz.{}".format(
                            index),
                        "5min"))
        return depends_dict

    def get_targets(self, build_context=None):
        target_type = builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget
        if not self.config.get("archive_h5dns", False):
            target_type = builder.deepy_targets.DeepyLocalFileSystemTarget

        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    target_type,
                    "$(h5dns_dir)/dns.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "dnsflow.py $A -t %Y-%m-%d-%H-%M"


class H5Bgp(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job Build BGP H5 file """
    unexpanded_id = "h5bgp_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.Expander(
                    builder.deepy_targets.DeepyS3BackedGlobLocalFileSystemTarget,
                    "$(bgp_dir)/dumps/*.gz"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyLocalFileSystemTarget,
                    "$(bgp_dir)/bgp.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "bgp.py"


class Routemap(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job Build routemap H5 file """
    unexpanded_id = "routemap_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyLocalFileSystemTarget,
                    "$(cache_dir)/routemap/routemap.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "routemap.py"


class ClassifyH5Flow(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job classify h5flow (.raw -> h5flow) """
    unexpanded_id = "classify_h5flow_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyLocalFileSystemTarget,
                    "$(h5flow_dir)/flow.%Y-%m-%d-%H-%M.h5.raw",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyLocalFileSystemTarget,
                    "$(h5dns_dir)/dns.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyLocalFileSystemTarget,
                    "$(h5flow_dir)/flow.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "classify.py $A -t %Y-%m-%d-%H-%M"


class CubesFromH5Flow5Min(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job cubes_from_h5flow_5min """
    unexpanded_id = "cubes_from_h5flow_5min_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"

    def local_only(self, config=None, build_context=None):
        return not self.config.get("archive_h5dns", False)

    def get_dependencies(self, build_context=None):
        target_type = builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget
        if not self.local_only(
                config=self.config, build_context=build_context):
            target_type = builder.deepy_targets.DeepyLocalFileSystemTarget

        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    target_type,
                    "$(h5flow_dir)/flow.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        targets_dict = {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/big_cube/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/site_peer/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_cloud/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_ip_version/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_site_total/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_cloud_total/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_category/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_site_peer/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_site/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_category_total/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_site_peer_total/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_ip_version_total/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyLocalFileSystemTarget,
                    "$(cubes_dir)/interface/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

        if self.config.get("has_backbone", False):
            targets_dict["produces"] = (targets_dict["produces"] + [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_backbone_total/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ])

        return targets_dict

    def get_command(self):
        return "cubes_from_h5flow.py $A -B -t %Y-%m-%d-%H-%M"


def get_enable(id):
        list = {
                "DrillBackbone": False}
        return list[id]


class CubesStreamBps5Min(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job cubes_stream_bps_5min """
    unexpanded_id = "cubes_stream_bps_5min_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"
    meta = {'cube_id': 'stream_bps'}

    def local_only(self, config=None, build_context=None):
        return not self.config.get("archive_h5flow", False)

    def get_dependencies(self, build_context=None):
        target_type = builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget
        if self.local_only(
                config=self.config, build_context=build_context):
            target_type = builder.deepy_targets.DeepyLocalFileSystemTarget

        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    target_type,
                    "$(h5flow_dir)/flow.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyLocalFileSystemTarget,
                    "$(cubes_dir)/stream_bps/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "h5flow_bps.py $A -t %Y-%m-%d-%H-%M --cube --data-frame"


class SnmpQuality(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job produced by the cube_snmp script, not actually triggered by anything """
    unexpanded_id = "snmp_quality_%Y-%m-%d-%H-%M"
    file_step = "1d"
    time_step = "1d"
    meta = {'cube_id': 'snmp_quality'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/snmp_quality/days/cube.%Y-%m-%d.h5",
                    "1d"),
            ],
        }

    def get_command(self):
        return "cube_from_snmp.py"


class SnmpQualityRouter(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job produced by the cube_snmp script, not actually triggered by anything """
    unexpanded_id = "snmp_quality_router_%Y-%m-%d-%H-%M"
    file_step = "1d"
    time_step = "1d"
    meta = {'cube_id': 'snmp_quality_router'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/snmp_quality_router/days/cube.%Y-%m-%d.h5",
                    "1d"),
            ],
        }

    def get_command(self):
        return "cube_from_snmp.py"


class Supplychain(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job make supplychains from h5flow """
    unexpanded_id = "supplychain_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyLocalFileSystemTarget,
                    "$(h5flow_dir)/flow.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyLocalFileSystemTarget,
                    "$(supplychain_dir)/supplychain.%Y-%m-%d-%H-%M.json.gz",
                    "5min"),
            ],
        }

    def get_command(self):
        return "supplychain.py $A -t %Y-%m-%d-%H-%M"


class CubesFromH5FlowIpcountHour(DeepyTimestampExpandedJob, DeepyHourJob):

    """class for the job cubes_from_h5flow_ipcount_hour """
    unexpanded_id = "cubes_from_h5flow_ipcount_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "5min"

    def local_only(self, config=None, build_context=None):
        return not self.config.get("archive_h5dns", False)

    def get_dependencies(self, build_context=None):
        target_type = builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget
        if self.local_only(
                config=self.config, build_context=build_context):
            target_type = builder.deepy_targets.DeepyLocalFileSystemTarget

        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    target_type,
                    "$(h5flow_dir)/flow.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        targets_dict = {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_site_total/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_ip_version_total/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_cloud/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_cloud_total/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_site/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }
        if self.config.get("has_backbone", False):
            targets_dict["produces"].append(
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_backbone_total/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"))

        return targets_dict

    def get_command(self):
        return "cubes_from_h5flow.py --ipcount_hour -t %Y-%m-%d-%H-%M $^"


class CubeBigCubeDay(DeepyTimestampExpandedJob, DeepyCachedJob):

    """class for the job cube_big_cube_day """
    unexpanded_id = "cube_big_cube_day_%Y-%m-%d-%H-%M"
    file_step = "1d"
    time_step = "1d"
    meta = {'cube_id': 'big_cube'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/big_cube/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/big_cube/days/cube.%Y-%m-%d.h5",
                    "1d"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A timestep_change(days) $^"


class RocketfuelTrace(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job rocketfuel_trace """
    unexpanded_id = "rocketfuel_trace_%Y-%m-%d-%H-%M"
    file_step = "1d"
    time_step = "1d"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(geoip_dir)/rocketfuel/traces/rocketfuel.%Y-%m-%d.json.gz",
                    "1d"),
            ],
        }

    def get_command(self):
        return "rocketfuel_trace.py"


class InfrastructureCountCdn(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job infrastructure_count_cdn """
    unexpanded_id = "infrastructure_count_cdn_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"
    meta = {'cube_id': 'infrastructure_count_cdn'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/infrastructure_count_cdn/hours/cube.%Y-%m-%d-%H.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "no command"


class InterfaceHour(DeepyTimestampExpandedJob, DeepyHourJob):

    """class for the job interface_hour """
    unexpanded_id = "interface_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'interface'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyLocalFileSystemTarget,
                    "$(cubes_dir)/interface/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/interface/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A timestep_change(hours) $^"


class CubeBigCubeHour(DeepyTimestampExpandedJob, DeepyHourJob):

    """class for the job cube_big_cube_hour """
    unexpanded_id = "cube_big_cube_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'big_cube'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/big_cube/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/big_cube/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A timestep_change(hours) $^"


class CubeSubCountCategoryHour(DeepyTimestampExpandedJob, DeepyHourJob):

    """class for the job cube_sub_count_category_hour """
    unexpanded_id = "cube_sub_count_category_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'sub_count_category'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/big_cube/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_category/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "no command"


class CubeSubCountCategoryTotalHour(DeepyTimestampExpandedJob, DeepyHourJob):

    """class for the job cube_sub_count_category_hour """
    unexpanded_id = "cube_sub_count_category_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'sub_count_category'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/big_cube/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sub_count_category_total/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "no command"


class CubeGeo1Hour(DeepyTimestampExpandedJob, DeepyHourJob):

    """class for the job cube_geo1_hour """
    unexpanded_id = "cube_geo1_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'geo1'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/big_cube/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/geo1/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -t 3600 -O $D $^"

    def get_dimensions(config=None, build_context=None):
        return [
                'timestamp',
                'path',
                'class.local',
                'class.remote',
                'service',
                'cdn',
                'geoip.local',
                'geoip.remote',
                'router.local',
                'router.remote',
                'pops.local',
                'pops.remote',
                'category'
        ]


class CubeGeo1Day(DeepyTimestampExpandedJob, DeepyCachedJob):

    """class for the job cube_geo1_day """
    unexpanded_id = "cube_geo1_day_%Y-%m-%d-%H-%M"
    file_step = "1d"
    time_step = "1d"
    meta = {'cube_id': 'geo1'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/geo1/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/geo1/days/cube.%Y-%m-%d.h5",
                    "1d"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A timestep_change(days) $^"


class CubeDrill15Min(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job cube_drill1_5min """
    unexpanded_id = "cube_drill1_5min_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"
    meta = {'cube_id': 'drill1'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/big_cube/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -t 300 -O $D $^"

    def get_dimensions(config=None, build_context=None):
        optional_dimensions = ['class.local', 'class.remote']
        return [
            'timestamp',
            'ip_version',
            'cdn',
            'service',
            'sites',
            'peer.remote',
            'origin_asn.remote',
            'aspaths.remote',
            'pops.remote',
            'path',
            'class.local',
            'class.remote',
            'category',
            'company'
        ]


class CubeDrill1Hour(DeepyTimestampExpandedJob, DeepyHourJob):

    """class for the job cube_drill1_hour """
    unexpanded_id = "cube_drill1_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'drill1'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/big_cube/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -t 3600 -O $D $^"

    def get_dimensions(config=None, build_context=None):
        return [
                'timestamp',
                'ip_version',
                'cdn',
                'service',
                'sites',
                'peer.remote',
                'origin_asn.remote',
                'aspaths.remote',
                'pops.remote',
                'path',
                'class.local',
                'class.remote',
                'category',
                'company'
        ]


class CubeDrill1Day(DeepyTimestampExpandedJob, DeepyCachedJob):

    """class for the job cube_drill1_day """
    unexpanded_id = "cube_drill1_day_%Y-%m-%d-%H-%M"
    file_step = "1d"
    time_step = "1d"
    meta = {'cube_id': 'drill1'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/days/cube.%Y-%m-%d.h5",
                    "1d"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A timestep_change(days) $^"


class CubeDrillSmall5Min(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job Using group_other for big dimensions. Make sure that null is kept out of other so it can be excluded when the dimension is used as drill dimension (2nd level). Also, can't slice null for a particular dimension out because that traffic will be part of the total for another dimension. """
    unexpanded_id = "cube_drill_small_5min_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"
    meta = {'cube_id': 'drill_small'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill_small/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/origin_asn.remote2/months/top_list.%Y-%m.json.gz",
                    "month", ignore_mtime=True),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/company2/months/top_list.%Y-%m.json.gz",
                    "month", ignore_mtime=True),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sites2/months/top_list.%Y-%m.json.gz",
                    "month", ignore_mtime=True),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/aspaths.remote2/months/top_list.%Y-%m.json.gz",
                    "month", ignore_mtime=True),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -t 300  -A group_other(origin_asn.local,null,<c.top_origins>)  -A group_other(aspaths.local,null,<c.top_aspaths>) -A group_other(origin_asn.remote,null,<c.top_origins>) --arg_join c.top_origins='$(cubes_dir)/origin_asn.remote2/months/top_list.%Y-%m.json.gz' -A group_other(aspaths.remote,null,<c.top_aspaths>) --arg_join c.top_aspaths='$(cubes_dir)/aspaths.remote2/months/top_list.%Y-%m.json.gz' -A group_other(sites,null,<c.top_sites>) -A group_other(company,null,<c.top_companies>) --arg_join c.top_companies='$(cubes_dir)/company2/months/top_list.%Y-%m.json.gz' --arg_join c.top_sites='$(cubes_dir)/sites2/months/top_list.%Y-%m.json.gz' {deepy.build.deepy_jobs:CubeDrill15Min}"


class CubeDrillSmallNetwork5Min(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job cube_drill_small_network_5min """
    unexpanded_id = "cube_drill_small_network_5min_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"
    meta = {'cube_id': 'drill_small_network'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill_small/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill_small_network/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -O $D $^"

    def get_dimensions(config=None, build_context=None):
        return ['timestamp', 'sites', 'category', 'cdn']


class CubeDrillSmallNetworkHour(DeepyTimestampExpandedJob, DeepyHourJob):

    """class for the job cube_drill_small_network_hour """
    unexpanded_id = "cube_drill_small_network_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'drill_small_network'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill_small_network/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill_small_network/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A timestep_change(hours) $^"

    def get_dimensions(config=None, build_context=None):
        return ['timestamp', 'sites', 'category', 'cdn']


class CubeDrillSmallHour(DeepyTimestampExpandedJob, DeepyHourJob):

    """class for the job Using group_other for big dimensions. Make sure that null is kept out of other so it can be excluded when the dimension is used as drill dimension (2nd level). Also, can't slice null for a particular dimension out because that traffic will be part of the total for another dimension. """
    unexpanded_id = "cube_drill_small_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'drill_small'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/origin_asn.remote2/months/top_list.%Y-%m.json.gz",
                    "month",
                    ignore_mtime=True),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sites2/months/top_list.%Y-%m.json.gz",
                    "month",
                    ignore_mtime=True),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/aspaths.remote2/months/top_list.%Y-%m.json.gz",
                    "month",
                    ignore_mtime=True),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill_small/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/origin_asn.remote2/months/top_list.%Y-%m.json.gz",
                    "month", ignore_mtime=True),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sites2/months/top_list.%Y-%m.json.gz",
                    "month", ignore_mtime=True),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/aspaths.remote2/months/top_list.%Y-%m.json.gz",
                    "month", ignore_mtime=True),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -t 3600 -A group_other(origin_asn.local,null,<c.top_origins>) -A group_other(origin_asn.remote,null,<c.top_origins>) --arg_join c.top_origins='$(cubes_dir)/origin_asn.remote2/months/top_list.%Y-%m.json.gz'  -A group_other(aspaths.local,null,<c.top_aspaths>)  -A group_other(aspaths.remote,null,<c.top_aspaths>) --arg_join c.top_aspaths='$(cubes_dir)/aspaths.remote2/months/top_list.%Y-%m.json.gz' -A group_other(sites,null,<c.top_sites>) --arg_join c.top_sites='$(cubes_dir)/sites2/months/top_list.%Y-%m.json.gz' {deepy.build.deepy_jobs:CubeDrill1Hour}"


class CubeCompany2Month(DeepyTimestampExpandedJob, DeepyCachedJob):

    """class for the job No timestamp or other dimensions, just total for company """
    unexpanded_id = "cube_company2_month_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'company2'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/company2/months/cube.%Y-%m.h5",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -D $D $^"

    def get_dimensions(self, build_graph):
        return ['company']


class CubeCompany2MonthTopList(DeepyTimestampExpandedJob, DeepyCachedJob):

    """class for the job Hack to get results to feed back into cube_op.py """
    unexpanded_id = "cube_company2_month_top_list_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'company2'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/company2/months/cube.%Y-%m.h5",
                    "month"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/company2/months/top_list.%Y-%m.json.gz",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A measure_fixup() -S company!=null -A sort(sum.total.bytes,desc,0,500) -A return_dimension_names() -F company $^"


class CubeOriginAsnRemote2Month(DeepyTimestampExpandedJob, DeepyCachedJob):

    """class for the job No timestamp or other dimensions, just total for asn """
    unexpanded_id = "cube_origin_asn_remote2_month_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'origin_asn_remote2'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/origin_asn.remote2/months/cube.%Y-%m.h5",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -D $D $^"

    def get_dimensions(config=None, build_context=None):
        return ['origin_asn.remote']


class CubeOriginAsnRemote2MonthTopList(DeepyTimestampExpandedJob, DeepyCachedJob):

    """class for the job Hack to get results to feed back into cube_op.py """
    unexpanded_id = "cube_origin_asn_remote2_month_top_list_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'origin_asn_remote2'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/origin_asn.remote2/months/cube.%Y-%m.h5",
                    "month"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/origin_asn.remote2/months/top_list.%Y-%m.json.gz",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A measure_fixup() -S origin_asn.remote!=null -A sort(sum.total.bytes,desc,0,500) -A return_dimension_names() -F origin_asn.remote $^"


class CubeOriginAsnRemote3Hour(DeepyTimestampExpandedJob, DeepyHourJob):

    """class for the job Origin using group_other """
    unexpanded_id = "cube_origin_asn_remote3_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'origin_asn_remote3'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/origin_asn.remote3/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -t 3600 -S origin_asn.remote!=null -A group_other(origin_asn.remote,<c.top_origins>) --arg_join c.top_origins='$(cubes_dir)/origin_asn.remote2/months/top_list.%Y-%m.json.gz' $^"


class CubeSites2Month(DeepyTimestampExpandedJob, DeepyCachedJob):

    """class for the job No timestamp or other dimensions, just total for site """
    unexpanded_id = "cube_sites2_month_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'sites2'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sites2/months/cube.%Y-%m.h5",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -D $D $^"

    def get_dimensions(config=None, build_context=None):
        return ['sites']


class CubeSites2MonthTopList(DeepyTimestampExpandedJob, DeepyCachedJob):

    """class for the job Hack to get results to feed back into cube_op.py """
    unexpanded_id = "cube_sites2_month_top_list_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'sites2'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sites2/months/cube.%Y-%m.h5",
                    "month"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/sites2/months/top_list.%Y-%m.json.gz",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A measure_fixup() -S sites!=null -A sort(sum.total.bytes,desc,0,500) -A return_dimension_names() -F sites $^"


class CubeAspathsRemote2Month(DeepyTimestampExpandedJob, DeepyCachedJob):

    """class for the job No timestamp or other dimensions, just total for aspaths_remote """
    unexpanded_id = "cube_aspaths_remote2_month_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'aspaths_remote2'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/aspaths.remote2/months/cube.%Y-%m.h5",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -D $D $^"

    def get_dimensions(config=None, build_context=None):
        return ['aspaths.remote']


class CubeAspathsRemote2MonthTopList(DeepyTimestampExpandedJob, DeepyCachedJob):

    """class for the job Hack to get results to feed back into cube_op.py """
    unexpanded_id = "cube_aspaths_remote2_month_top_list_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'aspaths_remote2'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/aspaths.remote2/months/cube.%Y-%m.h5",
                    "month"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/aspaths.remote2/months/top_list.%Y-%m.json.gz",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A measure_fixup() -S aspaths.remote!=null -A sort(sum.total.bytes,desc,0,500) -A return_dimension_names() -F aspaths.remote $^"


class CubeAspathsRemote3Hour(DeepyTimestampExpandedJob, DeepyHourJob):

    """class for the job Aspaths using group_other """
    unexpanded_id = "cube_aspaths_remote3_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'aspaths_remote3'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/drill1/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/aspaths.remote3/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -t 3600 -S aspaths.remote!=null -A group_other(aspaths.remote,<c.top_aspaths>) --arg_join c.top_aspaths='$(cubes_dir)/aspaths.remote2/months/top_list.%Y-%m.json.gz' {deepy.build.deepy_jobs:CubeDrill1Hour}"


class Searchips(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job searchips """
    unexpanded_id = "searchips_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"

    def local_only(self, config=None, build_context=None):
        return not self.config.get("archive_h5flow", False)

    def get_dependencies(self, build_context=None):
        target_type = builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget
        if not self.local_only(
                config=self.config, build_context=build_context):
            target_type = builder.deepy_targets.DeepyLocalFileSystemTarget

        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    target_type,
                    "$(h5flow_dir)/flow.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyLocalFileSystemTarget,
                    "$(cache_dir)/search_ips/summary.%Y-%m-%d-%H-%M.json.gz",
                    "5min"),
            ],
        }

    def get_command(self):
        return "searchips.py $A -t %Y-%m-%d-%H-%M"


class BuildSearchIps(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job build_search_ips """
    unexpanded_id = "build_search_ips_%Y-%m-%d-%H-%M"
    file_step = "5min (default)"
    time_step = "1h"

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cache_dir)/summaries/top_ips/top_ips_summary.%Y-%m-%d-%H.json.gz",
                    "5min (default)"),
            ],
        }

    def get_command(self):
        return "build_search_ips.py $A -t %Y-%m-%d-%H"


class VmStat(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job vm_stat """
    unexpanded_id = "vm_stat_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"
    meta = {'cube_id': 'vm_stat'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/vm_stat/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "cube_from_hb.py -t %Y-%m-%d-%H-%M"


class DnsflowMatches(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job dnsflow_matches """
    unexpanded_id = "dnsflow_matches_%Y-%m-%d-%H-%M"
    file_step = "5min (default)"
    time_step = "5min"
    meta = {'cube_id': 'dnsflow_matches'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/dnsflow_matches/cube.%Y-%m-%d-%H-%M.h5",
                    "5min (default)"),
            ],
        }

    def get_command(self):
        return "match_dnsflow.py -t %Y-%m-%d-%H-%M -r 6 -c 500 -R -o $(cubes_dir)/dnsflow_matches/cube.%Y-%m-%d-%H-%M.h5"


class RouterStat(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job router_stat """
    unexpanded_id = "router_stat_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"
    meta = {'cube_id': 'router_stat'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(heartbeat_dir)/$(vm_uuid)/vm/vm.%Y-%m-%d-%H-%M.json.gz",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyLocalFileSystemTarget,
                    "$(cubes_dir)/router_stat/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "router_cube.py -t %Y-%m-%d-%H-%M"


class CubeBackboneSmallBgp5Min(DeepyTimestampExpandedJob, DeepyJob):

    """class for the job cube_backbone_small_bgp_5min """
    unexpanded_id = "cube_backbone_small_bgp_5min_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"
    meta = {'cube_id': 'backbone_small_bgp'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_small/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_small_bgp/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -O $D $^"

    def get_dimensions(config=None, build_context=None):
        return ['timestamp', 'sites', 'category', 'cdn']

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackboneSmallBgpHour(DeepyTimestampExpandedJob, DeepyHourJob):
    """class for the job cube_backbone_small_bgp_hour """
    unexpanded_id = "cube_backbone_small_bgp_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'backbone_small_bgp'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_small_bgp/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_small_bgp/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A timestep_change(hours) $^"

    def get_dimensions(config=None, build_context=None):
        return ['timestamp', 'sites', 'category', 'cdn']

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackboneHour(DeepyTimestampExpandedJob, DeepyHourJob):
    """class for the job cube_backbone_hour """
    unexpanded_id = "cube_backbone_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'backbone'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A timestep_change(hours) $^"

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackboneSmall5Min(DeepyTimestampExpandedJob, DeepyJob):
    """class for the job cube_backbone_small_5min """
    unexpanded_id = "cube_backbone_small_5min_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"
    meta = {'cube_id': 'backbone_small'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_small/minutes/cube.%y-%m-%d-%h-%m.h5",
                    "5min"),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/aspaths.remote2/months/top_list.%Y-%m.json.gz",
                    "month", ignore_mtime=True),
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/origin_asn.remote2/months/top_list.%Y-%m.json.gz",
                    "month", ignore_mtime=True),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -t 300 -A group_other(origin_asn.remote,null,<c.top_origins>) --arg_join c.top_origins='$(cubes_dir)/origin_asn.remote2/months/top_list.%Y-%m.json.gz' -A group_other(origin_asn.local,null,<c.top_origins>)   -A group_other(aspaths.remote,null,<c.top_aspaths>) --arg_join c.top_aspaths='$(cubes_dir)/aspaths.remote2/months/top_list.%Y-%m.json.gz'  -A group_other(aspaths.local,null,<c.top_aspaths>) -A group_other(sites,null,<c.top_sites>) -A group_other(company,null,<c.top_companies>) --arg_join c.top_companies='$(cubes_dir)/company2/months/top_list.%Y-%m.json.gz' --arg_join c.top_sites='$(cubes_dir)/sites2/months/top_list.%Y-%m.json.gz' -O $D {deepy.build.deepy_jobs:CubeBackbone5Min}"

    def get_dimensions(config=None, build_context=None):
        return [
                'timestamp',
                'path',
                'origin_asn.local',
                'origin_asn.remote',
                'pops.local',
                'pops.remote',
                'peer.local',
                'peer.remote',
                'aspaths.local',
                'aspaths.remote',
                'market.local',
                'market.remote',
                'router.local',
                'router.remote',
                'interfaces.local',
                'interfaces.remote',
                'class.local',
                'class.remote',
                'category',
                'cdn',
                'sites'
        ]

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackboneSmallHour(DeepyTimestampExpandedJob, DeepyHourJob):
    """class for the job cube_backbone_small_hour """
    unexpanded_id = "cube_backbone_small_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'backbone_small'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_small/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_small/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A timestep_change(hours) $^"

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackboneAspathsSmall5Min(DeepyTimestampExpandedJob, DeepyJob):
    """class for the job cube_backbone_aspaths_small_5min """
    unexpanded_id = "cube_backbone_aspaths_small_5min_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"
    meta = {'cube_id': 'backbone_aspaths_small'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_aspaths_small/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -t 60 -D $D -A group_other(sites,null,<c.top_sites>) --arg_join c.top_sites='$(cubes_dir)/sites2/months/top_list.%Y-%m.json.gz' $^"

    def get_dimensions(config=None, build_context=None):
        return [
                'timestamp',
                'peer.local',
                'service',
                'category',
                'aspaths.local',
                'sites',
                'origin_asn.local'
        ]

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackboneAsn5Min(DeepyTimestampExpandedJob, DeepyJob):
    """class for the job cube_backbone_asn_5min """
    unexpanded_id = "cube_backbone_asn_5min_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"
    meta = {'cube_id': 'backbone_asn'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_aspaths_small/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -t 60 -D $D -A 'asn_eval_aspath_expand()' $^"

    def get_dimensions(config=None, build_context=None):
        return [
                'timestamp',
                'asn',
                'interconnection',
                'peer.local',
                'service',
                'category',
                'aspaths.local',
                'sites',
                'origin_asn.local'
        ]

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackboneAsnHour(DeepyTimestampExpandedJob, DeepyHourJob):
    """class for the job cube_backbone_asn_hour """
    unexpanded_id = "cube_backbone_asn_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'backbone_asn'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A timestep_change(hours) $^"

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackboneAsnTotal5Min(DeepyTimestampExpandedJob, DeepyJob):
    """class for the job cube_backbone_asn_total_5min """
    unexpanded_id = "cube_backbone_asn_total_5min_%Y-%m-%d-%H-%M"
    file_step = "5min"
    time_step = "5min"
    meta = {'cube_id': 'backbone_asn_total'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_aspaths_small/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn_total/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -t 60 -D $D -A 'asn_eval_interconnection()' $^"

    def get_dimensions(config=None, build_context=None):
        return [
                'timestamp',
                'interconnection',
                'peer.local',
                'service',
                'category',
                'aspaths.local',
                'sites',
                'origin_asn.local'
        ]

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackboneAsn2IndMonth(DeepyTimestampExpandedJob, DeepyCachedJob):
    """class for the job No timestamp or other dimensions, just total for asn """
    unexpanded_id = "cube_backbone_asn2_ind_month_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'backbone_asn2_ind'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn2_ind/months/cube.%Y-%m.h5",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py -S interconnection=Indirect $A -o $@ -D $D $^"

    def get_dimensions(config=None, build_context=None):
        return ['asn']

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackboneAsn2IndMonthTopList(DeepyTimestampExpandedJob, DeepyCachedJob):
    """class for the job Hack to get results to feed back into cube_op.py """
    unexpanded_id = "cube_backbone_asn2_ind_month_top_list_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'backbone_asn2_ind'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn2_ind/months/cube.%Y-%m.h5",
                    "month"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn2_ind/months/top_list.%Y-%m.json.gz",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A measure_fixup() -S asn!=null -A sort(sum.total.bytes,desc,0,100) -A return_dimension_names() -F asn $^"

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackboneAsn2Month(DeepyTimestampExpandedJob, DeepyCachedJob):
    """class for the job No timestamp or other dimensions, just total for asn """
    unexpanded_id = "cube_backbone_asn2_month_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'backbone_asn2'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn2/months/cube.%Y-%m.h5",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -D $D $^"

    def get_dimensions(config=None, build_context=None):
        return ['asn']

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackboneAsn2MonthTopList(DeepyTimestampExpandedJob, DeepyCachedJob):
    """class for the job Hack to get results to feed back into cube_op.py """
    unexpanded_id = "cube_backbone_asn2_month_top_list_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'backbone_asn2'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn2/months/cube.%Y-%m.h5",
                    "month"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn2/months/top_list.%Y-%m.json.gz",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A measure_fixup() -S asn!=null -A sort(sum.total.bytes,desc,0,100) -A return_dimension_names() -F asn $^"

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackboneAsn2MonthTopList200(DeepyTimestampExpandedJob, DeepyCachedJob):
    """class for the job Hack to get results to feed back into cube_op.py """
    unexpanded_id = "cube_backbone_asn2_month_top_list_200_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'backbone_asn2'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn2/months/cube.%Y-%m.h5",
                    "month"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn2/months/top_list_200.%Y-%m.json.gz",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A measure_fixup() -S asn!=null -A sort(sum.total.bytes,desc,0,200) -A return_dimension_names() -F asn $^"

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackboneAsn2IndMonthTopList200(DeepyTimestampExpandedJob, DeepyCachedJob):
    """class for the job Hack to get results to feed back into cube_op.py """
    unexpanded_id = "cube_backbone_asn2_ind_month_top_list_200_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'backbone_asn2'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn2_ind/months/cube.%Y-%m.h5",
                    "month"),
            ],
            "depends_one_or_more": [
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_asn2_ind/months/top_list_200.%Y-%m.json.gz",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -A measure_fixup() -S asn!=null -A sort(sum.total.bytes,desc,0,200) -A return_dimension_names() -F asn $^"

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackbonePeerLocalHour(DeepyTimestampExpandedJob, DeepyHourJob):
    """class for the job cube_backbone_peer_local_hour """
    unexpanded_id = "cube_backbone_peer_local_hour_%Y-%m-%d-%H-%M"
    file_step = "1h"
    time_step = "1h"
    meta = {'cube_id': 'cube_backbone_peer_local'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone/minutes/cube.%Y-%m-%d-%H-%M.h5",
                    "5min"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_peer_local/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -D 'timestamp,peer.local' '-S' 'peer.local!=null' '-A' 'timestep_change(hours)' $^"

    def get_enable(self):
        return self.config.get("has_backbone", False)


class CubeBackbonePeerLocalMonthCostList(DeepyTimestampExpandedJob, DeepyCachedJob):
    """class for the job Monthly peer local traffic and cost values saved off for use by cost apply """
    unexpanded_id = "cube_backbone_peer_local_month_cost_list_%Y-%m-%d-%H-%M"
    file_step = "month"
    time_step = "month"
    meta = {'cube_id': 'cube_backbone_peer_local'}

    def get_dependencies(self, build_context=None):
        return {
            "depends": [
            ],
            "depends_one_or_more": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_peer_local/hours/cube.%Y-%m-%d-%H.h5",
                    "1h"),
            ],
        }

    def get_targets(self, build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    "$(cubes_dir)/backbone_peer_local/months/cost_list.%Y-%m.json.gz",
                    "month"),
            ],
        }

    def get_command(self):
        return "cube_op.py $A -o $@ -D 'peer.local' '-S' 'peer.local!=null' '-A' 'time_dist(months)' '-A' 'costing(peer.local)' '-A' 'return_dimension_names()' '-F' 'peer.local,avg.total.bps,cost,cost_per_mbps' $^"

    def get_enable(self):
        return self.config.get("has_backbone", False)

def __init__(self):
    return
    config = deepy.cfg.slice_config
    config.update(deepy.cfg.vm_config)

    drill_rules = deepy.make.construct_rules()

    new_drill_dict = {}

    for drill_key, drill in drill_rules.iteritems():
        drill_name = drill_key
        drill_name = drill_name.title()
        drill_name = drill_name.replace("_", "")
        unexpanded_id = drill_key + "_%Y-%m-%d-%H-%M"
        file_step = drill.get(
                "file_step", drill.get(
                        "make_time_step", drill.get(
                                "time_step", "5min")))
        time_step = drill.get("time_step", file_step)
        meta = drill.get("meta", {})

        # first pass at adding the dependencies
        # if the dependency doesn't exist yet, the clazz_name is added so that
        # it can later be fixed
        dependencies = {}
        for depends_type in ["depends_one_or_more", "depends"]:
            dependencies[depends_type] = []
            for dependency_name, dependency in drill.get(depends_type, {}).iteritems():
                job_name = dependency_name.title()
                job_name = job_name.replace("_", "")
                clazz_name = "deepy.build.deepy_jobs:" + job_name
                clazz = deepy.util.object_from_string(clazz_name)
                if clazz is None:
                    pass
                    # dependencies[depends_type].append(clazz_name)
                else:
                    depends_targets = clazz.get_targets(config=self.config)
                    depends_targets = depends_targets["produces"]
                    dependencies[depends_type] = (dependencies[depends_type] +
                            depends_targets)

        def get_dependencies_generator(dependencies):
            copy_dependencies = copy.deepcopy(dependencies)
            def get_dependencies(self, build_context=None):
                """returns all the dependencies"""
                if build_context is None:
                    build_context = {}
                return copy_dependencies
            return get_dependencies

        targets = {}
        targets["produces"] = []
        if drill.get("target") is not None:
            expander = builder.expanders.TimestampExpander(
                    builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                    drill.get("target"),
                    file_step)
            targets["produces"].append(expander)

        for query_key, query in drill.get("queries", {}).iteritems():
            target_id = query.get("target")
            if target_id is not None:
                expander = builder.expanders.TimestampExpander(
                        builder.deepy_targets.DeepyS3BackedLocalFileSystemTarget,
                        target_id,
                        file_step)
                targets["produces"].append(expander)

        def get_targets_generator(targets):
            copy_targets = copy.deepcopy(targets)
            def get_targets(self, build_context=None):
                """returns all the targets"""
                if build_context is None:
                    build_context = {}
                return copy_targets
            return get_targets

        if targets["produces"]:
            sub_class = type(
                    drill_name,
                    (DeepyTimestampExpandedJob,
                            DeepyJob),
                    {"get_dependencies": get_dependencies_generator(dependencies),
                            "get_targets": get_targets_generator(targets)})
        else:
            sub_class = type(
                    drill_name,
                    (DeepyTimestampExpandedJob,
                            builder.jobs.MetaJob,
                            DeepyJob),
                    {"get_dependencies": get_dependencies_generator(dependencies),
                          "get_targets": get_targets_generator(targets)})

        sub_class.unexpanded_id = unexpanded_id
        sub_class.file_step = file_step
        sub_class.time_step = time_step
        sub_class.meta = meta

        setattr(self, drill_name, sub_class)

    jobs = globals()
    for job_key, job in jobs.iteritems():
        if inspect.isclass(job):
            dependencies = job.get_dependencies(config=self.config)
            depends_type = ["depends_one_or_more", "depends"]


__init__(sys.modules[__name__])


def get_jobs(config=None):
    """Returns all the jobs in this file"""
    jobs = globals()
    real_jobs = []
    for job_key, job in jobs.iteritems():
        if inspect.isclass(job):
            if issubclass(job, builder.deepy_jobs.DeepyJob):
                real_jobs.append(job)


    return real_jobs
