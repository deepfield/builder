"""Used to implement the basic framework around job nodes. Job nodes are
the nodes that can be called and will perform an action.
"""

import arrow

import builder.expanders
import builder.targets
import deepy.timerange

class JobState(object):
    """A job state is basically a job in the build graph. It is used to keep
    state on the specific job
    """
    def __init__(self, unexpanded_id, unique_id, build_context, command,
            cache_time, config=None):
        if config is None:
            config = {}

        self.unexpanded_id = unexpanded_id
        self.unique_id = unique_id
        self.build_context = build_context
        self.command = command
        self.cache_time = cache_time
        self.config = config

        self.stale = None
        self.buildable = None
        self.should_run = None
        self.parents_should_not_run = None
        self.expanded_directions = {"up": False, "down": False}

    def __str__(self):
        return "{}:{}".format(self.unexpanded_id, self.unique_id)

    def __repr__(self):
        return "{}:{} {}".format(self.unexpanded_id, self.unique_id, self.command)

    def get_stale_alternates(self, build_graph):
        """Returns True if the job does not have an alternate or if any
        of it's alternates don't exist
        """
        alt = False
        alternate_edges = build_graph.out_edges(self.unique_id, data=True)
        for alt_edge in alternate_edges:
            if alt_edge[2]["label"] == "alternates":
                alt = True
                alternate_id = alt_edge[1]
                alternate = (build_graph.node[alternate_id]["object"])
                if not alternate.get_exists():
                    return True
        return not alt

    def update_stale(self, new_value, build_graph):
        """Updates the stale value of the node and then updates all the above
        nodes.

        This is needed due to alternates. If the job above this job has an
        alternate that is this job's target, then the above job may not be
        stale when it's target doesn't exist.
        If this job is stale then it needs the targert from the above job.
        Therefore this job will then tell the above job that it needs to be
        stale. That is the goal of this function.

        If the new value is True and the old value was not True then
        everything above it is updated.
        Updating the above involves looking at all the dependencies.
        If a dependency doesn't exist, then it updates the job of the
        dependency to stale
        """
        if new_value == True and self.stale != True:
            self.stale = new_value
            for depends_node_id in build_graph.predecessors(self.unique_id):
                for dependency_id in build_graph.predecessors(depends_node_id):
                    dependency = build_graph.node[dependency_id]["object"]
                    if not dependency.get_exists():
                        in_edges = build_graph.in_edges(
                            dependency_id, data=True)
                        for in_edge in in_edges:
                            if in_edge[2]["label"] == "produces":
                                (build_graph.node[in_edge[0]]["object"]
                                 .update_stale(True, build_graph))
        self.stale = new_value

    def get_minimum_target_mtime(self, build_graph):
        """Returns the minimum target mtime or returns True if one
        is missing

        A target is considered missing if there is a missing target and a
        missing alternate. The alternate does not need to be corresponding
        to the target

        Returns:
            True: if a target is missing
            Minimum mtime: the lowest mtime of the targets
        """
        alt_check = False
        target_mtimes = [float("inf")]
        for target_edge in build_graph.out_edges(self.unique_id, data=True):
            # edge is form (src_node_id, dest_node_id, data_dict)
            if target_edge[2]["label"] == "produces":
                if target_edge[2].get("ignore_mtime", False):
                    continue
                target_id = target_edge[1]
                target = build_graph.node[target_id]["object"]
                if not target.get_exists() and not alt_check:
                    if self.get_stale_alternates(build_graph):
                        return True
                else:
                    target_mtimes.append(target.get_mtime())
        min_target_mtime = min(target_mtimes)
        return min_target_mtime

    def get_maximum_dependency_mtime(self, build_graph, minimum_target_mtime):
        """Returns True if a dependency mtime is greater than the
        minimum_target_mtime
        """
        for in_edge in build_graph.in_edges(self.unique_id, data=True):
            if in_edge[2].get("ignore_mtime", False):
                continue
            dependency_node_id = in_edge[0]
            for dependency_id in build_graph.predecessors(dependency_node_id):
                dependency = build_graph.node[dependency_id]["object"]
                if dependency.get_exists():
                    if dependency.get_mtime() > minimum_target_mtime:
                        return True
        return False

    def get_stale(self, build_graph, cached=True):
        """Returns whether or not the job needs to run to update it's output

        Often this job will look at the mtime of it's inputs and it's outputs
        and determine if the job needs to run

        Stale conditions:
            The job has been updated to stale with update_stale
            A target doesn't exist and the job doesn't have an alternate
            A target doesn't exist and a single alternate doesn't exist
            A target's mtime is lower than a dependency's mtime
        """
        if cached and self.stale != None:
            return self.stale
        if not self.past_cache_time(build_graph):
            self.stale = False
            return False

        minimum_target_mtime = self.get_minimum_target_mtime(build_graph)
        if minimum_target_mtime is True:
            self.update_stale(True, build_graph)
            return True

        greater_mtime = self.get_maximum_dependency_mtime(
            build_graph, minimum_target_mtime)
        if greater_mtime:
            self.update_stale(True, build_graph)
            return True

        self.update_stale(False, build_graph)
        return False

    def get_buildable(self, build_graph, cached=True):
        """Returns whether or not the job is buildable

        Buildability is true when all the depends are met. This is true when
        all of the depends node's return True

        Buildable conditions:
            All the above dependency nodes return true
        """
        if cached and self.buildable is not None:
            return self.buildable

        for dependency_node_id in build_graph.predecessors(self.unique_id):
            dependency_node = build_graph.node[dependency_node_id]
            dependency_func = dependency_node["object"].func
            buildable_ids = build_graph.predecessors(dependency_node_id)
            buildable_nodes = []
            for buildable_id in buildable_ids:
                buildable_nodes.append(
                    build_graph.node[buildable_id]["object"])
            buildable = dependency_func(buildable_nodes)
            if not buildable:
                self.buildable = False
                return False

        self.buildable = True
        return True

    def past_cache_time(self, build_graph):
        """Returns true if the job is past it's cache time

        This implementation returns true if the oldest mtime is older than
        the cache_time or if non of the targets exist
        """
        cache_time = self.cache_time
        if cache_time is None:
            return True
        cache_delta = deepy.timerange.convert_to_timedelta(cache_time)
        current_time = arrow.get()
        for target_edge in build_graph.out_edges(self.unique_id, data=True):
            if target_edge[2]["label"] == "produces":
                target = build_graph.node[target_edge[1]]["object"]
                if not target.get_exists():
                    return True
                elif arrow.get(target.get_mtime()) + cache_delta < current_time:
                    return True
        return False

    def all_dependencies(self, build_graph):
        """Returns whether or not all the jobs dependencies exist"""
        for depends_node_id in build_graph.predecessors(self.unique_id):
            for dependency_id in build_graph.predecessors(depends_node_id):
                dependency = build_graph.node[dependency_id]["object"]
                if not dependency.get_exists():
                    return False
        return True

    def past_curfew(self):
        """Returns whether or not the job is past it's curfew

        True by default
        """
        return True

    def get_parent_jobs(self, build_graph):
        """Returns a list of all the parent jobs"""
        parent_jobs = []
        for depends_node_id in build_graph.predecessors(self.unique_id):
            for dependency_id in build_graph.predecessors(depends_node_id):
                parent_jobs = (parent_jobs +
                               build_graph.predecessors(dependency_id))
        return parent_jobs

    def update_lower_nodes_should_run(self, build_graph, cache_set=None,
                                      update_set=None):
        """Updates whether or not the job should run based off the new
        information on the referrer
        """
        if update_set is None:
            update_set = set([])

        if self.unique_id in update_set:
            return

        self.get_should_run(build_graph, cached=False, cache_set=cache_set)
        for target_id in build_graph.neighbors(self.unique_id):
            for depends_id in build_graph.neighbors(target_id):
                for job_id in build_graph.neighbors(depends_id):
                    job = build_graph.node[job_id]["object"]
                    job.update_lower_nodes_should_run(
                        build_graph, cache_set=cache_set,
                        update_set=update_set)

        update_set.add(self.unique_id)

    def get_parents_should_not_run(self, build_graph, cache_time,
                                   cached=True, cache_set=None):
        """Returns whether or not any contiguous ancestor job with the
        same cache_time bool value should run

        False if an ancestor should run
        True if no ancestor should run
        """
        if cached and self.parents_should_not_run is not None:
            return self.parents_should_not_run

        if cache_set is None:
            cache_set = set([])
        if (self.unique_id in cache_set and
                self.parents_should_not_run is not None):
            return self.parents_should_not_run

        for dependency_id in self.get_parent_jobs(build_graph):
            dependency = build_graph.node[dependency_id]["object"]
            has_cache_time = dependency.cache_time is not None
            if has_cache_time == cache_time:
                if (not dependency.get_parents_should_not_run(
                        build_graph, has_cache_time, cached=cached,
                        cache_set=cache_set) or
                        dependency.get_should_run_immediate(
                        build_graph, cached=cached)):
                    self.parents_should_not_run = False
                    cache_set.add(self.unique_id)
                    return False

                if not dependency.get_parents_should_not_run(
                        build_graph, cache_time, cached=cached,
                        cache_set=cache_set):
                    cache_set.add(self.unique_id)
                    return False

        cache_set.add(self.unique_id)
        self.parents_should_not_run = True
        return True

    def get_should_run_immediate(self, build_graph, cached=True):
        """Returns whether or not the node should run not caring about the
        ancestors should run status
        """
        if self.build_context.get("force", False):
            return True
        if cached and self.should_run is not None:
            return self.should_run

        has_cache_time = self.cache_time is not None
        self.get_stale(build_graph)
        self.get_buildable(build_graph)
        if (not self.get_stale(build_graph) or
                not self.get_buildable(build_graph)):
            self.should_run = False
            return False

        if (has_cache_time or
                self.past_curfew() or
                self.all_dependencies(build_graph)):
            self.should_run = True
            return True
        self.should_run = False
        return False

    def get_should_run(self, build_graph, cached=True, cache_set=None):
        """Returns whether or not the job should run

        depends on it's current state and whether or not it's ancestors
        should run
        """
        should_run_immediate = self.get_should_run_immediate(
            build_graph, cached=cached)

        cache_time = self.cache_time is not None

        parents_should_not_run = self.get_parents_should_not_run(
            build_graph, cache_time, cached=cached, cache_set=cache_set)

        return should_run_immediate and parents_should_not_run

    def get_command(self, build_graph):
        """Returns the job's expanded command"""
        return self.command

class MetaJobState(JobState):
    def get_should_run_immediate(self, build_graph, cache=True):
        return False

    def get_should_run(self, build_graph, cached=True, cache_set=None):
        return False

class TimestampExpandedJobState(JobState):
    def __init__(self, unexpanded_id, unique_id, build_context, command,
            cache_time, curfew, config=None):
        super(TimestampExpandedJobState, self).__init__(unexpanded_id,
                unique_id, build_context, command, cache_time, config=config)
        self.curfew = curfew

    def past_curfew(self):
        time_delta = deepy.timerange.convert_to_timedelta(self.curfew)
        end_time = self.build_context["end_time"]
        curfew_time = end_time + time_delta
        return curfew_time < arrow.get()


class Job(object):
    """A job"""
    unexpanded_id = "job"
    cache_time = None

    def __init__(self, config=None):
        if config is None:
            config = {}
        self.config = config

    def get_expandable_id(self):
        """Returns the unexpanded_id with any expansion neccessary information
        appended
        """
        return self.unexpanded_id

    def get_job_state(self):
        """Returns the type of state to use for expansions"""
        return JobState

    def expand(self, build_context):
        """Used to expand the node using a build context returns a list of
        nodes

        a typical expansion is a timestamp expansion where build
        context would use start time and end time and the node
        would expand from there
        """
        state_type = self.get_job_state()
        return [
            state_type(self.unexpanded_id, self.get_expandable_id(),
                     build_context, self.get_command(),
                     self.cache_time)]

    def get_enable(self):
        """Used to determine if the node should end up in the build graph
        or not. For example, when the deployment doesn't have backbone
        no backbone node should be in the graph
        """
        return True

    def get_command(self):
        """Used to get the command related to the command"""
        return "base command for " + self.unexpanded_id

    def get_dependencies(self, build_context=None):
        """most jobs will depend on the existance of a file, this is what is
        returned here. It is in the form
        {
            "dependency_type": [
                dependency_class,
            ],
        }
        """
        if build_context is None:
            build_context = {}
        return {}

    def get_targets(self, build_context=None):
        """most jobs will output a target, specify them here
        form:
            {
                "target_type": [
                    target_class
                ],
            }
        """
        if build_context is None:
            build_context = []
        return {}


class MetaJob(Job):
    """A job that should never run"""
    def get_state_type(self):
        return MetaJobState

    def expand(self, build_context):
        job_type = self.get_job_state()
        return [job_type(self.unexpanded_id, self.unexpanded_id,
                build_context, self.get_command(), self.cache_time)]


class TimestampExpandedJob(Job):
    """A job that combines the timestamp expadned node and the job node
    logic
    """
    curfew = "10min"
    file_step = "5min"

    def get_expandable_id(self):
        return self.unexpanded_id + "_%Y-%m-%d-%H-%M"

    def get_state_type(self):
        return TimestampExpandedJobState

    def expand(self, build_context):
        """Expands the node based off of the file step and the start and
        end times
        """
        job_type = self.get_state_type()
        expanded_contexts = (builder.expanders.TimestampExpander.expand_build_context(
            build_context, self.get_expandable_id(), self.file_step))

        expanded_nodes = []
        for expanded_id, build_context in expanded_contexts.iteritems():
            expanded_node = job_type(self.unexpanded_id,
                expanded_id, build_context, self.get_command(),
                self.cache_time, self.curfew, config=self.config)
            expanded_nodes.append(expanded_node)

        return expanded_nodes

