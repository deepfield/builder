"""The graph file holds logic on how to build out the rule dependency graph
and the build graph
"""

import arrow
import networkx

import deepy.util

import builder.dependencies


class RuleDependencyGraph(networkx.DiGraph):
    """The rule dependency graph holds all the information on how jobs relate to jobs
    and their targets. It also holds information on what their aliases are
    """
    def __init__(self, jobs, config=None):
        super(RuleDependencyGraph, self).__init__()
        if config is None:
            config = {}

        self.jobs = jobs

    def add_node(self, node, attr_dict=None, **attr):
        """Add a job instance, expander instance, or meta node to the graph"""
        if attr_dict is None:
            attr_dict = {}

        node_data = {}
        node_data["object"] = node
        # targets get special coloring
        if isinstance(node, builder.expanders.Expander):
            node_data["style"] = "filled"
            node_data["fillcolor"] = "#C2FFFF"
            node_data["color"] = "blue"

        attr_dict.update(attr)
        attr_dict.update(node_data)

        super(RuleDependencyGraph, self).add_node(node.unexpanded_id,
                attr_dict=attr_dict)

    def add_job(self, job):
        """Used to add a job instance and add anything else about the job"""
        self.add_node(job)
        targets = job.get_targets()
        for target_type, target in targets.iteritems():
            for sub_target in target:
                self.add_node(sub_target)
                self.add_edge(
                    job.unexpanded_id,
                    sub_target.unexpanded_id,
                    label=target_type)

        dependencies = job.get_dependencies()
        for dependency_type, dependency in dependencies.iteritems():
            for sub_dependency in dependency:
                self.add_node(sub_dependency)
                self.add_edge(
                    sub_dependency.unexpanded_id,
                    job.unexpanded_id,
                    label=dependency_type)

    def construct(self):
        """Used to construct the rule dep graph, sets the rule_dep_graph
        attribute
        """
        for job in self.jobs:
            if not job.get_enable():
                continue
            self.add_job(job)

    def write_dot(self, file_name):
        """Writes the rule dependency graph to the file_name"""
        networkx.write_dot(self, file_name)

    def assert_job(self, job_id):
        """Raises a runtime error if the job_id doesn't correspond to a job"""
        job = self.node[job_id]
        if "object" not in job:
            raise RuntimeError("{} is not a job node".format(job_id))
        if not isinstance(job["object"], builder.jobs.Job):
            raise RuntimeError("{} is not a job node".format(job_id))

    def assert_target(self, target_id):
        """Raises a runtime error if the target_id doesn't correspond to a
        target
        """
        target = self.node[target_id]
        if "object" not in target:
            raise RuntimeError("{} is not a target node".format(target_id))
        if not isinstance(target["object"], builder.expanders.Expander):
            raise RuntimeError("{} is not a target node".format(target_id))

    def filter_target_ids(self, target_ids):
        """Takes in a list of ids in the graph and returns all the ids that are
        targets
        """
        output_target_ids = []
        for target_id in target_ids:
            target = self.node[target_id]
            if "object" in target:
                the_object = target["object"]
                if isinstance(the_object, builder.expanders.Expander):
                    output_target_ids.append(target_id)
        return output_target_ids

    def filter_job_ids(self, job_ids):
        """Takes in a list of ids in the graph and returns all the ids that are
        jobs
        """
        output_job_ids = []
        for job_id in job_ids:
            job = self.node[job_id]
            if "object" in job:
                the_object = job["object"]
                if isinstance(the_object, builder.jobs.Job):
                    output_job_ids.append(job_id)
        return output_job_ids

    def get_targets(self, job_id):
        """Returns a list of the ids of all the targets for the job_id"""
        self.assert_job(job_id)
        neighbor_ids = self.neighbors(job_id)
        return self.filter_target_ids(neighbor_ids)

    def get_dependencies(self, job_id):
        """Returns a list of the ids of all the dependency targets for the
        job_id
        """
        self.assert_job(job_id)
        dependency_target_ids = []
        depends_ids = self.predecessors(job_id)
        for depends_id in depends_ids:
            target_ids = self.predecessors(depends_id)
            dependency_target_ids = (dependency_target_ids +
                    self.filter_target_ids(target_ids))
        return dependency_target_ids

    def get_creators(self, target_id):
        """Returns a list of the ids of all the creators for the target_id"""
        self.assert_target(target_id)
        parent_ids = self.predecessors(target_id)
        return self.filter_job_ids(parent_ids)

    def get_dependants(self, target_id):
        """Returns a list of the ids of all the dependants for the target_ids"""
        self.assert_target(target_id)
        dependant_ids = []
        depends_ids = self.neighbors(target_id)
        for depends_id in depends_ids:
            job_ids = self.neighbors(depends_id)
            dependant_ids = dependant_ids + self.filter_job_ids(job_ids)
        return dependant_ids



class Build(networkx.DiGraph):
    """The build object will control the rule dependency graph and the
    build graph"""
    def __init__(self, jobs, config=None):
        super(Build, self).__init__()
        if config is None:
            config = {}

        self.jobs = jobs
        self.rule_dep_graph = None
        self.config = config
        self.time = arrow.get()
        self.count = 0
        self.cache_count = 0

    def write_rule_dep_graph(self, file_name):
        """Old"""
        if self.rule_dep_graph is None:
            self.rule_dep_graph = RuleDependencyGraph()
            self.rule_dep_graph.construct()

        self.rule_dep_graph.write_dot(file_name)

    def write_build_graph(self, file_name):
        """Writes the build graph to the file_name"""
        networkx.write_dot(self, file_name)

    def construct_rule_dependency_graph(self):
        """Old"""
        self.rule_dep_graph = RuleDependencyGraph(self.jobs)
        self.rule_dep_graph.construct()

    def add_node(self, node, attr_dict=None, **kwargs):
        """Adds an expanded node to the graph"""
        if attr_dict is None:
            attr_dict = {}

        node_data = {}
        if node.unique_id in self:
            node_data = self.node[node.unique_id]
            node = self.node[node.unique_id]["object"]

        if isinstance(node, builder.targets.Target):
            node_data["style"] = "filled"
            node_data["fillcolor"] = "#C2FFFF"
            node_data["color"] = "blue"
        node_data.update(attr_dict)
        node_data.update(kwargs)
        node_data["object"] = node

        super(Build, self).add_node(node.unique_id, attr_dict=node_data)
        return node

    def _connect_targets(self, node, target_type, targets):
        """Connets the node to it's targets"""
        for target in targets:
            target = self.add_node(target)
            self.add_edge(node.unique_id, target.unique_id,
                                      label=target_type)

    def _expand_targets(self, node):
        """Expands out the targets of the node"""
        if node.expanded_directions["down"]:
            next_nodes = []
            for node_id in self.neighbors(node.unique_id):
                next_nodes.append(self.node[node_id]["object"])
            return next_nodes

        expanded_target_list = []
        unexpanded_node = (self.rule_dep_graph
                                 .node[node.unexpanded_id]["object"])
        targets = unexpanded_node.get_targets(build_context=node.build_context)

        for target_type, target_group in targets.iteritems():
            for target in target_group:
                expanded_targets = target.expand(node.build_context)
                expanded_target_list = (expanded_target_list +
                                        expanded_targets)
                self._connect_targets(node, target_type, expanded_targets)

        node.expanded_directions["down"] = True
        return expanded_target_list

    def _connect_dependencies(self, node, dependency_type, dependencies, data):
        """connects the node to all of the dependencies"""
        dependency_node_id = "{}_{}_{}".format(
            node.unique_id, dependency_type.func_name,
            "_".join([x.unique_id for x in dependencies]))

        dependency = builder.dependencies.Dependency(
                dependency_type, dependency_node_id)

        self.add_node(dependency, label=dependency_type.func_name)

        self.add_edge(dependency_node_id, node.unique_id, data,
                                  label=dependency_type.func_name)

        for dependency in dependencies:
            dependency = self.add_node(dependency)
            self.add_edge(
                dependency.unique_id, dependency_node_id, data,
                label=dependency_type.func_name)

    def _expand_dependencies(self, node):
        """Expands out the dependencies of the node"""
        if node.expanded_directions["up"]:
            next_nodes = []
            for depends_id in self.predecessors(node.unique_id):
                for node_id in self.predecessors(depends_id):
                    next_nodes.append(self.node[node_id]["object"])
            return next_nodes

        expanded_dependency_list = []
        unexpanded_node = (self.rule_dep_graph
                               .node[node.unexpanded_id]["object"])
        dependencies = unexpanded_node.get_dependencies(
                build_context=node.build_context)

        for dependency_type, dependency_group in dependencies.iteritems():
            dependency_type = (builder.dependencies
                               .get_dependencies(dependency_type))
            for dependency in dependency_group:
                build_context = node.build_context
                edge_data = dependency.edge_data
                expanded_dependencies = dependency.expand(build_context)
                self._connect_dependencies(node, dependency_type,
                                           expanded_dependencies, edge_data)
                expanded_dependency_list = (expanded_dependency_list +
                                            expanded_dependencies)

        node.expanded_directions["up"] = True
        return expanded_dependency_list

    def _self_expand_down_next(self, node, expanded_targets,
                               depth, current_depth, top_jobs, cache_set):
        """Gets the next nodes to expand and expands them"""
        next_nodes = []
        for expanded_target in expanded_targets:
            if expanded_target.unique_id in cache_set:
                continue
            if expanded_target.expanded_directions["down"]:
                for node_id in self.neighbors(node.unique_id):
                    next_nodes.append(self.node[node_id]["object"])
                continue
            neighbor_ids = self.rule_dep_graph.get_dependants(
                expanded_target.unexpanded_id)
            for node_id in neighbor_ids:
                node = self.rule_dep_graph[node_id]
                expanded_nodes = node.expand(
                    expanded_target.build_context)
                next_nodes = next_nodes + expanded_nodes
            cache_set.add(expanded_target.unique_id)
            expanded_target.expanded_directions["down"] = True
        for expanded_node in next_nodes:
            self._self_expand(expanded_node, "down", depth, current_depth,
                              top_jobs, cache_set)
        return next_nodes

    def _self_expand_up_next(self, node, expanded_dependencies,
                             depth, current_depth, top_jobs, cache_set):
        """Gets the next nodes to expand and expands them"""
        next_nodes = []
        for expanded_dependency in expanded_dependencies:
            if expanded_dependency.unique_id in cache_set:
                continue
            if expanded_dependency.expanded_directions["up"]:
                parent_node_ids = self.predecessors(
                    expanded_dependency.unique_id)
                for node_id in parent_node_ids:
                    next_nodes.append(self.node[node_id]["object"])
                continue
            parent_ids = self.rule_dep_graph.get_creators(
                expanded_dependency.unexpanded_id)
            for parent_node_id in parent_ids:
                parent_node = (self.rule_dep_graph
                                   .node[parent_node_id]["object"])
                expanded_nodes = parent_node.expand(
                    expanded_dependency.build_context)
                next_nodes = next_nodes + expanded_nodes
            cache_set.add(expanded_dependency.unique_id)
            expanded_dependency.expanded_directions["up"] = True
        for expanded_node in next_nodes:
            self._self_expand(
                expanded_node, "up", depth, current_depth, top_jobs, cache_set)
        return next_nodes

    def _self_expand(self, node, direction, depth, current_depth, top_jobs,
                     cache_set=None):
        """Input a node to expand and a build_context, magic ensues"""
        if cache_set is None:
            cache_set = set([])

        if node.unique_id in cache_set:
            return

        self.add_node(node)

        expanded_targets = self._expand_targets(node)
        expanded_dependencies = self._expand_dependencies(node)
        cache_set.add(node.unique_id)

        next_nodes = []
        if depth is not None:
            if not isinstance(node, builder.jobs.MetaJobState):
                current_depth = current_depth + 1
            if current_depth >= depth:
                top_jobs.add(node.unique_id)
                return

        if direction == "down":
            next_nodes = self._self_expand_down_next(node, expanded_targets,
                                                     depth, current_depth,
                                                     top_jobs, cache_set)
            if not next_nodes:
                top_jobs.add(node.unique_id)
        else:
            next_nodes = self._self_expand_up_next(node, expanded_dependencies,
                                                   depth, current_depth,
                                                   top_jobs, cache_set)


    def construct_build_graph(self, build_context):
        """Used to construct up a build graph using a build_context

        takes in a start job from the build_context and follows a path in the
        rule dependency graph, expanding every node that it hits and expanding
        up the following nodes in the path

        valid build_context:
            requires:
                start_node: the class name of the node to start with
                start_node_start: the values that the start node require as
                    well as any that may come after

            optional:
                exact:
                    (default) False
                    (possible) True, False
                    True sets the depth to 0, overrides depth
                    False does nothing
                direction:
                    XXX
                    (not implemented) (default) up
                    (possible) down, exact
                    the direction to traverse in the graph
                depth:
                    (default) None
                    (possible) integer value
                    the maximum number of jobs to traverse
                start_time:
                    (default) arrow.get()
                    (possible) any arrow timestamp
                    the first timestamp floored to expand on
                end_time:
                    (default) arrow.get()
                    (possible) any arrow timestmap
                    the end timestamp floored to expand on
                    if end_time is before start_time it is ignored
                range_num:
                    (default) None
                    (possible) integer value
                    the number of timesteps - 1 to add on before start_time
                force:
                    (default) False
                    (possible) boolean
                    the specifier for forcing a job. The top most job recieves
                    this property
        """
        if self.rule_dep_graph is None:
            self.construct_rule_dependency_graph()

        current_depth = 0
        if build_context.get("exact", False):
            depth = 1
        else:
            depth = build_context.get("depth", None)

        if build_context.get("start_time") is None:
            build_context["start_time"] = arrow.get()
        if build_context.get("end_time") is None:
            build_context["end_time"] = build_context["start_time"]
        if build_context.get("range_num") is not None:
            file_step = self.rule_dep_graph.node[build_context.get("start_job")]["object"].file_step
            timedelta = deepy.timerange.convert_to_timedelta(file_step)
            timedelta = timedelta * (build_context.get("range_num") - 1)
            start_datetime = build_context["start_time"].datetime
            start_datetime = start_datetime - timedelta
            build_context["start_time"] = arrow.get(start_datetime)
        if build_context.get("force") is None:
            build_context["force"] = False

        unexpanded_id = build_context["start_job"]
        del build_context["start_job"]
        start_node = (self.rule_dep_graph
                          .node
                          [unexpanded_id]["object"])
        expanded_nodes = start_node.expand(build_context)
        cache_set = set([])
        top_jobs = set([])
        for expanded_node in expanded_nodes:
            self._self_expand(expanded_node, "up", depth, current_depth,
                              top_jobs, cache_set=cache_set)

        # the top_jobs returned by _self_expand is a unintelligent top_job
        # some top_jobs may be sibilings of other top_jobs. This will remove
        # all top_jobs that are sibilings of other top_jobs
        self.remove_sibilings(top_jobs)
        if build_context["force"]:
            for top_job_id in top_jobs:
                top_job = self.node[top_job_id]["object"]
                top_job.build_context["force"] = True

    def _remove_sibilings_recurse(self, job_id, top_jobs, cache_set):
        if job_id in cache_set:
            return
        top_jobs.discard(job_id)
        cache_set.add(job_id)
        for target_id in self.neighbors(job_id):
            for depends_id in self.neighbors(target_id):
                for next_job_id in self.neighbors(depends_id):
                    self._remove_sibilings_recurse(
                        next_job_id, top_jobs, cache_set)

    def remove_sibilings(self, top_jobs):
        cache_set = set([])
        for top_job in top_jobs.copy():
            for target_id in self.neighbors(top_job):
                for depends_id in self.neighbors(target_id):
                    for job_id in self.neighbors(depends_id):
                        self._remove_sibilings_recurse(
                            job_id, top_jobs, cache_set)

    def get_starting_jobs(self):
        """Used to return a list of jobs to run"""
        should_run_list = []
        for _, node in self.node.iteritems():
            job = node["object"]
            if isinstance(job, builder.jobs.JobState):
                if job.get_should_run(self):
                    should_run_list.append(job)
        return should_run_list

    def get_next_jobs_to_run(self, job_id, update_set=None):
        """Returns the jobs that are below job_id that need to run"""
        if update_set is None:
            update_set = set([])

        if job_id in update_set:
            return []

        next_jobs_list = []

        job = self.node[job_id]["object"]
        if job.get_should_run(self):
            next_jobs_list.append(job_id)
            update_set.add(job_id)
            return next_jobs_list

        target_ids = self.neighbors(job_id)
        for target_id in target_ids:
            depends_ids = self.neighbors(target_id)
            for depends_id in depends_ids:
                neighbor_job_ids = self.neighbors(depends_id)
                for neighbor_job_id in neighbor_job_ids:
                    next_jobs = self.get_next_jobs_to_run(
                        neighbor_job_id, update_set=update_set)
                    next_jobs_list = next_jobs_list + next_jobs

        update_set.add(job_id)

        return next_jobs_list

    def update_job_cache(self, job_id):
        """Updates the cache due to a job finishing"""
        target_ids = self.neighbors(job_id)
        for target_id in target_ids:
            target = self.node[target_id]["object"]
            target.get_mtime(cached=False)

        job = self.node[job_id]["object"]
        job.get_stale(self, cached=False)

        for target_id in target_ids:
            depends_ids = self.neighbors(target_id)
            for depends_id in depends_ids:
                neighbor_job_ids = self.neighbors(depends_id)
                for neighbor_job_id in neighbor_job_ids:
                    neighbor_job = (self.node
                                    [neighbor_job_id]
                                    ["object"])
                    neighbor_job.get_buildable(self, cached=False)
                    neighbor_job.get_stale(self, cached=False)

        job.update_lower_nodes_should_run(self)

    def update_target_cache(self, target_id):
        """Updates the cache due to a target finishing"""
        target = self.node[target_id]["object"]
        target.get_mtime(cached=False)

        depends_ids = self.neighbors(target_id)
        for depends_id in depends_ids:
            job_ids = self.neighbors(depends_id)
            for job_id in job_ids:
                job = self.node[job_id]["object"]
                job.get_stale(self, cached=False)
                job.get_buildable(self, cached=False)
                job.update_lower_nodes_should_run(self)

    def finish(self, job_id):
        """Checks what should happen now that the job is done"""
        self.update_job_cache(job_id)
        next_jobs = self.get_next_jobs_to_run(job_id)
        for next_job in next_jobs:
            self.run(next_job)

    def update(self, target_id):
        """Checks what should happen now that there is new information
        on a target
        """
        self.update_target_cache(target_id)
        producer_ids = self.predecessors(target_id)
        producers_exist = False
        for producer_id in producer_ids:
            producers_exist = True
            next_jobs = self.get_next_jobs_to_run(producer_id)
            for next_job in next_jobs:
                self.run(next_job)
        if producers_exist == False:
            for depends_id in self.neighbors(target_id):
                for job_id in self.neighbors(depends_id):
                    next_jobs = self.get_next_jobs_to_run(job_id)
                    for next_job in next_jobs:
                        self.run(next_job)

    def run(self, job_id):
        """For now returns immediatlly"""
        job_id = job_id
        return
