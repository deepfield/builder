"""The graph file holds logic on how to build out the rule dependency graph
and the build graph
"""

import arrow
import networkx

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
        self.config = config

    def add_node(self, node, attr_dict=None, **attr):
        """Add a job instance, expander instance, or meta node to the graph

        The node is added as the "object" keyword to the node. Some defaults are
        given to the node but can be overwritten by attr_dict, anything in attr
        overwrites that

        Args:
            node: The object to add to the "object" value
            attr_dict: Node attributes, same as attr_dict for a normal networkx
                graph overwrites anything that is defaulted, can even overwrite
                node
            attr: overwrites anything that is defaulted, can even overwrite node
                and attr_dict
        """
        if attr_dict is None:
            attr_dict = {}

        node_data = {}
        node_data["object"] = node
        # targets get special coloring
        if isinstance(node, builder.expanders.Expander):
            node_data["style"] = "filled"
            node_data["fillcolor"] = "#C2FFFF"
            node_data["color"] = "blue"

        node_data.update(attr)
        node_data.update(attr_dict)

        super(RuleDependencyGraph, self).add_node(node.unexpanded_id,
                attr_dict=node_data)

    def add_job(self, job):
        """Adds a job and it's targets and dependencies to the rule dependency
        graph

        Using the job class passed in, a job node, and nodes for each of the
        targets specified by get_targets and get_dependencies is added. The
        resulting nodes have a job as the job node's object and a expander as the
        target nodes' object.

        Args:
            job: the job to add to the rule dependency graph
        """
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
        """Constructs the rule dependency graph.

        Adds all the jobs that are specified by the jobs keyword to the graph
        """
        for job in self.jobs:
            if not job.get_enable():
                continue
            self.add_job(job)

    def write_dot(self, file_name):
        """Writes the rule dependency graph to the file_name

        Currently does not modify the graph in anyway before writing out
        """
        networkx.write_dot(self, file_name)

    def assert_job(self, job_id):
        """Raises a runtime error if the job_id doesn't correspond to a job.

        Checks the node with id job_id and then raises and error if there is no
        object in the node or the object is not a job

        Args:
            job_id: the id of the node to check

        Returns:
            None

        Raises:
            RuntimeError: raised if the node specified is not a job node
        """
        job = self.node[job_id]
        if "object" not in job:
            raise RuntimeError("{} is not a job node".format(job_id))
        if not isinstance(job["object"], builder.jobs.Job):
            raise RuntimeError("{} is not a job node".format(job_id))

    def assert_target(self, target_id):
        """Raises a runtime error if the target_id doesn't correspond to a
        target

        Checks the node with id target_id and then raises an error if there is
        no object in the node or the object is not an Expander

        Args:
            target_id: the id of the node to check

        Returns:
            None

        Raises:
            RuntimeError: raised if the node specified is not a target node
        """
        target = self.node[target_id]
        if "object" not in target:
            raise RuntimeError("{} is not a target node".format(target_id))
        if not isinstance(target["object"], builder.expanders.Expander):
            raise RuntimeError("{} is not a target node".format(target_id))

    def filter_target_ids(self, target_ids):
        """Takes in a list of ids in the graph and returns a list of the ids
        that correspond to targets

        An id is considered to be a target id if the object in the node
        specified by the id is an instance of Expander

        Args:
            target_ids: A list of ids that are potentially targets

        Returns:
            A filtered list of target_ids where only the id's corresponding to
            target nodes are left.
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
        """Takes in a list of ids in the graph and returns a list of ids that
        correspond to jobs

        An id is considered to be a job id if the object in the node specified
        by the id is an instance of Job

        Args:
            job_ids: A list of ids that are potentially jobs

        Returns:
            A filtered list of job_ids where only the id's corresponding to job
            nodes are left.
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
        """Returns a list of the ids of all the targets for the job_id

        The targets for the job_id are the target nodes that are direct
        decendants of job_id

        Args:
            job_id: The job to return the targets of

        Returns:
            A list of ids corresponding to the targets of job_id
        """
        self.assert_job(job_id)
        neighbor_ids = self.neighbors(job_id)
        return self.filter_target_ids(neighbor_ids)

    def get_dependencies(self, job_id):
        """Returns a list of the ids of all the dependency targets for the
        job_id

        The dependencies for the job_id are the target nodes that are direct
        predecessors of job_id

        Args:
            job_id: The job to return the targets of

        Returns:
            A list of ids corresponding to the dependencies of job_id
        """
        self.assert_job(job_id)
        target_ids = self.predecessors(job_id)
        dependency_target_ids = self.filter_target_ids(target_ids)
        return dependency_target_ids

    def get_creators(self, target_id):
        """Returns a list of the ids of all the creators for the target_id

        The creators of a target are all direct predecessors of the target

        Args:
            target_id: The target_id to return the creators of

        Returns:
            A list of ids corresponding to the creators of the target_id
        """
        self.assert_target(target_id)
        parent_ids = self.predecessors(target_id)
        return self.filter_job_ids(parent_ids)

    def get_dependants(self, target_id):
        """Returns a list of the ids of all the dependants for the target_ids

        The dependants of a target are all the direct decendants of the target

        Args:
            target_id: The target_id to return the dependants of

        Returns:
            A list of ids corresponding to the dependants of the target_id
        """
        self.assert_target(target_id)
        job_ids = self.neighbors(target_id)
        dependant_ids = self.filter_job_ids(job_ids)
        return dependant_ids

    def get_dependants_or_creators(self, target_id, direction):
        """Returns the dependants or the creators of the targets depending on
        the direction

        direction can be up (creators) down (dependants)

        Args:
            target_id: the target to return the dependants or creators of
            direction: The direction that the returned nodes will be to the
                target_id
        """
        if direction == "up":
            return self.get_creators(target_id)
        else:
            return self.get_dependants(target_id)

    def get_job(self, job_id):
        """Returns the object corresponding to the job_id

        The object corresponding to the job_id is the object keyword of the node
        with the id job_id

        Args:
            job_id: the id of the node holding the job

        Returns:
            the object in the object keyword for the node corresponding to
            job_id
        """
        return self.node[job_id]['object']


class BuildGraph(networkx.DiGraph):
    """The build object will control the rule dependency graph and the
    build graph"""
    def __init__(self, jobs, config=None):
        super(BuildGraph, self).__init__()
        if config is None:
            config = {}

        self.jobs = jobs
        self.rule_dep_graph = None
        self.config = config
        self.time = arrow.get()
        self.count = 0
        self.cache_count = 0

    def write_rule_dep_graph(self, file_name):
        """Ensures the rule dep graph exists and then writes it to file_name

        If the rule dep graph doesn't exist then it is constructed. After the
        graph is constructed it's write_dot is called

        Args:
            file_name: the name fo the file to write the dot file to
        """
        if self.rule_dep_graph is None:
            self.rule_dep_graph = RuleDependencyGraph()
            self.rule_dep_graph.construct()

        self.rule_dep_graph.write_dot(file_name)

    def write_dot(self, file_name):
        """Writes the build graph to the file_name.

        Does not ensure that the graph is built. It write's it in what ever
        state the graph is currently in

        Args:
            file_name: the name for the file to write the dot file to"""
        networkx.write_dot(self, file_name)

    def construct_rule_dependency_graph(self):
        """Builds a rule dependency graph using the same jobs as the build_graph

        Uses the jobs that the build_graph will be made off of to make the rule
        dependency graph.
        """

        self.rule_dep_graph = RuleDependencyGraph(self.jobs)
        self.rule_dep_graph.construct()
        return self.rule_dep_graph

    def add_node(self, node, attr_dict=None, **kwargs):
        """Adds a jobstate, target, dependency node to the graph

        A node is added to the graph where the object keyword of the node will
        be node and the other keywords will be defined by the defaults, kwargs,
        and attr_dict. The id of the added node is defined by the unique id of
        node.

        If the node already is in the graph, then the new node data updates the
        data of the old node.

        Args:
            node: the node to add to the build_graph
            attr_dict: a dict of node data, will overwrite the default values.
                Can even overwrite the object value
            kwrags: the remaining attributes are considered to be node data.
                Will overwrite the default values. Can also overwrite attr_dict
                and the object value
        """
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

        super(BuildGraph, self).add_node(node.unique_id, attr_dict=node_data)
        return node

    def assert_job(self, job_id):
        """Raises a runtime error if the job_id doesn't correspond to a job.

        Checks the node with id job_id and then raises and error if there is no
        object in the node or the object is not a JobState

        Args:
            job_id: the id of the node that should be a JobState

        Returns:
            None

        Raises:
            RuntimeError: raised if the node specified is not a JobState node
        """
        job = self.node[job_id]
        if "object" not in job:
            raise RuntimeError("{} is not a job node".format(job_id))
        if not isinstance(job["object"], builder.jobs.JobState):
            raise RuntimeError("{} is not a job node".format(job_id))

    def assert_target(self, target_id):
        """Raises a runtime error if the target_id doesn't correspond to a
        target

        Checks the node with id target_id and then raises an error if there is
        no object in the node or the object is not an Target

        Args:
            target_id: the id of the node that should be a Target node

        Returns:
            None

        Raises:
            RuntimeError: raised if the node specified is not a Target node
        """
        target = self.node[target_id]
        if "object" not in target:
            raise RuntimeError("{} is not a target node".format(target_id))
        if not isinstance(target["object"], builder.targets.Target):
            raise RuntimeError("{} is not a target node".format(target_id))

    def filter_target_ids(self, target_ids):
        """Takes in a list of ids in the graph and returns a list of the ids
        that correspond to targets

        An id is considered to be a target id if the object in the node
        specified by the id is an instance of Target

        Args:
            target_ids: A list of ids that are potentially targets

        Returns:
            A filtered list of target_ids where only the id's corresponding to
            target nodes are left.
        """
        output_target_ids = []
        for target_id in target_ids:
            target = self.node[target_id]
            if "object" in target:
                the_object = target["object"]
                if isinstance(the_object, builder.targets.Target):
                    output_target_ids.append(target_id)
        return output_target_ids

    def filter_job_ids(self, job_ids):
        """Takes in a list of ids in the graph and returns a list of ids that
        correspond to jobs

        An id is considered to be a job id if the object in the node specified
        by the id is an instance of JobState

        Args:
            job_ids: A list of ids that are potentially jobs

        Returns:
            A filtered list of job_ids where only the id's corresponding to job
            nodes are left.
        """
        output_job_ids = []
        for job_id in job_ids:
            job = self.node[job_id]
            if "object" in job:
                the_object = job["object"]
                if isinstance(the_object, builder.jobs.JobState):
                    output_job_ids.append(job_id)
        return output_job_ids

    def get_targets(self, job_id):
        """Returns a list of the ids of all the targets for the job_id

        The targets for the job_id are the target nodes that are direct
        decendants of job_id

        Args:
            job_id: The job to return the targets of

        Returns:
            A list of ids corresponding to the targets of job_id
        """
        self.assert_job(job_id)
        neighbor_ids = self.neighbors(job_id)
        return self.filter_target_ids(neighbor_ids)

    def get_dependencies(self, job_id):
        """Returns a list of the ids of all the dependency targets for the
        job_id

        The dependencies for the job_id are the target nodes that are direct
        predecessors of the depends nodes for the job

        Args:
            job_id: The job to return the targets of

        Returns:
            A list of ids corresponding to the dependencies of job_id
        """
        self.assert_job(job_id)
        dependency_target_ids = []
        depends_ids = self.predecessors(job_id)
        for depends_id in depends_ids:
            target_ids = self.predecessors(depends_id)
            dependency_target_ids = (dependency_target_ids +
                    self.filter_target_ids(target_ids))
        return dependency_target_ids

    def get_targets_or_dependencies(self, job_id, direction):
        """Returns either the targets or the dependencies depending on the
        direction

        direction cane be "up" (dependencies) or "down" (targets)

        Args:
            job_id: The job to return the targets or the dependencies of
            direction: the direciton that the returned nodes will be in realtion
                to the job.
        """
        if direction == "up":
            return self.get_dependencies(job_id)
        else:
            return self.get_targets(job_id)

    def get_creators(self, target_id):
        """Returns a list of the ids of all the creators for the target_id

        The creators of a target are all direct predecessors of the target

        Args:
            target_id: The target_id to return the creators of

        Returns:
            A list of ids corresponding to the creators of the target_id
        """
        self.assert_target(target_id)
        parent_ids = self.predecessors(target_id)
        return self.filter_job_ids(parent_ids)

    def get_dependants(self, target_id):
        """Returns a list of the ids of all the dependants for the target_ids

        The dependants of a target are all the direct decendants of all the
        depends nodes that depend on the target

        Args:
            target_id: The target_id to return the dependants of

        Returns:
            A list of ids corresponding to the dependants of the target_id
        """
        self.assert_target(target_id)
        dependant_ids = []
        depends_ids = self.neighbors(target_id)
        for depends_id in depends_ids:
            job_ids = self.neighbors(depends_id)
            dependant_ids = dependant_ids + self.filter_job_ids(job_ids)
        return dependant_ids

    def get_dependants_or_creators(self, target_id, direction):
        """Returns the dependants or the creators of the targets depending on
        the direction

        direction can be up (creators) down (dependants)

        Args:
            target_id: the target to return the dependants or creators of
            direction: The direction that the returned nodes will be to the
                target_id
        """
        if direction == "up":
            return self.get_creators(target_id)
        else:
            return self.get_dependants(target_id)

    def _connect_targets(self, node, target_type, targets, edge_data):
        """Connets the node to it's targets

        All the targets are connected to the node. The corresponding edge data
        is what is given by edge_data and the label is target_type

        Args:
            node: the node that the targets are targets for.
            target_type: the type of the targets (produces, alternates, ...) and
                the label for the edge
            targets: all the targets that should be connected to the node.
            edge_data: any extra data to be added to the edge dict
        """
        for target in targets:
            target = self.add_node(target)
            self.add_edge(node.unique_id, target.unique_id, edge_data, label=target_type)

    def _connect_dependencies(self, node, dependency_type, dependencies, data):
        """Connets the node to it's dependnecies

        All the depenencies are connected to the node. The corresponding edge
        data is what is given by data and the label is dependency_type.

        A depends node is put inbetween the job node and the dependencies. The
        type of depends node is looked up with the id dependency_type

        Args:
            node: the node that the dependencies are dependencies for.
            dependency_type: the type of dependency. Looked up to create the
                depends node. Is also the label for the edge
            dependencies: The nodes that shoulds be connected to the node
            data: any extra data to be added to the edge dict
        """
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

    def _expand_direction(self, node, direction):
        """Takes in a node and expands it's targets or dependencies and adds
        them to the graph

        The taregets are expanded if direction is down and the dependencies are
        expanded if the direction is up

        Args:
            node: the node that need's it's targets or dependnecies expanded for
            direction: the direction that the expanded nodes are in realtion to
                the node
        """
        # The node has already been expanded in that direction
        if node.expanded_directions[direction]:
            return self.get_targets_or_dependencies(node.unique_id, direction)

        # get the list of targets or dependencies to expand
        target_depends = {}
        unexpanded_node = self.rule_dep_graph.node[node.unexpanded_id]["object"]
        if direction == "up":
            target_depends = unexpanded_node.get_dependencies(
                    build_context=node.build_context)
        else:
            target_depends = unexpanded_node.get_targets(
                    build_context=node.build_context)

        expanded_targets_list = []
        # expanded for each type of target or dependency
        for target_type, target_group in target_depends.iteritems():
            for target in target_group:
                build_context = node.build_context
                edge_data = target.edge_data
                expanded_targets = target.expand(build_context)
                if direction == "up":
                    dependency_type = (builder.dependencies
                            .get_dependencies(target_type))
                    self._connect_dependencies(node, dependency_type,
                            expanded_targets, edge_data)

                if direction == "down":
                    self._connect_targets(node, target_type, expanded_targets,
                                          edge_data)
                expanded_targets_list = expanded_targets_list + expanded_targets
        return expanded_targets_list

    def _self_expand_next_direction(self, expanded_directions, depth,
            current_depth, top_jobs, cache_set, direction):
        """Expands out the next job nodes

        Args:
            expanded_directions: Eithe the list of the dependencies or the
                targets of the current node
            depth: How far the graph should be expanded in any branch
            current_depth: The depth the branch has been expanded
            top_jobs: Jobs that were at the end of a branch
            cache_set: A set of jobs that have already been expanded
            direction: The direction that the next nodes sould be in relation to
                the current
        """
        next_nodes = []
        for expanded_direction in expanded_directions:
            if expanded_direction.unique_id in cache_set:
                continue

            # if the node is already in the graph, then return the nodes in the
            # direction of direction
            if expanded_direction.expanded_directions[direction]:
                next_node_ids = self.get_dependants_or_creators(
                        expanded_direction.unique_id, direction)
                for next_node_id in next_node_ids:
                    next_nodes.append(self.node[next_node_id]["object"])
                continue

            # we have to use the unexpanded node to look in the rule dependnecy
            # graph for the next job
            unexpanded_next_node_ids = (
                    self.rule_dep_graph
                        .get_dependants_or_creators(
                                expanded_direction.unexpanded_id, direction))

            # expand out the job and then add it to a list so that they can
            # continue the expansion later
            for unexpanded_next_node_id in unexpanded_next_node_ids:
                unexpanded_next_node = (self.rule_dep_graph
                                            .node[unexpanded_next_node_id]
                                                 ["object"])
                next_nodes = next_nodes + unexpanded_next_node.expand(
                        expanded_direction.build_context)
            cache_set.add(expanded_direction.unique_id)
            expanded_direction.expanded_directions[direction] = True

        # continue expanding in the direction given
        for next_node in next_nodes:
            self._self_expand(
                    next_node, direction, depth, current_depth, top_jobs,
                    cache_set)


    def _self_expand(self, node, direction, depth, current_depth, top_jobs,
                     cache_set=None):
        """Input a node to expand and a build_context, magic ensues

        The node should already be an expanded node. It then expands out the
        graph in the direction given in relation to the node.

        Args:
            node: the expanded node to continue the expansion of the graph in
            direction: the direction to expand in the graph
            depth: the maximum depth that any branch should be
            current_depth: the depth that the branch is in
            top_jobs: jobs that are potentially the highest jobs in the branch
                (used for force)
            cache_set: A set of jobs that have already been expanded
        """
        if cache_set is None:
            cache_set = set([])

        if node.unique_id in cache_set:
            return

        self.add_node(node)

        expanded_targets = self._expand_direction(node, "down")
        expanded_dependencies = self._expand_direction(node, "up")
        cache_set.add(node.unique_id)

        next_nodes = []
        if depth is not None:
            if not isinstance(node, builder.jobs.MetaJobState):
                current_depth = current_depth + 1
            if current_depth >= depth:
                top_jobs.add(node.unique_id)
                return

        if direction == "up":
            next_nodes = self._self_expand_next_direction(
                    expanded_dependencies, depth, current_depth,
                    top_jobs, cache_set, direction)
            if not next_nodes:
                top_jobs.add(node.unique_id)
        if direction == "down":
            next_nodes = self._self_expand_next_direction(
                    expanded_targets, depth, current_depth, top_jobs,
                    cache_set, direction)


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
        if not build_context.get("force"):
            build_context["force"] = False

        unexpanded_id = build_context["start_job"]
        del build_context["start_job"]
        start_node = (self.rule_dep_graph
                          .node[unexpanded_id]["object"])
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

        return self

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

    def get_job(self, job_id):
        """
        Fetch job with the given ID
        """
        return self.rule_dep_graph.get_job(job_id)

    def get_job_state(self, job_state_id):
        """
        Fetch job state with the given ID
        """
        return self.node[job_state_id]['object']

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
        raise NotImplementedError()
