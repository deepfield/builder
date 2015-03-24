"""The graph file holds logic on how to build out the rule dependency graph
and the build graph
"""

import copy
import collections
import multiprocessing
import Queue
import shlex
import subprocess

import arrow
import networkx

import builder.dependencies
import builder.jobs
import builder.targets


class RuleDependencyGraph(networkx.DiGraph):
    """The rule dependency graph holds all the information on how jobs relate
    to jobs and their targets. It also holds information on what their aliases
    are
    """
    def __init__(self, jobs, metas=None, config=None):
        super(RuleDependencyGraph, self).__init__()
        if config is None:
            config = {}

        if metas is None:
            metas = []

        self.jobs = jobs
        self.metas = metas
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

        if isinstance(node, builder.jobs.MetaTarget):
            node_data["style"] = "filled"
            node_data["fillcolor"] = "#FFE0FF"
            node_data["color"] = "purple"

        node_data.update(attr)
        node_data.update(attr_dict)

        super(RuleDependencyGraph, self).add_node(node.unexpanded_id,
                                                  attr_dict=node_data)

    def add_meta(self, meta):
        """Adds a meta target into the rule dependency graph

        Using the meta passed in, a meta node and an edge between the meta node
        and all of it's jobs specified in it's job collection are added to the
        graph. The resulting node has a meta instance as it's object and is
        connected to the nodes specified by the job collection.

        Args:
            meta: the meta target to add to the rule dependnecy graph
        """
        self.add_node(meta)
        jobs = meta.get_job_collection()
        for job in jobs:
            self.add_edge(job, meta.unexpanded_id, label="meta")

    def add_job(self, job):
        """Adds a job and it's targets and dependencies to the rule dependency
        graph

        Using the job class passed in, a job node, and nodes for each of the
        targets specified by get_targets and get_dependencies is added. The
        resulting nodes have a job as the job node's object and a expander as
        the target nodes' object.

        Args:
            job: the job to add to the rule dependency graph
        """
        self.add_node(job)
        targets = job.get_targets()
        for target_type, target in targets.iteritems():
            for sub_target in target:
                self.add_node(sub_target)
                self.add_edge(job.unexpanded_id, sub_target.unexpanded_id,
                              label=target_type)

        dependencies = job.get_dependencies()
        for dependency_type, dependency in dependencies.iteritems():
            for sub_dependency in dependency:
                self.add_node(sub_dependency)
                self.add_edge(sub_dependency.unexpanded_id, job.unexpanded_id,
                              label=dependency_type)

    def construct(self):
        """Constructs the rule dependency graph.

        Adds all the jobs that are specified by the jobs keyword to the graph
        """
        for job in self.jobs:
            if not job.get_enable():
                continue
            self.add_job(job)

        for meta in self.metas:
            if not meta.get_enable():
                continue
            self.add_meta(meta)

    def write_dot(self, file_name):
        """Writes the rule dependency graph to the file_name

        Currently does not modify the graph in anyway before writing out
        """
        networkx.write_dot(self, file_name)

    def is_job(self, job_id):
        """Returns if the id passed in relates to a job node"""
        job = self.node[job_id]
        if "object" not in job:
            return False
        if not isinstance(job["object"], builder.jobs.Job):
            return False
        return True

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
        if not self.is_job(job_id):
            raise RuntimeError("{} is not a job node".format(job_id))

    def is_target(self, target_id):
        """Returns if the id passed in relates to a target node or not"""
        target = self.node[target_id]
        if "object" not in target:
            return False
        if not isinstance(target["object"], builder.expanders.Expander):
            return False
        return True

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
        if not self.is_target(target_id):
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
            if self.is_target(target_id):
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
            if self.is_job(job_id):
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

    def get_target(self, target_id):
        """Returns the object corresponding to the target_id"""
        return self.node[target_id]["object"]

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
        potential_job = self.node.get(job_id, {}).get('object')
        if not isinstance(potential_job, builder.jobs.Job):
            raise LookupError("Job {} not found".format(job_id))
        return potential_job

    def get_all_jobs(self):
        """Return a list of all jobs in the rule dependency graph
        """
        jobs = []
        for job_node in filter(lambda x: isinstance(x.get('object'), builder.jobs.Job), self.node.itervalues()):
            jobs.append(job_node['object'])

        return jobs

    def get_all_target_expanders(self):
        """Return a list of all jobs in the rule dependency graph
        """
        targets = []

        def select_nodes(node):
            data = node.get('object')
            if isinstance(data, builder.expanders.Expander) and issubclass(data.base_class, builder.targets.Target):
                return True

            return False

        for target_node in filter(select_nodes, self.node.itervalues()):
            targets.append(target_node['object'])

        return targets
class BuildGraph(networkx.DiGraph):
    """The build object will control the rule dependency graph and the
    build graph"""
    def __init__(self, jobs, metas=None, number_of_consumers=None, config=None):
        super(BuildGraph, self).__init__()
        if metas is None:
            metas = {}

        if config is None:
            config = {}

        if number_of_consumers is None:
            number_of_consumers = multiprocessing.cpu_count()

        self.jobs = jobs
        self.rule_dep_graph = RuleDependencyGraph(jobs, metas=metas,
                                                  config=config)
        self.rule_dep_graph.construct()
        self.config = config
        self.time = arrow.get()
        self.count = 0
        self.cache_count = 0

        self.number_of_consumers = number_of_consumers

        self.queue = Queue.Queue()
        self.lock = multiprocessing.Lock()

    def write_rule_dep_graph(self, file_name):
        """Ensures the rule dep graph exists and then writes it to file_name

        If the rule dep graph doesn't exist then it is constructed. After the
        graph is constructed it's write_dot is called

        Args:
            file_name: the name fo the file to write the dot file to
        """
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

        self.rule_dep_graph.construct()
        return self.rule_dep_graph

    def add_node(self, node, new_nodes=None, attr_dict=None, **kwargs):
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
        else:
            if new_nodes is not None:
                new_nodes.add(node.unique_id)

        if isinstance(node, builder.targets.Target):
            node_data["style"] = "filled"
            node_data["fillcolor"] = "#C2FFFF"
            node_data["color"] = "blue"
        node_data.update(attr_dict)
        node_data.update(kwargs)
        node_data["object"] = node

        super(BuildGraph, self).add_node(node.unique_id, attr_dict=node_data)
        return node

    def is_job(self, job_id):
        """Returns if the node relating to job id is a job node"""
        job = self.node[job_id]
        if "object" not in job:
            return False
        if not isinstance(job["object"], builder.jobs.JobState):
            return False
        return True

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
        if not self.is_job(job_id):
            raise RuntimeError("{} is not a job node".format(job_id))

    def is_target(self, target_id):
        """Returns if the node related to target_id is a target node"""
        target = self.node[target_id]
        if "object" not in target:
            return False
        if not isinstance(target["object"], builder.targets.Target):
            return False
        return True

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
        if not self.is_target(target_id):
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
            if self.is_target(target_id):
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
            if self.is_job(job_id):
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

    def get_ancestors(self, node_id, ancestors_list=None):
        """Returns a list of all the ancestors for node id merged with
        ancestor list
        """
        if ancestors_list is None:
            ancestors_list = set([])

        if node_id in ancestors_list:
            return ancestors_list

        for neighbor_id in self.neighbors(node_id):
            self.get_ancestors(neighbor_id, ancestors_list=ancestors_list)

        return ancestors_list

    def get_all_ancestors(self, node_ids):
        """Returns a list of all the ancestors for a given list of node ids"""
        ancestors_list = set([])
        for node_id in node_ids:
            self.get_ancestors(node_id, ancestors_list)

        return ancestors_list

    def _connect_targets(self, node, target_type, targets, edge_data,
                         new_nodes):
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
            target = self.add_node(target, new_nodes=new_nodes)
            self.add_edge(node.unique_id, target.unique_id, edge_data,
                          label=target_type)

    def _connect_dependencies(self, node, dependency_type, dependencies, data,
                              new_nodes):
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

        dependency = builder.dependencies.Dependency(dependency_type,
                                                     dependency_node_id)

        self.add_node(dependency, new_nodes=new_nodes,
                      label=dependency_type.func_name)

        self.add_edge(dependency_node_id, node.unique_id, data,
                      label=dependency_type.func_name)

        for dependency in dependencies:
            dependency = self.add_node(dependency, new_nodes=new_nodes)
            self.add_edge(dependency.unique_id, dependency_node_id, data,
                          label=dependency_type.func_name)

    def _expand_direction(self, job, direction, new_nodes):
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
        if job.expanded_directions[direction]:
            return self.get_targets_or_dependencies(job.unique_id, direction)

        # get the list of targets or dependencies to expand
        target_depends = {}
        unexpanded_job = self.rule_dep_graph.get_job(job.unexpanded_id)
        if direction == "up":
            target_depends = unexpanded_job.get_dependencies(
                    build_context=job.build_context)
        else:
            target_depends = unexpanded_job.get_targets(
                    build_context=job.build_context)

        expanded_targets_list = []
        # expanded for each type of target or dependency
        for target_type, target_group in target_depends.iteritems():
            for target in target_group:
                build_context = job.build_context
                edge_data = target.edge_data
                expanded_targets = target.expand(build_context)
                if direction == "up":
                    dependency_type = (builder.dependencies
                                              .get_dependencies(target_type))
                    self._connect_dependencies(job, dependency_type,
                                               expanded_targets, edge_data,
                                               new_nodes)

                if direction == "down":
                    self._connect_targets(job, target_type, expanded_targets,
                                          edge_data, new_nodes)
                expanded_targets_list = expanded_targets_list + expanded_targets
        return expanded_targets_list

    def _self_expand_next_direction(self, expanded_directions, depth,
                                    current_depth, top_jobs, new_nodes,
                                    cache_set, direction):
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
                    next_nodes.append(self.get_job(next_node_id))
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
                unexpanded_next_node = self.rule_dep_graph.get_job(
                        unexpanded_next_node_id)
                next_nodes = next_nodes + unexpanded_next_node.expand(
                        expanded_direction.build_context)
            cache_set.add(expanded_direction.unique_id)
            expanded_direction.expanded_directions[direction] = True

        # continue expanding in the direction given
        for next_node in next_nodes:
            self._self_expand(next_node, direction, depth, current_depth,
                              top_jobs, new_nodes, cache_set)
        return next_nodes


    def _self_expand(self, node, direction, depth, current_depth, top_jobs,
                     new_nodes, cache_set=None):
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

        self.add_node(node, new_nodes)

        expanded_targets = self._expand_direction(node, "down", new_nodes)
        expanded_dependencies = self._expand_direction(node, "up", new_nodes)
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
                    top_jobs, new_nodes, cache_set, direction)
            if not next_nodes:
                top_jobs.add(node.unique_id)
        if direction == "down":
            next_nodes = self._self_expand_next_direction(expanded_targets,
                                                          depth, current_depth,
                                                          top_jobs, new_nodes,
                                                          cache_set, direction)

    def construct_build_graph(self, build_context, cache_set=None,
                              top_jobs=None, new_nodes=None):
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
                force:
                    (default) False
                    (possible) boolean
                    the specifier for forcing a job. The top most job recieves
                    this property
        """
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
        if not 'object' in self.rule_dep_graph.node[unexpanded_id]:
            raise ValueError("Starting job with id {} not found in graph".format(unexpanded_id))
        start_node = self.rule_dep_graph.node[unexpanded_id]["object"]

        if cache_set is None:
            cache_set = set([])

        if top_jobs is None:
            top_jobs = set([])

        if new_nodes is None:
            new_nodes = set([])

        if isinstance(start_node, builder.jobs.MetaTarget):
            job_collection = start_node.get_job_collection()
            for job_id in job_collection:
                copy_build_context = copy.copy(build_context)
                copy_build_context["start_job"] = job_id
                self.construct_build_graph(copy_build_context,
                                           cache_set=cache_set,
                                           top_jobs=top_jobs,
                                           new_nodes=new_nodes)
            return


        expanded_nodes = start_node.expand(build_context)
        for expanded_node in expanded_nodes:
            self._self_expand(expanded_node, "up", depth, current_depth,
                              top_jobs, new_nodes, cache_set=cache_set)

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
        """Removes all jobs from top_jobs that is below job_id"""
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
        """Removes all jobs in top_jobs that is below another job in top_jobs"""
        cache_set = set([])
        for top_job in top_jobs.copy():
            for target_id in self.neighbors(top_job):
                for depends_id in self.neighbors(target_id):
                    for job_id in self.neighbors(depends_id):
                        self._remove_sibilings_recurse(job_id, top_jobs,
                                                       cache_set)

    def get_starting_jobs(self):
        """Used to return a list of jobs to run"""
        should_run_list = []
        for _, node in self.node.iteritems():
            job = node["object"]
            if isinstance(job, builder.jobs.JobState):
                if job.get_should_run(self):
                    should_run_list.append(job)
        return should_run_list

    def get_target(self, target_id):
        """
        Fetch target with the given ID
        """
        return self.node[target_id]['object']

    def get_job(self, job_id):
        """
        Fetch job state with the given ID
        """
        return self.node[job_id]['object']

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
        self.update_targets(target_ids)

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

    def update_targets(self, target_ids):
        """Takes in a list of target ids and updates all of their needed
        values
        """
        update_function_list = collections.defaultdict(list)
        for target_id in target_ids:
            target = self.get_target(target_id)
            func = target.get_bulk_exists_mtime
            update_function_list[func].append(target_id)

        for update_function, target_ids in update_function_list.iteritems():
            exists_mtime_dict = update_function(target_ids)
            for target_id in target_ids:
                target = self.get_target(target_id)
                target.exists = exists_mtime_dict[target_id]["exists"]
                target.mtime = exists_mtime_dict[target_id]["mtime"]

    def update_jobs(self, job_ids):
        """Takes in a list of job ids and updates all of their needed values"""
        job_ids = self.get_all_ancestors(job_ids)
        job_ids = self.filter_job_ids(job_ids)

        cache_set = set([])
        for job_id in job_ids:
            job = self.get_job(job_id)
            job.get_is_buildable(self, cached=False)
            job.get_is_stale(self, cached=False)
            job.get_should_run(self, cached=False, cache_set=cache_set)

    def update_new_nodes(self, node_ids):
        """Takes in a list of nodes to update, and it updates all of them
        assuming they have never been in the graph before.

        The list can include targets, jobs, and dependency nodes
        """
        job_node_ids = self.filter_job_ids(node_ids)
        target_node_ids = self.filter_target_ids(node_ids)

        self.update_targets(target_node_ids)
        self.update_jobs(job_node_ids)

    @staticmethod
    def consumer(queue):
        """Takes off a job from the queue and runs it"""
        while True:
            func_dict = queue.get()
            build_graph = func_dict["build_graph"]
            unique_id = func_dict["unique_id"]
            func = func_dict["function"]
            if func == "break":
                return
            args = func_dict["args"]
            kwargs = func_dict["kwargs"]
            func(build_graph, unique_id, *args, **kwargs)

    @staticmethod
    def finish_job(build_graph, job_id):
        """Takes in a unique_id and marks it as finished in the build graph and
        does what ever needs to happen next
        """
        build_graph.lock.acquire()
        build_graph.update_job_cache(job_id)
        next_job_ids_to_run = build_graph.get_next_jobs_to_run(job_id)
        for next_job_id_to_run in next_job_ids_to_run:
            next_job_to_run = build_graph.get_job(next_job_id_to_run)
            command = next_job_to_run.get_command(build_graph)
            build_graph.add_command_to_queue(next_job_to_run.unique_id,
                                             command)

        build_graph.lock.release()

    @staticmethod
    def finish_target(build_graph, target_id):
        """Takes in a target id and marks updates the values associated with it
        and then does what ever needs to happen next
        """
        build_graph.lock.acquire()
        build_graph.update_target_cache(target_id)
        update_job_ids = build_graph.get_creators(target_id)
        if not update_job_ids:
            update_job_ids = build_graph.get_dependants(target_id)
        for update_job_id in update_job_ids:
            next_job_ids_to_run = build_graph.get_next_jobs_to_run(
                    update_job_id)
            for next_job_id_to_run in next_job_ids_to_run:
                next_job_to_run = build_graph.get_job(next_job_id_to_run)
                command = next_job_to_run.get_command(build_graph)
                build_graph.add_command_to_queue(next_job_to_run.unique_id,
                                                 command)

        build_graph.lock.release()

    @staticmethod
    def run_command(build_graph, unique_id, command):
        """Takes in a command string to run as a subprocess"""
        args = shlex.split(command)
        p = subprocess.Popen(args)
        p.communicate()
        build_graph.add_command_to_queue(unique_id, BuildGraph.finish_job)

    def add_command_to_queue(self, unique_id, command):
        """Takes in a command string to run and adds it to the queue"""
        function = BuildGraph.run_command
        self.add_job_to_queue(unique_id, function, command)

    def add_job_to_queue(self, unique_id, function, *args, **kwargs):
        """Adds the function to the queue so that it can be run by a consumer"""
        func_dict = {
            "build_graph": self,
            "unique_id": unique_id,
            "function": function,
            "args": args,
            "kwargs": kwargs,
        }
        self.queue.put(func_dict)

    def consume(self):
        """Starts up the consumers and starts the running"""
        p = multiprocessing.Pool(self.number_of_consumers)
        p.map(BuildGraph.consumer,
              [self.queue for _ in xrange(self.number_of_consumers)])
