"""The graph file holds logic on how to build out the rule dependency graph
and the build graph
"""

import arrow
import collections
import copy
import os
import networkx
import tempfile
import subprocess

import builder.dependencies
import builder.jobs
import builder.targets

class BuildManager(object):
    dependency_registery = {
        "depends": builder.dependencies.depends,
        "depends_one_or_more": builder.dependencies.depends_one_or_more,
    }

    """A build manager holds a rule dependency graph and is then creates a new
    build graph by recieving a list of start jobs and a build_context

    A build manager is usefull when looking to creating separate build graphs
    using the same rule dependency graph.
    """
    def __init__(self, jobs, metas, dependency_registery=None, config=None):
        super(BuildManager, self).__init__()

        if dependency_registery is None:
            dependency_registery = BuildManager.dependency_registery
        if config is None:
            config = {}

        self.jobs = jobs
        self.metas = metas
        self.dependency_registery = dependency_registery
        self.config = config

        self.rule_dependency_graph = RuleDependencyGraph(jobs, metas,
                                                         config=config)

    def make_build(self):
        """Constructs a new build graph by adding the jobs and following the
        build_context's rules
        """
        build_graph = BuildGraph(
                self.rule_dependency_graph,
                dependency_registery=self.dependency_registery,
                config=self.config)
        return build_graph

    def get_rule_dependency_graph(self):
        """ Return the rule dependency graph that drives all builds by this BuildManager
        """
        return self.rule_dependency_graph


class BaseGraph(networkx.DiGraph):

    def write_dot(self, file_name):
        """Writes the rule dependency graph to the file_name

        Currently does not modify the graph in anyway before writing out
        """
        networkx.write_dot(self, file_name)

    def write_pdf(self, file_name):
        """
        Writes the rule dependency graph to file_name
        """
        with tempfile.NamedTemporaryFile() as f:
            self.write_dot(f.name)
            dot = '/usr/bin/dot' if os.path.exists('/usr/bin/dot') else 'dot'
            subprocess.check_call([dot, '-Tpdf', f.name, file_name])


class RuleDependencyGraph(BaseGraph):
    """The rule dependency graph holds all the information on how jobs relate
    to jobs and their targets. It also holds information on what their aliases
    are
    """
    def __init__(self, jobs, metas, config=None):
        super(RuleDependencyGraph, self).__init__()
        if config is None:
            config = {}

        if metas is None:
            metas = []

        self.jobs = jobs
        self.metas = metas
        self.config = config
        self.construct()

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
        targets specified by get_target_ids and get_dependency_ids is added. The
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



    def is_meta(self, meta_id):
        """Returns if the id passed in relates to a meta node"""
        meta = self.node[meta_id]
        if "object" not in meta:
            return False
        if not isinstance(meta["object"], builder.jobs.MetaTarget):
            return False
        return True

    def assert_meta(self, meta_id):
        """Asserts that the id is a meta node"""
        if not self.is_meta(meta_id):
            raise RuntimeError("{} is not a meta node".format(meta_id))

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

    def get_target_ids(self, job_id, type=None):
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

    def get_dependency_ids(self, job_id):
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

    def get_dependents(self, target_id):
        """Returns a list of the ids of all the dependents for the target_ids

        The dependents of a target are all the direct decendants of the target

        Args:
            target_id: The target_id to return the dependents of

        Returns:
            A list of ids corresponding to the dependents of the target_id
        """
        self.assert_target(target_id)
        job_ids = self.neighbors(target_id)
        dependent_ids = self.filter_job_ids(job_ids)
        return dependent_ids

    def get_dependents_or_creators(self, target_id, direction):
        """Returns the dependents or the creators of the targets depending on
        the direction

        direction can be up (creators) down (dependents)

        Args:
            target_id: the target to return the dependents or creators of
            direction: The direction that the returned nodes will be to the
                target_id
        """
        if direction == "up":
            return self.get_creators(target_id)
        else:
            return self.get_dependents(target_id)

    def get_meta(self, meta_id):
        """Returns the object corresponding to the meta_id"""
        self.assert_meta(meta_id)
        return self.node[meta_id]["object"]

    def get_target(self, target_id):
        """Returns the object corresponding to the target_id"""
        self.assert_target(target_id)
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
        self.assert_job(job_id)
        return self.node.get(job_id, {}).get('object')

    def get_all_jobs(self):
        """Return a list of all jobs in the rule dependency graph
        """
        jobs = []
        for job_id in filter(lambda x: self.is_job(x), self.node):
            jobs.append(self.get_job(job_id))

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

    def get_jobs_from_meta(self, meta_id):
        """Returns job ids for the meta, different from job collection as metas
        can point to other metas"""
        job_ids = []
        meta = self.get_meta(meta_id)
        job_collection = meta.get_job_collection()
        for job_id in job_collection:
            if self.is_meta(job_id):
                job_ids = job_ids + self.get_jobs_from_meta(job_id)
            else:
                self.assert_job(job_id)
                job_ids.append(job_id)

        return job_ids

class BuildGraph(BaseGraph):
    """The build object will control the rule dependency graph and the
    build graph"""
    def __init__(self, rule_dependency_graph, dependency_registery=None, config=None):
        super(BuildGraph, self).__init__()
        if dependency_registery is None:
            dependency_registery = {}
        if config is None:
            config = {}

        self.rule_dependency_graph = rule_dependency_graph
        self.dependency_registery = dependency_registery
        self.config = config

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

        Returns:
            Returns the nodes that is now in the graph
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
        node = self.node[node.unique_id]["object"]
        return node

    def is_job(self, job_id):
        """Returns if the node relating to job id is a job node"""
        job = self.node[job_id]
        if "object" not in job:
            return False
        if not isinstance(job["object"], builder.jobs.JobState):
            return False
        return True

    def is_dependency_type(self, dependency_id):
        """Returns if the ndoe relating to dependnecy id is a dependency node"""
        dependency_container = self.node[dependency_id]
        if "object" not in dependency_container:
            return False
        if not isinstance(dependency_container["object"],
                          builder.dependencies.Dependency):
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

    def assert_dependency_type(self, dependency_id):
        if not self.is_dependency_type(dependency_id):
            raise RuntimeError("{} is not a depends node".format(dependency_id))

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

    def get_target_ids_iter(self, job_state_id):
        self.assert_job_state(job_state_id)
        for target_id in self.neighbors_iter(job_state_id):
            if self.is_target(target_id):
                yield target_id

    def get_target_ids(self, job_state_id):
        return list(self.get_target_ids_iter(job_state_id))

    def get_dependency_ids_iter(self, job_state_id):
        self.assert_job_state(job_state_id)
        for depends_id in self.predecessors_iter(job_state_id):
            if self.is_dependency_type(depends_id):
                for dependency_id in self.predecessors_iter(depends_id):
                    if self.is_target(dependency_id):
                        yield dependency_id

    def get_dependency_ids(self, job_state_id):
        return list(self.get_dependency_ids_iter(job_state_id))

    def get_creator_ids_iter(self, target_id):
        self.assert_target(target_id)
        for creator_id in self.predecessors_iter(target_id):
            if self.is_job_state(creator_id):
                yield creator_id

    def get_creator_ids(self, target_id):
        return list(self.get_creator_ids_iter(target_id))

    def get_dependent_ids_iter(self, target_id):
        self.assert_target(target_id)
        for depends_id in self.neighbors_iter(target_id):
            if self.is_dependency_type(depends_id):
                for dependent_id in self.neighbors_iter(depends_id):
                    if self.is_job_state(dependent_id):
                        yield dependent_id

    def get_dependent_ids(self, target_id):
        return list(self.get_dependent_ids_iter(target_id))

    def get_target_or_dependency_ids_iter(self, job_state_id, direction):
        if direction == "up":
            return self.get_dependency_ids_iter(job_state_id)
        else:
            return self.get_target_ids_iter(job_state_id)

    def get_target_or_dependency_ids(self, job_state_id, direction):
        return list(self.get_target_or_dependency_ids_iter(job_state_id,
                                                           direction))

    def get_dependent_or_creator_ids_iter(self, target_id, direction):
        if direction == "up":
            return self.get_creator_ids_iter(target_id)
        else:
            return self.get_dependent_ids_iter(target_id)

    def get_dependent_or_creator_ids(self, target_id, direction):
        return list(self.get_dependent_or_creator_ids_iter(target_id,
                                                           direction))

    def get_target_relationships(self, job_state_id):
        self.assert_job_state(job_state_id)
        out_edges_iter = self.out_edges_iter(job_state_id, data=True)
        target_dict = collections.defaultdict(dict)
        for _, target_id, data in out_edges_iter:
            if self.is_target(target_id):
                target_dict[data["kind"]][target_id] = data
        return target_dict

    def get_dependency_relationships(self, job_state_id):
        self.assert_job_state(job_state_id)
        in_edges_iter = self.in_edges_iter(job_state_id, data=True)
        dependency_dict = collections.defaultdict(list)
        for depends_node_id, _, data in in_edges_iter:
            if self.is_dependency_type(depends_node_id):
                depends_node = self.get_dependency_type(depends_node_id)
                group_dict = {}
                group_dict["function"] = depends_node.func
                group_dict["data"] = data
                group_dict["targets"] = self.filter_target_ids(
                        self.predecessors(depends_node_id))
                dependency_dict[depends_node.kind].append(group_dict)
        return dependency_dict


    def get_dependents_or_creators_iter(self, target_id, direction):
        """Takes in a target id and returns an iterator for either the dependent
        ids or the creator ids depending on the direcion

        Args:
            target_id: the target id to get the dependent ids or the creator ids
                for
            direction: If the direction is "up" the creators are retrieved and
                if it is "down" then the dependents are retrieved.
                Must be "up" or "down" raises a value error if it is
                neither.

        Returns:
            An iterator for either the dependency ids or the target ids of the
            job depending on the the direction.
        """
        if direction == "up":
            return self.get_creator_ids_iter(target_id)
        elif direction == "down":
            return self.get_dependent_ids_iter(target_id)
        else:
            raise ValueError("direction must be up or down, recieved "
                             "{}".format(direction))

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
            new = target.unique_id not in self
            target = self.add_node(target)
            if new:
                new_nodes.append(target.unique_id)
            self.add_edge(node.unique_id, target.unique_id, edge_data,
                          label=target_type, kind=target_type)

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
                                                     dependency_node_id,
                                                     dependency_type.func_name)

        new = dependency.unique_id not in self
        self.add_node(dependency, label=dependency_type.func_name)
        if new:
            new_nodes.append(dependency.unique_id)

        self.add_edge(dependency_node_id, node.unique_id, data,
                      label=dependency_type.func_name)

        for dependency in dependencies:
            new = dependency.unique_id not in self
            dependency = self.add_node(dependency)
            if new:
                new_nodes.append(dependency.unique_id)
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
            target_ids = self.get_target_or_dependency_ids(job.unique_id, direction)
            return [self.get_target(x) for x in target_ids]
        job.expanded_directions[direction] = True

        # get the list of targets or dependencies to expand
        target_depends = {}
        unexpanded_job = self.rule_dependency_graph.get_job(job.unexpanded_id)
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
                    dependency_type = self.dependency_registery[target_type]
                    self._connect_dependencies(job, dependency_type,
                                               expanded_targets, edge_data,
                                               new_nodes)

                if direction == "down":
                    self._connect_targets(job, target_type, expanded_targets,
                                          edge_data, new_nodes)
                expanded_targets_list = expanded_targets_list + expanded_targets
        return expanded_targets_list

    def _self_expand_next_direction(self, expanded_directions, depth,
                                    current_depth, new_nodes,
                                    cache_set, direction, directions_to_recurse):
        """Expands out the next job nodes

        Args:
            expanded_directions: Eithe the list of the dependencies or the
                targets of the current node
            depth: How far the graph should be expanded in any branch
            current_depth: The depth the branch has been expanded
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
                next_node_ids = self.get_dependent_or_creator_ids(
                        expanded_direction.unique_id, direction)
                for next_node_id in next_node_ids:
                    next_nodes.append(self.get_job_state(next_node_id))
                continue

            # we have to use the unexpanded node to look in the rule dependency
            # graph for the next job
            unexpanded_next_node_ids = (
                    self.rule_dependency_graph
                        .get_dependents_or_creators(
                                expanded_direction.unexpanded_id, direction))

            # expand out the job and then add it to a list so that they can
            # continue the expansion later
            for unexpanded_next_node_id in unexpanded_next_node_ids:
                unexpanded_next_node = self.rule_dependency_graph.get_job(
                        unexpanded_next_node_id)
                next_nodes = next_nodes + unexpanded_next_node.expand(
                        expanded_direction.build_context)
            cache_set.add(expanded_direction.unique_id)
            expanded_direction.expanded_directions[direction] = True

        # continue expanding in the direction given
        for next_node in next_nodes:
            self._self_expand(next_node, directions_to_recurse, depth, current_depth,
                              new_nodes, cache_set)
        return next_nodes


    def _self_expand(self, node, direction, depth, current_depth, new_nodes,
                     cache_set):
        """Input a node to expand and a build_context, magic ensues

        The node should already be an expanded node. It then expands out the
        graph in the direction given in relation to the node.

        Args:
            node: the expanded node to continue the expansion of the graph in
            direction: the direction to expand in the graph
            depth: the maximum depth that any branch should be
            current_depth: the depth that the branch is in
            cache_set: A set of jobs that have already been expanded
        """
        if node.unique_id in cache_set:
            return

        new = node.unique_id not in self
        node = self.add_node(node)
        if new:
            new_nodes.append(node.unique_id)

        expanded_targets = self._expand_direction(node, "down", new_nodes)
        expanded_dependencies = self._expand_direction(node, "up", new_nodes)
        cache_set.add(node.unique_id)

        if depth is not None:
            if not isinstance(node, builder.jobs.MetaJobState):
                current_depth = current_depth + 1
            if current_depth >= depth:
                return

        if "up" in direction:
            new_direction = set(["up"])
            self._self_expand_next_direction(expanded_dependencies, depth,
                                             current_depth, new_nodes,
                                             cache_set, "up", new_direction)
        if "down" in direction:
            self._self_expand_next_direction(expanded_targets, depth,
                                             current_depth, new_nodes,
                                             cache_set, "down", direction)

    def add_meta(self, new_meta, build_context, direction=None, depth=None,
                 force=False):
        """Adds in a specific meta and expands it using the expansion strategy

        Args:
            new_meta: the meta to add to the graph

            All the rest of the args are forwarded onto add_job

        Returns:
            A list of ids of nodes that are new to the graph during the adding
            of this new meta
        """
        jobs = self.rule_dependency_graph.get_jobs_from_meta(new_meta)
        new_nodes = []
        for job in jobs:
            new_nodes = new_nodes + self.add_job(job, build_context,
                                                 direction=direction,
                                                 depth=depth, force=force)
        return new_nodes


    def add_job(self, new_job, build_context, direction=None, depth=None,
                force=False):
        """Adds in a specific job and expands it using the expansion strategy

        Args:
            new_job: the job to add to the graph
            build_context: the context to expand this job out for
            direction: the direction to expand the graph
            depth: the number of job nodes deep to expand
            force: whether or not to force the new job

        Returns:
            A list of ids of nodes that are new to the graph during the adding
            of this new job
        """
        if direction is None:
            direction = set(["up"])

        # take care of meta targets
        new_nodes = []

        start_job = self.rule_dependency_graph.get_job(new_job)
        expanded_jobs = start_job.expand(build_context)


        current_depth = 0
        cache_set = set()

        for expanded_job in expanded_jobs:
            if force:
                expanded_job.force = True
            self._self_expand(expanded_job, direction, depth, current_depth,
                              new_nodes, cache_set)
        return new_nodes

    def get_starting_jobs(self):
        """Used to return a list of jobs to run"""
        should_run_list = []
        for _, job_state in self.job_state_iter():
            if job_state.get_should_run(self):
                should_run_list.append(job_state)
        return should_run_list

    def get_target(self, target_id):
        """
        Fetch target with the given ID
        """
        return self.node[target_id]["object"]

    def get_job(self, job_id):
        """
        Fetch job with the given ID
        """
        return self.rule_dependency_graph.get_job(job_id)

    def is_job_state(self, job_state_id):
        """Returns if the id passed in relates to a job node"""
        job_state_container = self.node[job_state_id]
        if "object" not in job_state_container:
            return False
        if not isinstance(job_state_container["object"], builder.jobs.JobState):
            return False
        return True

    def filter_target_ids(self, target_ids):
        return [x for x in target_ids if self.is_target(x)]

    def assert_job_state(self, job_state_id):
        """Asserts it is a job_state"""
        if not self.is_job_state(job_state_id):
            raise RuntimeError(
                    "{} is not a job state node".format(job_state_id))

    def get_job_state(self, job_state_id):
        """
        Fetch job state with the given ID
        """
        self.assert_job_state(job_state_id)
        return self.node[job_state_id]['object']

    def get_dependency_type(self, dependency_type_id):
        self.assert_dependency_type(dependency_type_id)
        return self.node[dependency_type_id]["object"]

    def job_state_iter(self):
        """Returns an iterator over the graph's (job_state_id, job_state)
        pairs
        """
        for node_id in self.node:
            if self.is_job_state(node_id):
                yield node_id, self.get_job_state(node_id)

    def get_next_jobs_to_run(self, job_id, update_set=None):
        """Returns the jobs that are below job_id that need to run"""
        if update_set is None:
            update_set = set([])

        if job_id in update_set:
            return []

        next_jobs_list = []

        job = self.get_job_state(job_id)
        if job.get_should_run(self):
            next_jobs_list.append(job_id)
            update_set.add(job_id)
            return next_jobs_list

        target_ids = self.get_target_ids(job_id)
        for target_id in target_ids:
            dependent_jobs = self.get_dependent_ids(target_id)
            for dependent_job in dependent_jobs:
                job = self.get_job_state(dependent_job)
                should_run = job.get_should_run_immediate(self, cached=False)
                if should_run:
                    next_jobs_list.append(dependent_job)

        update_set.add(job_id)

        return next_jobs_list

    def update_job_cache(self, job_state_id):
        """Updates the cache due to a job finishing"""
        target_ids = self.get_target_ids(job_state_id)
        self.update_targets(target_ids)

        job_state = self.get_job_state(job_state_id)
        job_state.get_stale(self, cached=False)

        for target_id in target_ids:
            dependent_ids = self.get_dependent_ids(target_id)
            for dependent_id in dependent_ids:
                dependent = self.get_job_state(dependent_id)
                dependent.get_buildable(self, cached=False)
                dependent.get_stale(self, cached=False)

        job_state.update_lower_nodes_should_run(self)

    def update_target_cache(self, target_id):
        """Updates the cache due to a target finishing"""
        target = self.get_target(target_id)
        target.get_mtime(cached=False)

        dependent_ids = self.get_dependent_ids(target_id)
        for dependent_id in dependent_ids:
            dependent = self.get_job_state(dependent_id)
            dependent.get_stale(self, cached=False)
            dependent.get_buildable(self, cached=False)
            dependent.update_lower_nodes_should_run(self)

    def update_targets(self, target_ids):
        """Takes in a list of target ids and updates all of their needed
        values
        """
        update_function_list = collections.defaultdict(list)
        for target_id in target_ids:
            target = self.get_target(target_id)
            func = target.get_bulk_exists_mtime
            update_function_list[func].append(target)

        for update_function, targets in update_function_list.iteritems():
            exists_mtime_dict = update_function(targets)
            for target in targets:
                target.exists = exists_mtime_dict[target.unique_id]["exists"]
                target.mtime = exists_mtime_dict[target.unique_id]["mtime"]

    def finish_job(self, job, success, log, update_job_cache=True):
        job.last_run = arrow.now()
        job.retries += 1
        if success:
            job.should_run = False
            job.force = False
            if update_job_cache:
                self.update_job_cache(job.get_id())

    def update(self, target_id):
        """Checks what should happen now that there is new information
        on a target
        """
        self.update_target_cache(target_id)
        creator_ids = self.get_creator_ids(target_id)
        creators_exist = False
        for creator_id in creator_ids:
            creators_exist = True
            next_jobs = self.get_next_jobs_to_run(creator_id)
            for next_job in next_jobs:
                self.run(next_job)
        if creators_exist == False:
            for dependent_id in self.get_dependent_ids(target_id):
                next_jobs = self.get_next_jobs_to_run(dependent_id)
                for next_job in next_jobs:
                    self.run(next_job)

    def run(self, job_id):
        raise NotImplementedError()
