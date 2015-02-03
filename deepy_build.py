"""This module is used to give the build defaults for the deepy jobs"""

import copy
import networkx

import deepy.cfg

import builder.build
import builder.deepy_jobs

class DeepyBuild(builder.build.BuildGraph):
    """Used to run the default deepy constructions"""
    def __init__(self, jobs=None, config=None):
        if config is None:
            config = deepy.cfg.slice_config
            config.update(deepy.cfg.vm_config)

        rules_db = deepy.make.construct_rules()

        jobs = []

        for rule_id in rules_db:
            dict_job = builder.deepy_jobs.DeepyDictJob(
                    rule_id, rules_db, config=config)
            if dict_job.get_type() != "target":
                jobs.append(dict_job)

        super(DeepyBuild, self).__init__(jobs, config)

    def write_rule_dep_graph(self, file_name):
        """Compresses all the bundle targets"""
        if self.rule_dep_graph is None:
            self.construct_rule_dependency_graph()
        write_graph = copy.deepcopy(self.rule_dep_graph)
        for node_id in write_graph.node.keys():
            if node_id not in write_graph:
                continue
            node = write_graph.node[node_id]["object"]
            if not isinstance(node, builder.jobs.Job):
                continue
            if "bundle" in node.get_type():
                neighbor_ids = write_graph.neighbors(node_id)
                if not neighbor_ids:
                    continue
                new_neighbor_id = node_id + " targets"
                networkx.DiGraph.add_node(write_graph, new_neighbor_id, style="filled",
                        fillcolor="green", color="black")
                for neighbor_id in neighbor_ids:
                    dependant_ids = write_graph.neighbors(neighbor_id)

                    for dependant_id in dependant_ids:
                        write_graph.add_edge(new_neighbor_id, dependant_id)
                    write_graph.remove_node(neighbor_id)
                write_graph.add_edge(node_id, new_neighbor_id)
        networkx.write_dot(write_graph, file_name)

