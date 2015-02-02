"""defines implementation specific targets"""

import pandas as pd

import deepy.make

import builder.targets
import deepy.impala.tables as impala_tables


class DeepyLocalFileSystemTarget(builder.targets.LocalFileSystemTarget):
    """Used to make sure that all the nodes have the deepy $(var)s replaced"""
    def __init__(self, unexpanded_id, local_path, build_context, config=None):
        super(DeepyLocalFileSystemTarget, self).__init__(
                unexpanded_id, local_path, build_context,
                config=config)
        self.unique_id = deepy.make.subst_deepy_str(self.unique_id)

class DeepyGlobLocalFileSystemTarget(
        builder.targets.GlobLocalFileSystemTarget):
    """Used to make sure that all the nodes have the deepy $(var)s replaced"""
    def __init__(self, unexpanded_id, pattern, build_context, config=None):
        super(DeepyGlobLocalFileSystemTarget, self).__init__(
                unexpanded_id, pattern, build_context,
                config=config)
        self.unique_id = deepy.make.subst_deepy_str(self.unique_id)

class DeepyS3BackedLocalFileSystemTarget(
        builder.targets.S3BackedLocalFileSystemTarget):
    """Used to make sure that all the nodes have the deepy $(var)s replaced"""
    def __init__(self, unexpanded_id, local_path, build_context, config=None):
        super(DeepyS3BackedLocalFileSystemTarget, self).__init__(
                unexpanded_id, local_path, build_context,
                config=config)
        self.unique_id = deepy.make.subst_deepy_str(self.unique_id)

class DeepyS3BackedGlobLocalFileSystemTarget(
        builder.targets.S3BackedGlobLocalFileSystemTarget):
    """Used to make sure that all the nodes have the deepy $(var)s replaced"""
    def __init__(self, unexpanded_id, pattern, build_context, config=None):
        super(DeepyS3BackedGlobLocalFileSystemTarget, self).__init__(
                unexpanded_id, pattern, build_context,
                config=config)
        self.unique_id = deepy.make.subst_deepy_str(self.unique_id)


class ImpalaTimePartitionedTarget(builder.targets.Target):

    """An ImpalaTableTarget is a target that corresponds to the insertion of a single partition
    into an impala table

    The mtime is retrieved from the impala data index
    The existence value is determined by the impala data index
    """

    def __init__(self, unexpanded_id, unique_id, build_context, dataset_name, time_step, config=None):
        super(ImpalaTimePartitionedTarget, self).__init__(unexpanded_id, unique_id, build_context, config=config)
        self.dataset_name = dataset_name
        self.time_step = time_step

    def _get_table_manager(self):
        return impala_tables.make_cube_data_manager(self.dataset_name, self.time_step)

    @staticmethod
    def _partition_exists(table_manager, build_context):
        return True

    @staticmethod
    def _partition_mtime(table_manager, build_context):
        return 0

    @staticmethod
    def get_bulk_exists_mtime(targets):
        """Gets all the exists and mtimes for the local paths and returns them
        in a dict. Just as efficient as normal mtime and exists
        """

        # Get unique dataset/time_step pairs
        pairs = set()
        for t in targets:
            pairs.add((t.dataset_name, t.time_step))

        # Get a table manager for each unique pair
        table_managers = {}
        for dataset_name, time_step in pairs:
            table_managers[(dataset_name, time_step)] = impala_tables.make_cube_data_manager(dataset_name, time_step)

        # Reduce targets down to (dataset_name, time_step, timestamp, unique_id) pairs
        partitions_to_query = {'unique_id':[], 'dataset_name': [], 'time_step': [], 'timestamp': []}
        for target in targets:
            partitions_to_query['unique_id'].append(target.unique_id)
            partitions_to_query['dataset_name'].append(target.dataset_name)
            partitions_to_query['time_step'].append(target.time_step)
            partitions_to_query['timestamp'].append(target.build_context.get('start_time'))

        data_frame = pd.DataFrame(partitions_to_query)
        groups = data_frame.groupby(('dataset_name', 'time_step'))

        # For each target, decide if it exists and get its mtime if it does
        exists_mtime_dict = {}
        for target in targets:
            table_manager = table_managers[(target.dataset_name, target.time_step)]
            entry = {}
            exists_mtime_dict[target.unique_id] = entry
            entry['mtime'] = ImpalaTimePartitionedTarget._partition_mtime(table_manager, target.build_context)
            entry['exists'] = ImpalaTimePartitionedTarget._partition_exists(table_manager, target.build_context)

        return exists_mtime_dict

    def get_exists(self, cached=True):
        """Returns whether or not the target is on the local file system

        also gets value of the mtime. The existance value is based on whether
        or not mtime is none
        """
        if cached and self.exists is not None:
            return self.exists
        self.exists = self._partition_exists(self._get_table_manager(), self.build_context)
        return self.exists


    def get_mtime(self, cached=True):
        """Returns the value of the mtime of the file as reported
        by the local filesystem

        Also caches the existance value of the file

        Returns:
            The value of the mtime if the file exists, otherwise None
        """
        if cached and self.mtime is not None:
            return self.mtime
        self.mtime = self._partition_mtime(self._get_table_manager(), self.build_context)
        return self.mtime
