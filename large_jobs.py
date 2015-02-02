"""A large job is one that is capable of creating templated jobs"""

import abc

import builder.expanders
import builder.targets

class LargeJobExpander(object):
    """Large job expanders hold the definitions for expanding many jobs. A job
    expander should be able to expand in to many large expander instances that
    are each capable of expanding a job.
    """
    def __init__(self, job_id, config=None):
        if config is None:
            config = {}

        self.unexpanded_id = job_id
        self.job_id = job_id
        self.config = config

    @classmethod
    @abc.abstractmethod
    def expand_large(cls):
        """Will return a list of instances that will be prepared to expand
        each job that the class contains
        """

    def enable(self):
        """Returns whether or not the job specified by job_id should be run"""

    def get_targets(self):
        """Returns the targets for the job_id specified"""

    def get_dependencies(self):
        """Returns the dependencies for the job_id specified"""

    def get_command(self):
        """Returns the command for the specific job"""

    def get_cache_time(self):
        """Returns the cache time for the specific job"""
        return {}.get(self.job_id, "10min")

    def get_job_type(self):
        """Returns the class that the job should be expanded for"""
        return {}.get(self.job_id, builder.jobs.TemplatedJob)

    def expand(self, build_context=None):
        """Returns a list of the expanded jobs"""
        if build_context is None:
            build_context = {}

        enable = self.enable()
        targets = self.get_targets()
        dependencies = self.get_dependencies()
        command = self.get_command()
        cache_time = self.get_cache_time()

        job_type = self.get_job_type()
        expanded_jobs = job_type.expand(
                build_context, unexpanded_id=self.job_id,
                config=self.config)

        for expanded_job in expanded_jobs:
            expanded_job.enable = enable
            expanded_job.targets = targets
            expanded_job.dependencies = dependencies
            expanded_job.command = command
            expanded_job.cache_time = cache_time

        return expanded_jobs
