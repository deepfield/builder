"""All jobs in here are used to test the DeepyJob"""

import builder.deepy_jobs

# all of the following are for test_get_command
class TestGetCommandMiddle(builder.deepy_jobs.DeepyTimestampExpandedJob,
        builder.deepy_jobs.DeepyJob):
    """The class to get the command with"""
    unexpanded_id = "job"
    file_step = "3min"
    @staticmethod
    def get_dependencies(build_context=None):
        return {
            "depends": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "target01-%Y-%m-%d-%H-%M",
                    "1min"),
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "target02-%Y-%m-%d-%H-%M",
                    "1min"),
            ]
        }

    def get_command(self):
        return ("$A $D {deepy.build.deepy_test_jobs:TestGetCommandTop01} "
                "{deepy.build.deepy_test_jobs:TestGetCommandTop02}")

    def get_dimensions(self, build_context=None):
        print "here"
        dimensions = [
            "dimension1",
        ]
        if self.config.get("dimension2", False):
            dimensions.append("dimension2")
        return dimensions

class TestGetCommandTop01(builder.deepy_jobs.DeepyJob):
    """The class that supplies the targets"""
    unexpanded_id = "top_01"
    file_step = "1min"
    @staticmethod
    def get_targets(build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "target01-%Y-%m-%d-%H-%M",
                    "1min"),
            ]
        }

class TestGetCommandTop02(builder.deepy_jobs.DeepyTimestampExpandedJob,
        builder.deepy_jobs.DeepyJob):
    """The class that supplies the targets"""
    unexpanded_id = "top_02"
    file_step = "1min"
    @staticmethod
    def get_targets(build_context=None):
        return {
            "produces": [
                builder.expanders.TimestampExpander(
                    builder.targets.LocalFileSystemTarget,
                    "target02-%Y-%m-%d-%H-%M",
                    "1min"),
            ]
        }
