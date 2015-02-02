"""Used to test the jobs that are in deepy_jobs"""

import unittest

import arrow

import testing
import builder.build
import builder.deepy_test_jobs
import builder.deepy_jobs
import deepy.make

class DeepyTest(unittest.TestCase):
    """Used to test the general graph construction of the deepy jobs"""

    @testing.unit
    def test_get_command(self):
        # Given
        config = {
            "dimension2": True
        }

        jobs = [
                builder.deepy_test_jobs.TestGetCommandTop01(config=config),
                builder.deepy_test_jobs.TestGetCommandTop02(config=config),
                builder.deepy_test_jobs.TestGetCommandMiddle(config=config),
        ]

        build_context = {
                "start_time": arrow.get("2014-12-05-11-45"),
                "start_job": "job", # deepy.build.deepy_test_jobs.TestGetCommandMiddle,
        }

        build = builder.build.BuildGraph(jobs, config=config)
        build.construct_build_graph(build_context)
        (build.node
                ["target01-2014-12-05-00-01"]
                ["object"].exists) = True
        (build.node
                ["target01-2014-12-05-00-01"]
                ["object"].mtime) = 1
        (build.node
                ["target01-2014-12-05-00-02"]
                ["object"].exists) = True
        (build.node
                ["target01-2014-12-05-00-02"]
                ["object"].mtime) = 1
        (build.node
                ["target02-2014-12-05-00-01"]
                ["object"].exists) = True
        (build.node
                ["target02-2014-12-05-00-01"]
                ["object"].mtime) = 1
        # when
        build.write_build_graph("graph.dot")
        command = (build.node
                ["job_2014-12-05-00-00"]["object"].get_command(build))

        self.assertEqual(" dimension1,dimension2 target01-2014-12-05-00-01 target01-2014-12-05-00-02 target02-2014-12-05-00-01", command)

    @testing.unit
    def test_cube_sub_count_ip_version_5min(self):
        # Given
        rules_db = deepy.make.construct_rules()

        # When
        job = builder.deepy_jobs.DeepyDictJob('cube_sub_count_ip_version_5min', rules_db)

        # Then
        self.assertEquals(job.get_type(), 'target')