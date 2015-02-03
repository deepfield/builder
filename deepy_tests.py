"""Used to test the jobs that are in deepy_jobs"""

import unittest

import arrow
import mock

import testing
import builder.build
import builder.deepy_test_jobs
import builder.deepy_jobs
import builder.deepy_build
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
        command = (build.node
                ["job_2014-12-05-00-00"]["object"].get_command(build))

        # Then
        self.assertEqual(" dimension1,dimension2 target01-2014-12-05-00-01 target01-2014-12-05-00-02 target02-2014-12-05-00-01", command)

    @testing.unit
    def test_cube_sub_count_ip_version_5min(self):
        # Given
        rules_db = deepy.make.construct_rules()

        # When
        job = builder.deepy_jobs.DeepyDictJob('cube_sub_count_ip_version_5min', rules_db)

        # Then
        self.assertEquals(job.get_type(), 'target')

    @testing.unit
    def test_drill1_hour_has_dimensions(self):
        # Given
        rules_db = deepy.make.construct_rules()

        # When
        job = builder.deepy_jobs.DeepyDictJob('cube_drill1_hour', rules_db)

        # Then
        self.assertTrue(len(job.get_dimensions())>0)

    @testing.unit
    def test_get_command_with_format_args(self):
        # Given
        rules_db = deepy.make.construct_rules()
        t = arrow.get()
        build_context = {
            'start_time':t,
            'end_time': t,
            'exact': True
        }
        build_graph = builder.deepy_build.DeepyBuild()

        # When
        job = builder.deepy_jobs.DeepyDictJob('cube_aspaths_remote3_hour', rules_db)
        build_context['start_job'] = job.unexpanded_id
        build_graph.construct_build_graph(build_context)
        states = job.expand(build_context)


        # Then
        self.assertTrue(len(states) == 1)
        command = states[0].get_command(build_graph)

    @testing.unit
    def test_basic_command_substitution(self):
        # Given
        command = ('cube_op.py $A -o '
                   '/Users/matt/env/deepfield-deploy/pipedream/cache/cubes/drill_small/minutes/cube.2014-01-27-08-55.h5 '
                   '-t 300  -A group_other(origin_asn.local,null,<c.top_origins>)  '
                   '-A group_other(aspaths.local,null,<c.top_aspaths>) -A group_other(origin_asn.remote,null,<c.top_origins>) '
                   '--arg_join c.top_origins=\'$(cubes_dir)/origin_asn.remote2/months/top_list.2014-01.json.gz\' '
                   '-A group_other(aspaths.remote,null,<c.top_aspaths>) --arg_join '
                   'c.top_aspaths=\'$(cubes_dir)/aspaths.remote2/months/top_list.2014-01.json.gz\' '
                   '-A group_other(sites,null,<c.top_sites>) -A group_other(company,null,<c.top_companies>) '
                   '--arg_join c.top_companies=\'$(cubes_dir)/company2/months/top_list.2014-01.json.gz\' '
                   '--arg_join c.top_sites=\'$(cubes_dir)/sites2/months/top_list.2014-01.json.gz\' {cube_drill1_5min}')
        ts = arrow.get("2014-01-01")
        # When
        substituted = builder.deepy_util.basic_command_substitution(command, ts)

        # Then
        self.assertEquals(substituted, """cube_op.py $A -o /Users/matt/env/deepfield-deploy/pipedream/cache/cubes/drill_small/minutes/cube.2014-01-27-08-55.h5 -t 300  -A group_other(origin_asn.local,null,<c.top_origins>)  -A group_other(aspaths.local,null,<c.top_aspaths>) -A group_other(origin_asn.remote,null,<c.top_origins>) --arg_join c.top_origins='$(cubes_dir)/origin_asn.remote2/months/top_list.2014-01.json.gz' -A group_other(aspaths.remote,null,<c.top_aspaths>) --arg_join c.top_aspaths='$(cubes_dir)/aspaths.remote2/months/top_list.2014-01.json.gz' -A group_other(sites,null,<c.top_sites>) -A group_other(company,null,<c.top_companies>) --arg_join c.top_companies='$(cubes_dir)/company2/months/top_list.2014-01.json.gz' --arg_join c.top_sites='$(cubes_dir)/sites2/months/top_list.2014-01.json.gz' {cube_drill1_5min}""")

    @testing.unit
    def test_deepy_command_substitution(self):
        # Given
        fmt_str = '$(heartbeat_dir)/$(vm_uuid)/vm/vm.2014-01-06-17-25.json.gz'
        config = mock.Mock()
        config.heartbeat_dir = '/foo'
        config.vm_uuid = 'bar'

        # When
        substituted = builder.deepy_util.deepy_command_substitution(fmt_str, config=config)

        # Then
        self.assertEquals("/foo/bar/vm/vm.2014-01-06-17-25.json.gz", substituted)

    @testing.unit
    def test_deepy_command_substitution_with_null(self):
        # Given
        fmt_str = '$(heartbeat_dir)/$(vm_uuid)/vm/vm.2014-01-06-17-25.json.gz'
        config = mock.Mock()
        config.heartbeat_dir = '/foo'
        config.vm_uuid = None

        # When
        substituted = builder.deepy_util.deepy_command_substitution(fmt_str, config=config)

        # Then
        self.assertEquals("/foo/None/vm/vm.2014-01-06-17-25.json.gz", substituted)
