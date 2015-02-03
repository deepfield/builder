"""Used to test the jobs that are in deepy_jobs"""

import unittest

import arrow
import mock

import testing
import builder.build
import builder.jobs
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
                TestGetCommandTop01(config=config),
                TestGetCommandTop02(config=config),
                TestGetCommandMiddle(config=config),
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

    @testing.unit
    def test_drill_summary_commands(self):
        #  Given
        job_id = 'drill_cdn_summary'
        rules_db = deepy.make.construct_rules()
        build_graph = builder.deepy_build.DeepyBuild()
        t = arrow.get('2014-01-01')
        build_context = {
            'start_time':t,
            'end_time': t,
            'exact': True
        }

        # When
        job = builder.deepy_jobs.DeepyDictJob(job_id, rules_db)
        build_context['start_job'] = job.unexpanded_id
        build_graph.construct_build_graph(build_context)
        commands = []
        for node_id, node in build_graph.node.iteritems():
            obj = node['object']
            if isinstance(obj, builder.jobs.JobState) and obj.get_should_run(build_graph):
                commands.append(obj.get_command(build_graph))


        # Then
        correct = ('bundle2.py -M drill_day_cdn_summary -m ' +
                   deepy.cfg.cubes_dir + '/drill/cdn/days/markers/summary.2014-01-01.marker '
                   '-t 2014-01-01')
        self.assertEquals(commands[0], correct)
        self.assertEquals(1, len(commands))

    @testing.unit
    def test_drill_commands(self):
        #  Given
        job_id = 'drill_cdn'
        rules_db = deepy.make.construct_rules()
        build_graph = builder.deepy_build.DeepyBuild()
        t = arrow.get('2014-01-01')
        build_context = {
            'start_time':t,
            'end_time': t,
            'exact': True
        }

        # When
        job = builder.deepy_jobs.DeepyDictJob(job_id, rules_db)
        build_context['start_job'] = job.unexpanded_id
        build_graph.construct_build_graph(build_context)
        commands = []
        for node_id, node in build_graph.node.iteritems():
            obj = node['object']
            if isinstance(obj, builder.jobs.JobState) and obj.get_should_run(build_graph):
                commands.append(obj.get_command(build_graph))


        # Then

        correct = ('bundle2.py -M drill_day_cdn -m ' +
                   deepy.cfg.cubes_dir + '/drill/cdn/days/markers/drill.2014-01-01.marker '
                   '-t 2014-01-01')
        self.assertEquals(commands[0], correct)
        self.assertEquals(1, len(commands))



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
        return ("$A $D {top_01} "
                "{top_02}")

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