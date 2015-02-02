
import unittest

import builder.deepy_jobs as deepy_jobs
import builder.deepy_targets as deepy_targets
import deepy.query_rules
import deepy.make
import deepy.timerange
arrow = deepy.timerange.arrow_factory

class DeepyDictJobTest(unittest.TestCase):

    def _simple_job(self):
        rules_db = deepy.make.construct_rules()
        return deepy_jobs.DeepyDictJob(
            'cube_drill_small_5min', rules_db
        )

    def _multitarget_job(self):
        rules_db = deepy.make.construct_rules()
        return deepy_jobs.DeepyDictJob(
            'cubes_from_h5flow_5min', rules_db
        )


    def test_multitarget_job_get_targets(self):

        # Given
        job = self._multitarget_job()

        # When
        targets = job.get_targets({})

        # Then
        produced_expanders = targets['produces']
        self.assertTrue(len(produced_expanders) > 1)
        unexpanded_ids = map(lambda x: x.unexpanded_id, produced_expanders)
        for produced in produced_expanders:
            self.assertTrue(produced.unexpanded_id in unexpanded_ids)
            self.assertTrue(produced.unexpanded_id is not None)

    def test_simple_job_get_target(self):
         # Given
        job = self._simple_job()

        # When
        targets = job.get_targets({})

        # Then
        produced_expanders = targets['produces']
        self.assertEquals(len(produced_expanders), 1)
        self.assertEquals(produced_expanders[0].unexpanded_id, '$(cubes_dir)/drill_small/minutes/cube.%Y-%m-%d-%H-%M.h5')


    def test_simple_job_get_dependencies(self):
         # Given
        job = self._simple_job()

        # When
        dependencies = job.get_dependencies({})
        depends = dependencies.get('depends')

        # Then
        self.assertTrue(len(dependencies) > 0)
        self.assertEquals(len(depends), 1)
        self.assertEquals(depends[0].unexpanded_id, '$(cubes_dir)/drill1/minutes/cube.%Y-%m-%d-%H-%M.h5')

    def test_multitarget_job_get_dependencies(self):
        # Given
        job = self._multitarget_job()

        # When
        dependencies = job.get_dependencies({})
        depends = dependencies.get('depends')

        # Then
        self.assertTrue(len(dependencies) > 0)
        self.assertEquals(len(depends), 1)
        self.assertEquals(depends[0].unexpanded_id, '$(h5flow_dir)/flow.%Y-%m-%d-%H-%M.h5')


class ImpalaTimePartitionedTargetTest(unittest.TestCase):

    def _get_test_targets(self):
        return [
            deepy_targets.ImpalaTimePartitionedTarget('foo', 'foo-12-25-20',
                {'start_time': arrow.get('2014-01-01'), 'end_time': arrow.get('2014-01-02')}, 'foo', '1d')
        ]

    def test_get_bulk_exists_mtime(self):
        # Given
        targets = self._get_test_targets()

        # When
        results = deepy_targets.ImpalaTimePartitionedTarget.get_bulk_exists_mtime(targets)

        # Then
        return results