"""Used to test the dependency types"""

import unittest

import builder.dependencies
import builder.tests_jobs

class DependenciesTest(unittest.TestCase):
    """Used to test the general dependecy api"""

    def test_get_dependencies_to_depends(self):
        # Given
        dependency_job1 = (builder.tests_jobs
                                .StandardDependsTargetTester())
        dependency_job2 = (builder.tests_jobs
                                .StandardDependsOneOrMoreTargetTester())

        expected_dependency1_1 = builder.dependencies.depends
        expected_dependency2_1 = builder.dependencies.depends
        expected_dependency2_2 = builder.dependencies.depends_one_or_more

        # When
        dependencies1 = dependency_job1.get_dependencies()

        dependency_types1 = []
        for dependency_type_string in dependencies1.iterkeys():
            dependency_type = getattr(builder.dependencies,
                                      dependency_type_string)
            dependency_types1.append(dependency_type)

        dependencies2 = dependency_job2.get_dependencies()

        dependency_types2 = []
        for dependency_type_string in dependencies2.iterkeys():
            dependency_type = getattr(builder.dependencies,
                                      dependency_type_string)
            dependency_types2.append(dependency_type)

        # Then
        self.assertIn(expected_dependency1_1, dependency_types1)
        self.assertIn(expected_dependency2_1, dependency_types2)
        self.assertIn(expected_dependency2_2, dependency_types2)

    def test_depends_fullfilled(self):
        # Given
        build_context = {}
        dependency_target1_1 = (builder.targets
                                    .LocalFileSystemTarget("unique_id1_1",
                                                          "unique_id1_1",
                                                          build_context))
        dependency_target2_1 = (builder.targets
                                    .LocalFileSystemTarget("unique_id2_1",
                                                          "unique_id2_1",
                                                          build_context))
        dependency_target3_1 = (builder.targets
                                    .LocalFileSystemTarget("unique_id3_1",
                                                          "unique_id3_1",
                                                          build_context))
        dependency_target3_2 = (builder.targets
                                    .LocalFileSystemTarget("unique_id3_2",
                                                          "unique_id3_2",
                                                          build_context))
        dependency_target3_3 = (builder.targets
                                    .LocalFileSystemTarget("unique_id3_3",
                                                          "unique_id3_3",
                                                          build_context))
        dependency_target4_1 = (builder.targets
                                    .LocalFileSystemTarget("unique_id4_1",
                                                          "unique_id4_1",
                                                          build_context))
        dependency_target4_2 = (builder.targets
                                    .LocalFileSystemTarget("unique_id4_2",
                                                          "unique_id4_2",
                                                          build_context))
        dependency_target4_3 = (builder.targets
                                    .LocalFileSystemTarget("unique_id4_3",
                                                          "unique_id4_3",
                                                          build_context))
        dependency_target5_1 = (builder.targets
                                    .LocalFileSystemTarget("unique_id5_1",
                                                          "unique_id5_1",
                                                          build_context))
        dependency_target5_2 = (builder.targets
                                    .LocalFileSystemTarget("unique_id5_2",
                                                          "unique_id5_2",
                                                          build_context))
        dependency_target5_3 = (builder.targets
                                    .LocalFileSystemTarget("unique_id5_3",
                                                          "unique_id5_3",
                                                          build_context))

        dependency_target1_1.exists = True
        dependency_target2_1.exists = False
        dependency_target3_1.exists = True
        dependency_target3_2.exists = True
        dependency_target3_3.exists = True
        dependency_target4_1.exists = True
        dependency_target4_2.exists = False
        dependency_target4_3.exists = True
        dependency_target5_1.exists = False
        dependency_target5_2.exists = False
        dependency_target5_3.exists = False

        dependency_target1_1.mtime = 100
        dependency_target2_1.mtime = None
        dependency_target3_1.mtime = 100
        dependency_target3_2.mtime = 100
        dependency_target3_3.mtime = 100
        dependency_target4_1.mtime = 100
        dependency_target4_2.mtime = None
        dependency_target4_3.mtime = 100
        dependency_target5_1.mtime = None
        dependency_target5_2.mtime = None
        dependency_target5_3.mtime = None

        dependency_targets1 = [
            dependency_target1_1,
        ]

        dependency_targets2 = [
            dependency_target2_1,
        ]

        dependency_targets3 = [
            dependency_target3_1,
            dependency_target3_2,
            dependency_target3_3,
        ]

        dependency_targets4 = [
            dependency_target4_1,
            dependency_target4_2,
            dependency_target4_3,
        ]

        dependency_targets5 = [
            dependency_target5_1,
            dependency_target5_2,
            dependency_target5_3,
        ]

        depends1 = builder.dependencies.depends
        depends2 = builder.dependencies.depends
        depends3 = builder.dependencies.depends
        depends4 = builder.dependencies.depends
        depends5 = builder.dependencies.depends

        # When
        depends_fullfilled1 = depends1(dependency_targets1)
        depends_fullfilled2 = depends2(dependency_targets2)
        depends_fullfilled3 = depends3(dependency_targets3)
        depends_fullfilled4 = depends4(dependency_targets4)
        depends_fullfilled5 = depends5(dependency_targets5)

        # Then
        self.assertTrue(depends_fullfilled1)
        self.assertFalse(depends_fullfilled2)
        self.assertTrue(depends_fullfilled3)
        self.assertFalse(depends_fullfilled4)
        self.assertFalse(depends_fullfilled5)

    def test_depends_one_or_more_fullfilled(self):
        # Given
        build_context = {}

        dependency_target1_1 = (builder.targets
                                    .LocalFileSystemTarget("unique_id1_1",
                                                          "unique_id1_1",
                                                          build_context))
        dependency_target2_1 = (builder.targets
                                    .LocalFileSystemTarget("unique_id2_1",
                                                          "unique_id2_1",
                                                          build_context))
        dependency_target3_1 = (builder.targets
                                    .LocalFileSystemTarget("unique_id3_1",
                                                          "unique_id3_1",
                                                          build_context))
        dependency_target3_2 = (builder.targets
                                    .LocalFileSystemTarget("unique_id3_2",
                                                          "unique_id3_2",
                                                          build_context))
        dependency_target3_3 = (builder.targets
                                    .LocalFileSystemTarget("unique_id3_3",
                                                          "unique_id3_3",
                                                          build_context))
        dependency_target4_1 = (builder.targets
                                    .LocalFileSystemTarget("unique_id4_1",
                                                          "unique_id4_1",
                                                          build_context))
        dependency_target4_2 = (builder.targets
                                    .LocalFileSystemTarget("unique_id4_2",
                                                          "unique_id4_2",
                                                          build_context))
        dependency_target4_3 = (builder.targets
                                    .LocalFileSystemTarget("unique_id4_3",
                                                          "unique_id4_3",
                                                          build_context))
        dependency_target5_1 = (builder.targets
                                    .LocalFileSystemTarget("unique_id5_1",
                                                          "unique_id5_1",
                                                          build_context))
        dependency_target5_2 = (builder.targets
                                    .LocalFileSystemTarget("unique_id5_2",
                                                          "unique_id5_2",
                                                          build_context))
        dependency_target5_3 = (builder.targets
                                    .LocalFileSystemTarget("unique_id5_3",
                                                          "unique_id5_3",
                                                          build_context))

        dependency_target1_1.exists = True
        dependency_target2_1.exists = False
        dependency_target3_1.exists = True
        dependency_target3_2.exists = True
        dependency_target3_3.exists = True
        dependency_target4_1.exists = True
        dependency_target4_2.exists = False
        dependency_target4_3.exists = True
        dependency_target5_1.exists = False
        dependency_target5_2.exists = False
        dependency_target5_3.exists = False

        dependency_target1_1.mtime = 100
        dependency_target2_1.mtime = None
        dependency_target3_1.mtime = 100
        dependency_target3_2.mtime = 100
        dependency_target3_3.mtime = 100
        dependency_target4_1.mtime = 100
        dependency_target4_2.mtime = None
        dependency_target4_3.mtime = 100
        dependency_target5_1.mtime = None
        dependency_target5_2.mtime = None
        dependency_target5_3.mtime = None

        dependency_targets1 = [
            dependency_target1_1,
        ]

        dependency_targets2 = [
            dependency_target2_1,
        ]

        dependency_targets3 = [
            dependency_target3_1,
            dependency_target3_2,
            dependency_target3_3,
        ]

        dependency_targets4 = [
            dependency_target4_1,
            dependency_target4_2,
            dependency_target4_3,
        ]

        dependency_targets5 = [
            dependency_target5_1,
            dependency_target5_2,
            dependency_target5_3,
        ]

        depends1 = builder.dependencies.depends_one_or_more
        depends2 = builder.dependencies.depends_one_or_more
        depends3 = builder.dependencies.depends_one_or_more
        depends4 = builder.dependencies.depends_one_or_more
        depends5 = builder.dependencies.depends_one_or_more

        # When
        depends_fullfilled1 = depends1(dependency_targets1)
        depends_fullfilled2 = depends2(dependency_targets2)
        depends_fullfilled3 = depends3(dependency_targets3)
        depends_fullfilled4 = depends4(dependency_targets4)
        depends_fullfilled5 = depends5(dependency_targets5)

        # Then
        self.assertTrue(depends_fullfilled1)
        self.assertFalse(depends_fullfilled2)
        self.assertTrue(depends_fullfilled3)
        self.assertTrue(depends_fullfilled4)
        self.assertFalse(depends_fullfilled5)
