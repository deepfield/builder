"""Used to test the various functions used to implement the
build api specifically the target ones
"""

import fnmatch
import unittest

import mock

import testing
import builder.targets


class LocalFileSystemTargetTest(unittest.TestCase):
    """Used to test specifically the local file system implemention of
    a target
    """
    @staticmethod
    def mock_mtime_generator(file_dict):
        """Used to generate a fake os.stat

        takes in a list of files and returns a function that will return the
        mtime corresponding to the path passed to it
        """
        def mock_mtime(path):
            """Returns the mtime corresponding to the path

            raises:
                OSError: if the path is not in the file_dict
            """
            if path not in file_dict:
                raise OSError(2, "No such file or directory")
            mock_stat = mock.Mock()
            mock_stat.st_mtime = file_dict[path]
            return mock_stat
        return mock_mtime


    @testing.unit
    def test_non_cached_mtime(self):
        # given
        path1 = "local_path/1"
        path2 = "local_path/2"

        mtimes = {
                "local_path/1": 1
        }

        mock_mtime = LocalFileSystemTargetTest.mock_mtime_generator(mtimes)

        # when
        build_context = {}
        file1 = builder.targets.LocalFileSystemTarget(
                path1, path1, build_context)
        file2 = builder.targets.LocalFileSystemTarget(
                path2, path2, build_context)

        with mock.patch("os.stat", mock_mtime):
            mtime1 = (builder.targets.LocalFileSystemTarget
                        .non_cached_mtime(file1.unique_id))
            mtime2 = (builder.targets.LocalFileSystemTarget
                        .non_cached_mtime(file2.unique_id))

        self.assertEqual(mtime1, 1)
        self.assertIsNone(mtime2)

    @testing.unit
    def test_get_exists(self):
        # given
        path1 = "local_path/1"
        path2 = "local_path/2"

        mtimes = {
            "local_path/1": 1
        }

        mock_mtime = LocalFileSystemTargetTest.mock_mtime_generator(mtimes)

        # when
        build_context = {}
        file1 = builder.targets.LocalFileSystemTarget(
                path1, path1, build_context)
        file2 = builder.targets.LocalFileSystemTarget(
                path2, path2, build_context)

        with mock.patch("os.stat", mock_mtime):
            exists1 = file1.get_exists()
            exists2 = file2.get_exists()

        self.assertTrue(exists1)
        self.assertFalse(exists2)

    @testing.unit
    def test_get_mtime(self):
        # given
        path1 = "local_path/1"
        path2 = "local_path/2"

        mtimes = {
            "local_path/1": 1
        }

        mock_mtime = LocalFileSystemTargetTest.mock_mtime_generator(mtimes)

        # when
        build_context = {}
        file1 = builder.targets.LocalFileSystemTarget(
                path1, path1, build_context)
        file2 = builder.targets.LocalFileSystemTarget(
                path2, path2, build_context)

        with mock.patch("os.stat", mock_mtime):
            mtime1 = file1.get_mtime()
            mtime2 = file2.get_mtime()

        self.assertEqual(mtime1, 1)
        self.assertIsNone(mtime2)

    @testing.unit
    def test_get_bulk_exists_mtime(self):
        # given
        path1 = "local_path/1"
        path2 = "local_path/2"

        mtimes = {
            "local_path/1": 1
        }

        mock_mtime = LocalFileSystemTargetTest.mock_mtime_generator(mtimes)

        # when
        build_context = {}
        file1 = builder.targets.LocalFileSystemTarget(
                path1, path1, build_context)

        mtime_fetcher = file1.get_bulk_exists_mtime

        with mock.patch("os.stat", mock_mtime):
            mtimes_exists = mtime_fetcher([path1, path2])
            file1_exists = mtimes_exists[path1]["exists"]
            file2_exists = mtimes_exists[path2]["exists"]
            file1_mtime = mtimes_exists[path1]["mtime"]
            file2_mtime = mtimes_exists[path2]["mtime"]

        self.assertTrue(file1_exists)
        self.assertFalse(file2_exists)
        self.assertEqual(file1_mtime, 1)
        self.assertEqual(file2_mtime, None)


class S3BackedLocalFileSystemTargetTest(unittest.TestCase):
    """Used to test specifically the local file system implemention of
    a target
    """
    @staticmethod
    def mock_s3_mtime_generator(file_dict):
        """Used to generate a fake os.stat

        takes in a list of files and returns a function that will return the
        mtime corresponding to the path passed to it
        """
        def mock_s3_mtime(path):
            """Returns the mtime corresponding to the path

            raises:
                OSError: if the path is not in the file_dict
            """
            path = path[0]
            if path not in file_dict:
                return {path: None}
            return {path: file_dict[path]}
        return mock_s3_mtime

    @staticmethod
    def mock_s3_bulk_mtime_generator(file_dict):
        """Used to generate a fake os.stat

        takes in a list of files and returns a function that will return the
        mtime corresponding to the path passed to it
        """
        def mock_bulk_mtime(paths):
            """Returns the mtime corresponding to the path

            raises:
                OSError: if the path is not in the file_dict
            """
            mtime_dict = {}
            for path in paths:
                for file in file_dict:
                    if file.startswith(path):
                        mtime_dict[file] = file_dict[file]
            return mtime_dict
        return mock_bulk_mtime

    @testing.unit
    def test_get_exists(self):
        # given
        path1 = "local_path/1"
        path2 = "local_path/2"
        path3 = "local_path/3"
        path4 = "local_path/4"

        local_mtimes = {
            path1: 1,
            path2: 2,
        }

        remote_mtimes = {
            path1: 3,
            path4: 4,
        }

        mock_local_mtime = (LocalFileSystemTargetTest
                .mock_mtime_generator(local_mtimes))

        mock_remote_mtime = (S3BackedLocalFileSystemTargetTest
                .mock_s3_mtime_generator(remote_mtimes))

        # when
        build_context = {}
        file1 = builder.targets.S3BackedLocalFileSystemTarget(
                path1, path1, build_context)
        file2 = builder.targets.S3BackedLocalFileSystemTarget(
                path2, path2, build_context)
        file3 = builder.targets.S3BackedLocalFileSystemTarget(
                path3, path3, build_context)
        file4 = builder.targets.S3BackedLocalFileSystemTarget(
                path4, path4, build_context)

        with mock.patch("os.stat", mock_local_mtime), \
                mock.patch("deepy.store.ls_files_remote", mock_remote_mtime):
            exists1 = file1.get_exists()
            exists2 = file2.get_exists()
            exists3 = file3.get_exists()
            exists4 = file4.get_exists()

        # then
        self.assertTrue(exists1)
        self.assertTrue(exists2)
        self.assertFalse(exists3)
        self.assertTrue(exists4)

    @testing.unit
    def test_get_mtime(self):
        # given
        path1 = "local_path/1"
        path2 = "local_path/2"
        path3 = "local_path/3"
        path4 = "local_path/4"

        local_mtimes = {
            path1: 1,
            path2: 2,
        }

        remote_mtimes = {
            path1: 3,
            path4: 4,
        }

        mock_local_mtime = (LocalFileSystemTargetTest
                .mock_mtime_generator(local_mtimes))

        mock_remote_mtime = (S3BackedLocalFileSystemTargetTest
                .mock_s3_mtime_generator(remote_mtimes))

        # when
        build_context = {}
        file1 = builder.targets.S3BackedLocalFileSystemTarget(
                path1, path1, build_context)
        file2 = builder.targets.S3BackedLocalFileSystemTarget(
                path2, path2, build_context)
        file3 = builder.targets.S3BackedLocalFileSystemTarget(
                path3, path3, build_context)
        file4 = builder.targets.S3BackedLocalFileSystemTarget(
                path4, path4, build_context)

        with mock.patch("os.stat", mock_local_mtime), \
                mock.patch("deepy.store.ls_files_remote", mock_remote_mtime):
            mtime1 = file1.get_mtime()
            mtime2 = file2.get_mtime()
            mtime3 = file3.get_mtime()
            mtime4 = file4.get_mtime()

        # then
        self.assertEqual(mtime1, 3)
        self.assertEqual(mtime2, 2)
        self.assertIsNone(mtime3)
        self.assertEqual(mtime4, 4)

    @testing.unit
    def test_get_bulk_exists_mtime(self):
        # given
        path1 = "local_path/1"
        path2 = "local_path/2"
        path3 = "local_path/3"
        path4 = "local_path/4"

        local_mtimes = {
            path1: 1,
            path2: 2,
        }

        remote_mtimes = {
            path1: 3,
            path4: 4,
        }

        mock_local_mtime = (LocalFileSystemTargetTest
                .mock_mtime_generator(local_mtimes))

        mock_remote_mtime = (S3BackedLocalFileSystemTargetTest
                .mock_s3_bulk_mtime_generator(remote_mtimes))

        # when
        build_context = {}
        file1 = builder.targets.S3BackedLocalFileSystemTarget(
                path1, path1, build_context)

        mtime_fetcher = file1.get_bulk_exists_mtime

        with mock.patch("os.stat", mock_local_mtime), \
                mock.patch("deepy.store.list_files_remote", mock_remote_mtime):
            mtimes_exists = mtime_fetcher([path1, path2, path3, path4])
            file1_exists = mtimes_exists[path1]["exists"]
            file2_exists = mtimes_exists[path2]["exists"]
            file3_exists = mtimes_exists[path3]["exists"]
            file4_exists = mtimes_exists[path4]["exists"]
            file1_mtime = mtimes_exists[path1]["mtime"]
            file2_mtime = mtimes_exists[path2]["mtime"]
            file3_mtime = mtimes_exists[path3]["mtime"]
            file4_mtime = mtimes_exists[path4]["mtime"]

        self.assertTrue(file1_exists)
        self.assertTrue(file2_exists)
        self.assertFalse(file3_exists)
        self.assertTrue(file4_exists)
        self.assertEqual(file1_mtime, 3)
        self.assertEqual(file2_mtime, 2)
        self.assertIsNone(file3_mtime)
        self.assertEqual(file4_mtime, 4)


class GlobLocalFileSystemTargetTest(unittest.TestCase):
    """Used to test specifically the local file system implemention of
    a target
    """

    @staticmethod
    def mock_glob_list_generator(file_dict):
        """Used to generate a fake os.stat

        takes in a list of files and returns a function that will return the
        mtime corresponding to the path passed to it
        """
        def mock_glob_list(path_pattern):
            """Returns the mtime corresponding to the path

            raises:
                OSError: if the path is not in the file_dict
            """
            path_list = []
            for path in file_dict:
                if fnmatch.fnmatchcase(path, path_pattern):
                    path_list.append(path)
            return path_list
        return mock_glob_list

    @testing.unit
    def test_get_exists(self):
        # given
        glob1 = "local_path/1/*.gz"
        glob1_path1 = "local_path/1/1.gz"
        glob1_path2 = "local_path/1/2.gz"

        glob2 = "local_path/2/*.gz"

        mtimes = {
            glob1_path1: 1,
            glob1_path2: 2,
        }

        mock_mtime = LocalFileSystemTargetTest.mock_mtime_generator(mtimes)

        mock_glob = (GlobLocalFileSystemTargetTest
                .mock_glob_list_generator(mtimes))

        # when
        build_context = {}

        glob_target1 = builder.targets.GlobLocalFileSystemTarget(
                glob1, glob1, build_context)
        glob_target2 = builder.targets.GlobLocalFileSystemTarget(
                glob2, glob2, build_context)

        with mock.patch("os.stat", mock_mtime), \
                mock.patch("glob.glob", mock_glob):
            exists1 = glob_target1.get_exists()
            exists2 = glob_target2.get_exists()

        self.assertTrue(exists1)
        self.assertFalse(exists2)

    @testing.unit
    def test_get_mtime(self):
        # given
        glob1 = "local_path/1/*.gz"
        glob1_path1 = "local_path/1/1.gz"
        glob1_path2 = "local_path/1/2.gz"

        glob2 = "local_path/2/*.gz"

        mtimes = {
            glob1_path1: 1,
            glob1_path2: 2,
        }

        mock_mtime = LocalFileSystemTargetTest.mock_mtime_generator(mtimes)

        mock_glob = (GlobLocalFileSystemTargetTest
                .mock_glob_list_generator(mtimes))

        # when
        build_context = {}
        glob_target1 = builder.targets.GlobLocalFileSystemTarget(
                glob1, glob1, build_context)
        glob_target2 = builder.targets.GlobLocalFileSystemTarget(
                glob2, glob2, build_context)

        with mock.patch("os.stat", mock_mtime), \
                mock.patch("glob.glob", mock_glob):
            mtime1 = glob_target1.get_mtime()
            mtime2 = glob_target2.get_mtime()

        self.assertEqual(mtime1, 2)
        self.assertIsNone(mtime2)

    @testing.unit
    def test_get_bulk_exists_mtime(self):
        # given
        glob1 = "local_path/1/*.gz"
        glob1_path1 = "local_path/1/1.gz"
        glob1_path2 = "local_path/1/2.gz"

        glob2 = "local_path/2/*.gz"

        mtimes = {
            glob1_path1: 1,
            glob1_path2: 2,
        }

        mock_mtime = LocalFileSystemTargetTest.mock_mtime_generator(mtimes)

        mock_glob = (GlobLocalFileSystemTargetTest
                .mock_glob_list_generator(mtimes))

        build_context = {}

        glob_target1 = builder.targets.GlobLocalFileSystemTarget(
                glob1, glob1, build_context)

        mtime_fetcher = glob_target1.get_bulk_exists_mtime

        with mock.patch("os.stat", mock_mtime), \
                mock.patch("glob.glob", mock_glob):
                mtimes_exists = mtime_fetcher([glob1, glob2])
                file1_exists = mtimes_exists[glob1]["exists"]
                file2_exists = mtimes_exists[glob2]["exists"]
                file1_mtime = mtimes_exists[glob1]["mtime"]
                file2_mtime = mtimes_exists[glob2]["mtime"]

        self.assertTrue(file1_exists)
        self.assertFalse(file2_exists)
        self.assertEqual(file1_mtime, 2)
        self.assertEqual(file2_mtime, None)


class S3BackedGlobLocalFileSystemTargetTest(unittest.TestCase):
    """Used to test specifically the local file system implemention of
    a target
    """
    @testing.unit
    def test_get_exists(self):
        # given
        # exists on both remote and local
        glob1 = "local_path/1/*.gz"
        glob1_path1 = "local_path/1/1.gz"
        glob1_path2 = "local_path/1/2.gz"

        # exists on neither
        glob2 = "local_path/2/*.gz"

        # exists on remote only
        glob3 = "local_path/3/*.gz"
        glob3_path1 = "local_path/3/1.gz"
        glob3_path2 = "local_path/3/2.gz"

        # exists on local only
        glob4 = "local_path/4/*.gz"
        glob4_path1 = "local_path/4/1.gz"
        glob4_path2 = "local_path/4/2.gz"

        # tests pattern matching correctness
        glob5 = "local_path/*1/5*.gz"
        glob5_path1 = "local_path/151/515.gz"
        glob5_path2 = "local_path/251/525.gz"
        close_glob5_path1 = "local_path/15/25.gz"

        local_mtimes = {
            glob1_path1: 1,
            glob1_path2: 2,
            glob4_path1: 3,
            glob4_path2: 4,
        }

        remote_mtimes = {
            glob1_path1: 3,
            glob1_path2: 5,
            glob3_path1: 2,
            glob3_path2: 3,
            glob5_path1: 4,
            glob5_path2: 5,
            close_glob5_path1: 6,
        }

        mock_local_mtime = (LocalFileSystemTargetTest
                .mock_mtime_generator(local_mtimes))

        mock_remote_mtime = (S3BackedLocalFileSystemTargetTest
                .mock_s3_bulk_mtime_generator(remote_mtimes))

        mock_glob = (GlobLocalFileSystemTargetTest
                .mock_glob_list_generator(local_mtimes))

        # when
        build_context = {}

        glob_target1 = builder.targets.S3BackedGlobLocalFileSystemTarget(
                glob1, glob1, build_context)
        glob_target2 = builder.targets.S3BackedGlobLocalFileSystemTarget(
                glob2, glob2, build_context)
        glob_target3 = builder.targets.S3BackedGlobLocalFileSystemTarget(
                glob3, glob3, build_context)
        glob_target4 = builder.targets.S3BackedGlobLocalFileSystemTarget(
                glob4, glob4, build_context)
        glob_target5 = builder.targets.S3BackedGlobLocalFileSystemTarget(
                glob5, glob5, build_context)

        with mock.patch("os.stat", mock_local_mtime), \
                mock.patch("deepy.store.list_files_remote", mock_remote_mtime), \
                mock.patch("glob.glob", mock_glob):
            exists1 = glob_target1.get_exists()
            exists2 = glob_target2.get_exists()
            exists3 = glob_target3.get_exists()
            exists4 = glob_target4.get_exists()
            exists5 = glob_target5.get_exists()

        self.assertTrue(exists1)
        self.assertFalse(exists2)
        self.assertTrue(exists3)
        self.assertTrue(exists4)
        self.assertTrue(exists5)

    @testing.unit
    def test_get_mtime(self):
        # given
        # exists on both remote and local
        glob1 = "local_path/1/*.gz"
        glob1_path1 = "local_path/1/1.gz"
        glob1_path2 = "local_path/1/2.gz"

        # exists on neither
        glob2 = "local_path/2/*.gz"

        # exists on remote only
        glob3 = "local_path/3/*.gz"
        glob3_path1 = "local_path/3/1.gz"
        glob3_path2 = "local_path/3/2.gz"

        # exists on local only
        glob4 = "local_path/4/*.gz"
        glob4_path1 = "local_path/4/1.gz"
        glob4_path2 = "local_path/4/2.gz"

        # tests pattern matching correctness
        glob5 = "local_path/*1/5*.gz"
        glob5_path1 = "local_path/151/515.gz"
        glob5_path2 = "local_path/251/525.gz"
        close_glob5_path1 = "local_path/15/25.gz"

        local_mtimes = {
            glob1_path1: 1,
            glob1_path2: 2,
            glob4_path1: 3,
            glob4_path2: 4,
        }

        remote_mtimes = {
            glob1_path1: 3,
            glob1_path2: 5,
            glob3_path1: 2,
            glob3_path2: 3,
            glob5_path1: 4,
            glob5_path2: 5,
            close_glob5_path1: 6,
        }

        mock_local_mtime = (LocalFileSystemTargetTest
                .mock_mtime_generator(local_mtimes))

        mock_remote_mtime = (S3BackedLocalFileSystemTargetTest
                .mock_s3_bulk_mtime_generator(remote_mtimes))

        mock_glob = (GlobLocalFileSystemTargetTest
                .mock_glob_list_generator(local_mtimes))

        build_context = {}

        # when
        glob_target1 = builder.targets.S3BackedGlobLocalFileSystemTarget(
                glob1, glob1, build_context)
        glob_target2 = builder.targets.S3BackedGlobLocalFileSystemTarget(
                glob2, glob2, build_context)
        glob_target3 = builder.targets.S3BackedGlobLocalFileSystemTarget(
                glob3, glob3, build_context)
        glob_target4 = builder.targets.S3BackedGlobLocalFileSystemTarget(
                glob4, glob4, build_context)
        glob_target5 = builder.targets.S3BackedGlobLocalFileSystemTarget(
                glob5, glob5, build_context)

        with mock.patch("os.stat", mock_local_mtime), \
                mock.patch("deepy.store.list_files_remote", mock_remote_mtime), \
                mock.patch("glob.glob", mock_glob):
            mtime1 = glob_target1.get_mtime()
            mtime2 = glob_target2.get_mtime()
            mtime3 = glob_target3.get_mtime()
            mtime4 = glob_target4.get_mtime()
            mtime5 = glob_target5.get_mtime()

        self.assertEqual(mtime1, 5)
        self.assertIsNone(mtime2)
        self.assertEqual(mtime3, 3)
        self.assertEqual(mtime4, 4)
        self.assertEqual(mtime5, 5)

    @testing.unit
    def test_get_bulk_exists_mtime(self):
        # given
        # exists on both remote and local
        glob1 = "local_path/1/*.gz"
        glob1_path1 = "local_path/1/1.gz"
        glob1_path2 = "local_path/1/2.gz"

        # exists on neither
        glob2 = "local_path/2/*.gz"

        # exists on remote only
        glob3 = "local_path/3/*.gz"
        glob3_path1 = "local_path/3/1.gz"
        glob3_path2 = "local_path/3/2.gz"

        # exists on local only
        glob4 = "local_path/4/*.gz"
        glob4_path1 = "local_path/4/1.gz"
        glob4_path2 = "local_path/4/2.gz"

        # tests pattern matching correctness
        glob5 = "local_path/*1/5*.gz"
        glob5_path1 = "local_path/151/515.gz"
        glob5_path2 = "local_path/251/525.gz"
        close_glob5_path1 = "local_path/15/25.gz"

        local_mtimes = {
            glob1_path1: 1,
            glob1_path2: 2,
            glob4_path1: 3,
            glob4_path2: 4,
        }

        remote_mtimes = {
            glob1_path1: 3,
            glob1_path2: 5,
            glob3_path1: 2,
            glob3_path2: 3,
            glob5_path1: 4,
            glob5_path2: 5,
            close_glob5_path1: 6,
        }

        mock_mtime = (LocalFileSystemTargetTest
                .mock_mtime_generator(local_mtimes))

        mock_glob = (GlobLocalFileSystemTargetTest
                .mock_glob_list_generator(local_mtimes))

        mock_remote_mtime = (S3BackedLocalFileSystemTargetTest
                .mock_s3_bulk_mtime_generator(remote_mtimes))

        build_context = {}

        glob_target1 = (builder.targets
                .S3BackedGlobLocalFileSystemTarget(
                        glob1, glob1, build_context))

        mtime_fetcher = glob_target1.get_bulk_exists_mtime

        with mock.patch("os.stat", mock_mtime), \
                mock.patch("deepy.store.list_files_remote",
                        mock_remote_mtime), \
                mock.patch("glob.glob", mock_glob):
                mtimes_exists = mtime_fetcher([glob1, glob2, glob3,
                        glob4, glob5])
                glob1_exists = mtimes_exists[glob1]["exists"]
                glob2_exists = mtimes_exists[glob2]["exists"]
                glob3_exists = mtimes_exists[glob3]["exists"]
                glob4_exists = mtimes_exists[glob4]["exists"]
                glob5_exists = mtimes_exists[glob5]["exists"]
                glob1_mtime = mtimes_exists[glob1]["mtime"]
                glob2_mtime = mtimes_exists[glob2]["mtime"]
                glob3_mtime = mtimes_exists[glob3]["mtime"]
                glob4_mtime = mtimes_exists[glob4]["mtime"]
                glob5_mtime = mtimes_exists[glob5]["mtime"]

        self.assertTrue(glob1_exists)
        self.assertFalse(glob2_exists)
        self.assertTrue(glob3_exists)
        self.assertTrue(glob4_exists)
        self.assertTrue(glob5_exists)
        self.assertEqual(glob1_mtime, 5)
        self.assertIsNone(glob2_mtime)
        self.assertEqual(glob3_mtime, 3)
        self.assertEqual(glob4_mtime, 4)
        self.assertEqual(glob5_mtime, 5)
