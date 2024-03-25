import unittest

from data_processing.utils import TransformUtils


class TestUtils(unittest.TestCase):
    def test_clean_path(self):
        path = " "
        expected_path = ""
        self.assertEqual(expected_path, TransformUtils.clean_path(path))

        path = " s3://rel0_7/a/lang%3Den/dataset%3Dfreelaw \t"
        expected_path = "rel0_7/a/lang=en/dataset=freelaw/"
        self.assertEqual(expected_path, TransformUtils.clean_path(path))

        path = "rel0_7/a/lang%3Den/dataset=freelaw/"
        self.assertEqual(expected_path, TransformUtils.clean_path(path))

        path = "http://myhostname.com/rel0_7/a/lang%3Den/dataset=freelaw/"
        self.assertEqual(expected_path, TransformUtils.clean_path(path))
