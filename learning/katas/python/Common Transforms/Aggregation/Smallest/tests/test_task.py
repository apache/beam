import unittest

from test_helper import test_is_not_empty, get_file_output


class TestCase(unittest.TestCase):
    def test_not_empty(self):
        self.assertTrue(test_is_not_empty(), 'The output is empty')

    def test_output(self):
        output = get_file_output(path='task.py')

        answer = '[1]'

        self.assertIn(answer, output, "Incorrect output. Use the Top.Smallest combiner.")
