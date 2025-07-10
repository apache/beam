#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Unit test for the TestPipeline class"""

# pytype: skip-file

import logging
import unittest

import mock
from hamcrest.core.assert_that import assert_that as hc_assert_that
from hamcrest.core.base_matcher import BaseMatcher

from apache_beam.internal import pickler
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline


# A simple matcher that is ued for testing extra options appending.
class SimpleMatcher(BaseMatcher):
  def _matches(self, item):
    return True


class TestPipelineTest(unittest.TestCase):

  TEST_CASE = {
      'options': ['--test-pipeline-options', '--job=mockJob --male --age=1'],
      'expected_list': ['--job=mockJob', '--male', '--age=1'],
      'expected_dict': {
          'job': 'mockJob', 'male': True, 'age': 1
      }
  }

  # Used for testing pipeline option creation.
  class TestParsingOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument('--job', action='store', help='mock job')
      parser.add_argument('--male', action='store_true', help='mock gender')
      parser.add_argument('--age', action='store', type=int, help='mock age')

  def test_option_args_parsing(self):
    test_pipeline = TestPipeline(argv=self.TEST_CASE['options'])
    self.assertListEqual(
        sorted(test_pipeline.get_full_options_as_args()),
        sorted(self.TEST_CASE['expected_list']))

  def test_empty_option_args_parsing(self):
    test_pipeline = TestPipeline()
    self.assertListEqual([], test_pipeline.get_full_options_as_args())

  def test_create_test_pipeline_options(self):
    test_pipeline = TestPipeline(argv=self.TEST_CASE['options'])
    test_options = PipelineOptions(test_pipeline.get_full_options_as_args())
    self.assertLessEqual(
        self.TEST_CASE['expected_dict'].items(),
        test_options.get_all_options().items())

  EXTRA_OPT_CASES = [{
      'options': {
          'name': 'Mark'
      }, 'expected': ['--name=Mark']
  }, {
      'options': {
          'student': True
      }, 'expected': ['--student']
  }, {
      'options': {
          'student': False
      }, 'expected': []
  },
                     {
                         'options': {
                             'name': 'Mark', 'student': True
                         },
                         'expected': ['--name=Mark', '--student']
                     }]

  def test_append_extra_options(self):
    test_pipeline = TestPipeline()
    for case in self.EXTRA_OPT_CASES:
      opt_list = test_pipeline.get_full_options_as_args(**case['options'])
      self.assertListEqual(sorted(opt_list), sorted(case['expected']))

  def test_append_verifier_in_extra_opt(self):
    extra_opt = {'matcher': SimpleMatcher()}
    opt_list = TestPipeline().get_full_options_as_args(**extra_opt)
    _, value = opt_list[0].split('=', 1)
    matcher = pickler.loads(value)
    self.assertTrue(isinstance(matcher, BaseMatcher))
    hc_assert_that(None, matcher)

  def test_get_option(self):
    name, value = ('job', 'mockJob')
    test_pipeline = TestPipeline()
    test_pipeline.options_list = ['--%s=%s' % (name, value)]
    self.assertEqual(test_pipeline.get_option(name), value)

  def test_skip_IT(self):
    with TestPipeline(is_integration_test=True) as _:
      # Note that this will never be reached since it should be skipped above.
      pass
    self.fail()

  @mock.patch('apache_beam.testing.test_pipeline.Pipeline.run', autospec=True)
  def test_not_use_test_runner_api(self, mock_run):
    with TestPipeline(argv=['--not-use-test-runner-api'],
                      blocking=False) as test_pipeline:
      pass
    mock_run.assert_called_once_with(test_pipeline, test_runner_api=False)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
