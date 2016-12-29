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

import unittest

from apache_beam.test_pipeline import TestPipeline
from apache_beam.utils.pipeline_options import PipelineOptions


class TestPipelineTest(unittest.TestCase):

  TEST_CASE = {'options':
                   ['--test-pipeline-options', '--job=mockJob --male --age=1'],
               'expected_list': ['--job=mockJob', '--male', '--age=1'],
               'expected_dict': {'job': 'mockJob',
                                 'male': True,
                                 'age': 1}}

  def setUp(self):
    self.pipeline = TestPipeline()

  # Used for testing pipeline option creation.
  class TestParsingOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_argument('--job', action='store', help='mock job')
      parser.add_argument('--male', action='store_true', help='mock gender')
      parser.add_argument('--age', action='store', type=int, help='mock age')

  def test_option_args_parsing(self):
    self.assertListEqual(
        self.pipeline.get_test_option_args(argv=self.TEST_CASE['options']),
        self.TEST_CASE['expected_list'])

  def test_create_test_pipeline_options(self):
    test_options = PipelineOptions(
        self.pipeline.get_test_option_args(self.TEST_CASE['options']))
    self.assertDictContainsSubset(
        self.TEST_CASE['expected_dict'], test_options.get_all_options())

  def test_append_extra_options(self):
    extra_opt = {'name': 'Mark'}
    options_list = self.pipeline.get_test_option_args(
        argv=self.TEST_CASE['options'], **extra_opt)
    expected_list = self.TEST_CASE['expected_list'] + ['--name=Mark']
    self.assertListEqual(expected_list, options_list)
