# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for the pipeline options module."""

import logging
import unittest

from google.cloud.dataflow.utils.options import PipelineOptions


class SetupTest(unittest.TestCase):

  def test_get_unknown_args(self):

    # Used for testing newly added flags.
    class MockOptions(PipelineOptions):

      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_argument('--mock_flag',
                            action='store_true',
                            help='Enable work item profiling')

    test_cases = [
        {'flags': ['--num_workers', '5'],
         'expected': {'num_workers': 5, 'mock_flag': False}},
        {
            'flags': [
                '--profile', '--profile_location', 'gs://bucket/', 'ignored'],
            'expected': {
                'profile': True, 'profile_location': 'gs://bucket/',
                'mock_flag': False}
        },
        {'flags': ['--num_workers', '5', '--mock_flag'],
         'expected': {'num_workers': 5, 'mock_flag': True}},
    ]

    for case in test_cases:
      options = PipelineOptions(flags=case['flags'])
      self.assertDictContainsSubset(case['expected'], options.get_all_options())
      self.assertEqual(options.view_as(MockOptions).mock_flag,
                       case['expected']['mock_flag'])

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
