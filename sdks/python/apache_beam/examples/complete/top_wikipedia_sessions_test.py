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

"""Test for the top wikipedia sessions example."""

# pytype: skip-file

import json
import logging
import os
import tempfile
import unittest

import pytest

import apache_beam as beam
from apache_beam.examples.complete import top_wikipedia_sessions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import open_shards


class ComputeTopSessionsTest(unittest.TestCase):

  EDITS = [
      json.dumps({
          'timestamp': 0.0, 'contributor_username': 'user1'
      }),
      json.dumps({
          'timestamp': 0.001, 'contributor_username': 'user1'
      }),
      json.dumps({
          'timestamp': 0.002, 'contributor_username': 'user1'
      }),
      json.dumps({
          'timestamp': 0.0, 'contributor_username': 'user2'
      }),
      json.dumps({
          'timestamp': 0.001, 'contributor_username': 'user2'
      }),
      json.dumps({
          'timestamp': 3.601, 'contributor_username': 'user2'
      }),
      json.dumps({
          'timestamp': 3.602, 'contributor_username': 'user2'
      }),
      json.dumps({
          'timestamp': 2 * 3600.0, 'contributor_username': 'user2'
      }),
      json.dumps({
          'timestamp': 35 * 24 * 3.600, 'contributor_username': 'user3'
      })
  ]

  EXPECTED = [
      'user1 : [0.0, 3600.002) : 3 : [0.0, 2592000.0)',
      'user2 : [0.0, 3603.602) : 4 : [0.0, 2592000.0)',
      'user2 : [7200.0, 10800.0) : 1 : [0.0, 2592000.0)',
      'user3 : [3024.0, 6624.0) : 1 : [0.0, 2592000.0)',
  ]

  def create_content_input_file(self, path, contents):
    logging.info('Creating temp file: %s', path)
    with open(path, 'w') as f:
      f.write(contents)

  def test_compute_top_sessions(self):
    with TestPipeline() as p:
      edits = p | beam.Create(self.EDITS)
      result = edits | top_wikipedia_sessions.ComputeTopSessions(1.0)

      assert_that(result, equal_to(self.EXPECTED))

  @pytest.mark.examples_postcommit
  def test_top_wikipedia_sessions_output_files_on_small_input(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    # Setup the files with expected content.
    temp_folder = tempfile.mkdtemp()
    self.create_content_input_file(
        os.path.join(temp_folder, 'input.txt'), '\n'.join(self.EDITS))
    extra_opts = {
        'input': '%s/input.txt' % temp_folder,
        'output': os.path.join(temp_folder, 'result'),
        'sampling_threshold': '1.0'
    }
    top_wikipedia_sessions.run(
        test_pipeline.get_full_options_as_args(**extra_opts))

    # Load result file and compare.
    with open_shards(os.path.join(temp_folder, 'result-*-of-*')) as result_file:
      result = result_file.read().strip().splitlines()

    self.assertEqual(self.EXPECTED, sorted(result, key=lambda x: x.split()[0]))


if __name__ == '__main__':
  unittest.main()
