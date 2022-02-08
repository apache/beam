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

"""Test for the coders example."""

# pytype: skip-file

import json
import logging
import os
import tempfile
import unittest

import pytest

import apache_beam as beam
from apache_beam.examples.cookbook import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import open_shards


class CodersTest(unittest.TestCase):

  SAMPLE_RECORDS = [{
      'host': ['Germany', 1], 'guest': ['Italy', 0]
  }, {
      'host': ['Germany', 1], 'guest': ['Brasil', 3]
  }, {
      'host': ['Brasil', 1], 'guest': ['Italy', 0]
  }]

  EXPECTED_RESULT = [('Italy', 0), ('Brasil', 6), ('Germany', 3)]

  def create_content_input_file(self, path, contents):
    logging.info('Creating temp file: %s', path)
    with open(path, 'w') as f:
      f.write(contents)

  def format_result(self, result_string):
    def format_tuple(result_elem_list):
      [country, counter] = result_elem_list
      return country, int(counter.strip())

    result_list = list(
        map(
            lambda result_elem: format_tuple(result_elem.split(',')),
            result_string.replace('\'',
                                  '').replace('[', '').replace(']', '').replace(
                                      '\"', '').split('\n')))
    return result_list

  def test_compute_points(self):
    with TestPipeline() as p:
      records = p | 'create' >> beam.Create(self.SAMPLE_RECORDS)
      result = (
          records
          | 'points' >> beam.FlatMap(coders.compute_points)
          | beam.CombinePerKey(sum))
      assert_that(result, equal_to(self.EXPECTED_RESULT))

  @pytest.mark.examples_postcommit
  def test_coders_output_files_on_small_input(self):
    test_pipeline = TestPipeline(is_integration_test=True)

    # Setup the files with expected content.
    temp_folder = tempfile.mkdtemp()
    self.create_content_input_file(
        os.path.join(temp_folder, 'input.txt'),
        '\n'.join(map(json.dumps, self.SAMPLE_RECORDS)))
    extra_opts = {
        'input': '%s/input.txt' % temp_folder,
        'output': os.path.join(temp_folder, 'result')
    }
    coders.run(test_pipeline.get_full_options_as_args(**extra_opts))

    # Load result file and compare.
    with open_shards(os.path.join(temp_folder, 'result-*-of-*')) as result_file:
      result = result_file.read().strip()

    self.assertEqual(
        sorted(self.EXPECTED_RESULT), sorted(self.format_result(result)))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
