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

"""Tests for the various custom Count implementation examples."""

# pytype: skip-file

import logging
import os
import tempfile
import unittest

import pytest

import apache_beam as beam
from apache_beam.examples.cookbook import custom_ptransform
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import open_shards


class CustomCountTest(unittest.TestCase):
  WORDS = ['CAT', 'DOG', 'CAT', 'CAT', 'DOG']

  def create_content_input_file(self, path, contents):
    logging.info('Creating temp file: %s', path)
    with open(path, 'w') as f:
      f.write(contents)

  def test_count1(self):
    self.run_pipeline(custom_ptransform.Count1())

  def test_count2(self):
    self.run_pipeline(custom_ptransform.Count2())

  def test_count3(self):
    factor = 2
    self.run_pipeline(custom_ptransform.Count3(factor), factor=factor)

  def run_pipeline(self, count_implementation, factor=1):
    with TestPipeline() as p:
      words = p | beam.Create(self.WORDS)
      result = words | count_implementation
      assert_that(
          result, equal_to([('CAT', (3 * factor)), ('DOG', (2 * factor))]))

  @pytest.mark.examples_postcommit
  def test_custom_ptransform_output_files_on_small_input(self):
    EXPECTED_RESULT = "('CAT DOG CAT CAT DOG', 2)"

    # Setup the files with expected content.
    temp_folder = tempfile.mkdtemp()
    self.create_content_input_file(
        os.path.join(temp_folder, 'input.txt'), ' '.join(self.WORDS))
    custom_ptransform.run([
        '--input=%s/input.txt' % temp_folder,
        '--output',
        os.path.join(temp_folder, 'result')
    ])

    # Load result file and compare.
    with open_shards(os.path.join(temp_folder, 'result-*-of-*')) as result_file:
      result = result_file.read().strip()

    self.assertEqual(result, EXPECTED_RESULT)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
