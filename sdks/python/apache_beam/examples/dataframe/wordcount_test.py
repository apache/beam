# -*- coding: utf-8 -*-
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

"""Test for the wordcount example."""

# pytype: skip-file

import collections
import logging
import re
import unittest
import uuid

import pytest

from apache_beam.examples.dataframe import wordcount
from apache_beam.testing.test_pipeline import TestPipeline

# Protect against environments where gcsio library is not available.
try:
  from apache_beam.io.gcp import gcsio
except ImportError:
  gcsio = None


def read_gcs_output_file(file_pattern):
  gcs = gcsio.GcsIO()
  file_names = gcs.list_prefix(file_pattern).keys()
  output = []
  for file_name in file_names:
    output.append(gcs.open(file_name).read().decode('utf-8').strip())
  return '\n'.join(output)


def create_content_input_file(path, contents):
  logging.info('Creating file: %s', path)
  gcs = gcsio.GcsIO()
  with gcs.open(path, 'w') as f:
    f.write(str.encode(contents, 'utf-8'))
  return path


class WordCountTest(unittest.TestCase):

  SAMPLE_TEXT = """
  a
  a b
  a b c
  loooooonger words
  """

  @pytest.mark.examples_postcommit
  def test_basics(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    # Setup the files with expected content.
    temp_location = test_pipeline.get_option('temp_location')
    temp_path = '/'.join([temp_location, str(uuid.uuid4())])
    input = create_content_input_file(
        '/'.join([temp_path, 'input.txt']), self.SAMPLE_TEXT)
    expected_words = collections.defaultdict(int)
    for word in re.findall(r'[\w]+', self.SAMPLE_TEXT):
      expected_words[word] += 1
    extra_opts = {'input': input, 'output': '%s.result' % temp_path}
    wordcount.run(test_pipeline.get_full_options_as_args(**extra_opts))
    # Parse result file and compare.
    results = []
    lines = read_gcs_output_file(temp_path + '.result-').splitlines()
    for line in lines:
      match = re.search(r'(\S+),([0-9]+)', line)
      if match is not None:
        results.append((match.group(1), int(match.group(2))))
      elif line.strip():
        self.assertEqual(line.strip(), 'word,count')
    self.assertEqual(sorted(results), sorted(expected_words.items()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
