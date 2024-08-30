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

from apache_beam.examples import wordcount
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import create_file
from apache_beam.testing.test_utils import read_files_from_pattern


@pytest.mark.examples_postcommit
class WordCountTest(unittest.TestCase):

  SAMPLE_TEXT = (
      'a b c a b a\nacento gráfico\nJuly 30, 2018\n\n aa bb cc aa bb aa')

  def test_basics(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    # Setup the files with expected content.
    temp_location = test_pipeline.get_option('temp_location')
    temp_path = '/'.join([temp_location, str(uuid.uuid4())])
    input = create_file('/'.join([temp_path, 'input.txt']), self.SAMPLE_TEXT)
    extra_opts = {'input': input, 'output': '%s.result' % temp_path}
    expected_words = collections.defaultdict(int)
    for word in re.findall(r'[\w\']+', self.SAMPLE_TEXT, re.UNICODE):
      expected_words[word] += 1
    wordcount.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    # Parse result file and compare.
    results = []
    lines = read_files_from_pattern(temp_path + '.result*').splitlines()
    for line in lines:
      match = re.search(r'(\S+): ([0-9]+)', line)
      if match is not None:
        results.append((match.group(1), int(match.group(2))))
    self.assertEqual(sorted(results), sorted(expected_words.items()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
