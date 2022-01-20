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

"""Test for the minimal wordcount example."""

# pytype: skip-file

# beam-playground:
#   name: WordCountMinimalTest
#   description: Unit-test for the minimal WordCount example.
#   multifile: false
#   pipeline_options:
#   categories:
#     - IO
#     - Combiners

import collections
import logging
import re
import tempfile
import unittest

import pytest

from apache_beam.examples import wordcount_minimal
from apache_beam.testing.util import open_shards


@pytest.mark.examples_postcommit
class WordCountMinimalTest(unittest.TestCase):
  """Unit test for wordcount_minimal example with direct runner."""

  SAMPLE_TEXT = 'a b c a b a\n aa bb cc aa bb aa'

  def create_temp_file(self, contents):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(contents.encode('utf-8'))
      return f.name

  def test_basics(self):
    temp_path = self.create_temp_file(self.SAMPLE_TEXT)
    expected_words = collections.defaultdict(int)
    for word in re.findall(r'\w+', self.SAMPLE_TEXT):
      expected_words[word] += 1
    wordcount_minimal.main(
        ['--input=%s*' % temp_path, '--output=%s.result' % temp_path],
        save_main_session=False)
    # Parse result file and compare.
    results = []
    with open_shards(temp_path + '.result-*-of-*') as result_file:
      for line in result_file:
        match = re.search(r'([a-z]+): ([0-9]+)', line)
        if match is not None:
          results.append((match.group(1), int(match.group(2))))
    self.assertEqual(sorted(results), sorted(expected_words.items()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
