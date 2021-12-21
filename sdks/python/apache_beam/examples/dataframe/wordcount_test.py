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
import tempfile
import unittest

import pytest

from apache_beam.examples.dataframe import wordcount
from apache_beam.testing.util import open_shards


class WordCountTest(unittest.TestCase):

  SAMPLE_TEXT = """
  a
  a b
  a b c
  loooooonger words
  """

  def create_temp_file(self, contents):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(contents.encode('utf-8'))
      return f.name

  @pytest.mark.examples_postcommit
  def test_basics(self):
    temp_path = self.create_temp_file(self.SAMPLE_TEXT)
    expected_words = collections.defaultdict(int)
    for word in re.findall(r'[\w]+', self.SAMPLE_TEXT):
      expected_words[word] += 1
    wordcount.run(['--input=%s*' % temp_path, '--output=%s.result' % temp_path])
    # Parse result file and compare.
    results = []
    with open_shards(temp_path + '.result-*') as result_file:
      for line in result_file:
        match = re.search(r'(\S+),([0-9]+)', line)
        if match is not None:
          results.append((match.group(1), int(match.group(2))))
        elif line.strip():
          self.assertEqual(line.strip(), 'word,count')
    self.assertEqual(sorted(results), sorted(expected_words.items()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
