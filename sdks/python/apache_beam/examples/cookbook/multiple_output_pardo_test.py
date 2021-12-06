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

"""Test for the multiple_output_pardo example."""

# pytype: skip-file

import logging
import re
import tempfile
import unittest

import pytest

from apache_beam.examples.cookbook import multiple_output_pardo
from apache_beam.testing.util import open_shards


class MultipleOutputParDo(unittest.TestCase):

  SAMPLE_TEXT = 'A whole new world\nA new fantastic point of view'
  EXPECTED_SHORT_WORDS = [('A', 2), ('new', 2), ('of', 1)]
  EXPECTED_WORDS = [('whole', 1), ('world', 1), ('fantastic', 1), ('point', 1),
                    ('view', 1)]

  def create_temp_file(self, contents):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(contents.encode('utf-8'))
      return f.name

  def get_wordcount_results(self, result_path):
    results = []
    with open_shards(result_path) as result_file:
      for line in result_file:
        match = re.search(r'([A-Za-z]+): ([0-9]+)', line)
        if match is not None:
          results.append((match.group(1), int(match.group(2))))
    return results

  @pytest.mark.examples_postcommit
  def test_multiple_output_pardo(self):
    temp_path = self.create_temp_file(self.SAMPLE_TEXT)
    result_prefix = temp_path + '.result'

    multiple_output_pardo.run(
        ['--input=%s*' % temp_path, '--output=%s' % result_prefix],
        save_main_session=False)

    expected_char_count = len(''.join(self.SAMPLE_TEXT.split('\n')))
    with open_shards(result_prefix + '-chars-*-of-*') as f:
      contents = f.read()
      self.assertEqual(expected_char_count, int(contents))

    short_words = self.get_wordcount_results(
        result_prefix + '-short-words-*-of-*')
    self.assertEqual(sorted(short_words), sorted(self.EXPECTED_SHORT_WORDS))

    words = self.get_wordcount_results(result_prefix + '-words-*-of-*')
    self.assertEqual(sorted(words), sorted(self.EXPECTED_WORDS))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
