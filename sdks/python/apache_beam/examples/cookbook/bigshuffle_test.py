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

"""Test for the bigshuffle example."""

import logging
import tempfile
import unittest

from apache_beam.examples.cookbook import bigshuffle


# TODO(dataflow-python): use gensort to generate input files.
class BigShuffleTest(unittest.TestCase):

  SAMPLE_TEXT = 'a b c a b a\naa bb cc aa bb aa'

  def create_temp_file(self, contents):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(contents)
      return f.name

  def test_basics(self):
    temp_path = self.create_temp_file(self.SAMPLE_TEXT)
    bigshuffle.run([
        '--input=%s*' % temp_path,
        '--output=%s.result' % temp_path,
        '--checksum_output=%s.checksum' % temp_path])
    # Parse result file and compare.
    results = []
    with open(temp_path + '.result-00000-of-00001') as result_file:
      for line in result_file:
        results.append(line.strip())
    expected = self.SAMPLE_TEXT.split('\n')
    self.assertEqual(sorted(results), sorted(expected))
    # Check the checksums
    input_csum = ''
    with open(temp_path + '.checksum-input-00000-of-00001') as input_csum_file:
      input_csum = input_csum_file.read().strip()
    output_csum = ''
    with open(temp_path +
              '.checksum-output-00000-of-00001') as output_csum_file:
      output_csum = output_csum_file.read().strip()
    expected_csum = 'd629c1f6'
    self.assertEqual(input_csum, expected_csum)
    self.assertEqual(input_csum, output_csum)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
