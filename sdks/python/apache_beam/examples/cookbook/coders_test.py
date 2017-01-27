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

import json
import logging
import tempfile
import unittest

from apache_beam.examples.cookbook import coders


class CodersTest(unittest.TestCase):

  SAMPLE_RECORDS = [
      {'host': ['Germany', 1], 'guest': ['Italy', 0]},
      {'host': ['Germany', 1], 'guest': ['Brasil', 3]},
      {'host': ['Brasil', 1], 'guest': ['Italy', 0]}]

  def create_temp_file(self, records):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      for record in records:
        f.write('%s\n' % json.dumps(record))
      return f.name

  def test_basics(self):
    temp_path = self.create_temp_file(self.SAMPLE_RECORDS)
    coders.run([
        '--input=%s*' % temp_path,
        '--output=%s.result' % temp_path])
    # Parse result file and compare.
    results = []
    with open(temp_path + '.result-00000-of-00001') as result_file:
      for line in result_file:
        results.append(json.loads(line))
      logging.info('result: %s', results)
    self.assertEqual(
        sorted(results),
        sorted([['Italy', 0], ['Brasil', 6], ['Germany', 3]]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
