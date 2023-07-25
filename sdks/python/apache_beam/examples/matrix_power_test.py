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

"""Test for the matrix power integration test."""

import logging
import tempfile
import unittest

from apache_beam.examples import matrix_power


class MatrixPowerTest(unittest.TestCase):

  MATRIX_INPUT = b'''
0:1 1 1
1:1 1 1
2:1 1 1
'''.strip()
  VECTOR_INPUT = b'1 2 3'
  EXPONENT = 3
  EXPECTED_OUTPUT = '''
(0, 54.0)
(1, 54.0)
(2, 54.0)
'''.lstrip()

  def create_temp_file(self, contents):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(contents)
      return f.name

  def test_basics(self):
    matrix_path = self.create_temp_file(self.MATRIX_INPUT)
    vector_path = self.create_temp_file(self.VECTOR_INPUT)
    matrix_power.run((
        '--input_matrix=%s --input_vector=%s --exponent=%d --output=%s.result' %
        (matrix_path, vector_path, self.EXPONENT, vector_path)).split())
    # Parse result file and compare.
    with open(vector_path + '.result-00000-of-00001') as result_file:
      results = result_file.read()
      self.assertEqual(sorted(self.EXPECTED_OUTPUT), sorted(results))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
