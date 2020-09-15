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

from __future__ import absolute_import

import os
import glob
import shutil
import tempfile
import unittest

import apache_beam as beam
from apache_beam.dataframe import io


class IOTest(unittest.TestCase):
  def setUp(self):
    self._temp_roots = []

  def tearDown(self):
    for root in self._temp_roots:
      shutil.rmtree(root)

  def temp_dir(self, files=None):
    dir = tempfile.mkdtemp(prefix='beam-test')
    self._temp_roots.append(dir)
    if files:
      for name, contents in files.items():
        with open(os.path.join(dir, name), 'w') as fout:
          fout.write(contents)
    return dir + os.path.sep

  def read_all_lines(self, pattern):
    for path in glob.glob(pattern):
      with open(path) as fin:
        # TODO(Py3): yield from
        for line in fin:
          yield line.rstrip('\n')

  def test_write_csv(self):
    input = self.temp_dir({'1.csv': 'a,b\n1,2\n', '2.csv': 'a,b\n3,4\n'})
    output = self.temp_dir()
    with beam.Pipeline() as p:
      df = p | io.read_csv(input + '*.csv')
      df['c'] = df.a + df.b
      df.to_csv(output + 'out.csv', index=False)
    self.assertCountEqual(['a,b,c', '1,2,3', '3,4,7'],
                          set(self.read_all_lines(output + 'out.csv*')))
