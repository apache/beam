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

import glob
import logging
import os
import tempfile
import unittest

from apache_beam.yaml import main

TEST_PIPELINE = '''
pipeline:
  type: chain
  transforms:
    - type: Create
      config:
        elements: [ELEMENT]
      # Writing to an actual file here rather than just using AssertThat
      # because this is an integration test and above all we want to ensure
      # the pipeline actually runs (and asserts may not fail if there's a
      # bug in the invocation logic).
    - type: WriteToText
      config:
        path: PATH
'''


class MainTest(unittest.TestCase):
  def test_pipeline_spec_from_file(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      yaml_path = os.path.join(tmpdir, 'test.yaml')
      out_path = os.path.join(tmpdir, 'out.txt')
      with open(yaml_path, 'wt') as fout:
        fout.write(TEST_PIPELINE.replace('PATH', out_path))
      main.run(['--yaml_pipeline_file', yaml_path])
      with open(glob.glob(out_path + '*')[0], 'rt') as fin:
        self.assertEqual(fin.read().strip(), 'ELEMENT')

  def test_pipeline_spec_from_flag(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      out_path = os.path.join(tmpdir, 'out.txt')
      main.run(['--yaml_pipeline', TEST_PIPELINE.replace('PATH', out_path)])
      with open(glob.glob(out_path + '*')[0], 'rt') as fin:
        self.assertEqual(fin.read().strip(), 'ELEMENT')

  def test_jinja_variables(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      out_path = os.path.join(tmpdir, 'out.txt')
      main.run([
          '--yaml_pipeline',
          TEST_PIPELINE.replace('PATH', out_path).replace('ELEMENT', '{{var}}'),
          '--jinja_variables',
          '{"var": "my_line"}'
      ])
      with open(glob.glob(out_path + '*')[0], 'rt') as fin:
        self.assertEqual(fin.read().strip(), 'my_line')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
