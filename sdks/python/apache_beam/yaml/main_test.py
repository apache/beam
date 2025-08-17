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

import datetime
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

tests:
  - name: InlineTest
    mock_outputs:
      - name: Create
        elements: ['a', 'b', 'c']
    expected_inputs:
      - name: WriteToText
        elements:
          - {element: a}
          - {element: b}
          - {element: c}
'''

PASSING_TEST_SUITE = '''
tests:
  - name: ExternalTest  # comment
    mock_outputs:
      - name: Create
        elements: ['a', 'b', 'c']
    expected_inputs:
      - name: WriteToText
        elements:
          - element: a
          - element: b
          - element: c
'''

FAILING_TEST_SUITE = '''
tests:
  - name: ExternalTest  # comment
    mock_outputs:
      - name: Create
        elements: ['a', 'b', 'c']
    expected_inputs:
      - name: WriteToText
        elements:
          - element: x
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

  def test_jinja_variable_flags(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      out_path = os.path.join(tmpdir, 'out.txt')
      main.run([
          '--yaml_pipeline',
          TEST_PIPELINE.replace('PATH', out_path).replace('ELEMENT', '{{var}}'),
          '--jinja_variable_flags=var',
          '--var=my_line'
      ])
      with open(glob.glob(out_path + '*')[0], 'rt') as fin:
        self.assertEqual(fin.read().strip(), 'my_line')

  def test_preparse_jinja_flags(self):
    argv = [
        '--jinja_variables={"from_vars": 1, "from_both": 2}',
        '--jinja_variable_flags=from_both,from_flag,from_missing_flag',
        '--from_both=30',
        '--from_flag=40',
        '--another_arg=foo',
        'pos_arg',
    ]
    self.assertCountEqual(
        main._preparse_jinja_flags(argv),
        [
            '--jinja_variables='
            '{"from_vars": 1, "from_both": "30", "from_flag": "40"}',
            '--another_arg=foo',
            'pos_arg',
        ])

  def test_jinja_datetime(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      out_path = os.path.join(tmpdir, 'out.txt')
      main.run([
          '--yaml_pipeline',
          TEST_PIPELINE.replace('PATH', out_path).replace(
              'ELEMENT', '"{{datetime.datetime.now().strftime("%Y-%m-%d")}}"'),
      ])
      with open(glob.glob(out_path + '*')[0], 'rt') as fin:
        self.assertEqual(
            fin.read().strip(), datetime.datetime.now().strftime("%Y-%m-%d"))

  def test_inline_test_specs(self):
    main.run_tests(['--yaml_pipeline', TEST_PIPELINE, '--test'], exit=False)

  def test_external_test_specs(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      good_suite = os.path.join(tmpdir, 'good.yaml')
      with open(good_suite, 'w') as fout:
        fout.write(PASSING_TEST_SUITE)
      bad_suite = os.path.join(tmpdir, 'bad.yaml')
      with open(bad_suite, 'w') as fout:
        fout.write(FAILING_TEST_SUITE)

      # Must pass.
      main.run_tests([
          '--yaml_pipeline',
          TEST_PIPELINE,
          '--test_suite',
          good_suite,
      ],
                     exit=False)

      # Must fail. (Ensures testing is not a no-op.)
      with self.assertRaisesRegex(Exception, 'errors=1 failures=0'):
        main.run_tests([
            '--yaml_pipeline',
            TEST_PIPELINE,
            '--test_suite',
            bad_suite,
        ],
                       exit=False)

  def test_fix_suite(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      test_suite = os.path.join(tmpdir, 'tests.yaml')
      with open(test_suite, 'w') as fout:
        fout.write(FAILING_TEST_SUITE)

      main.run_tests([
          '--yaml_pipeline',
          TEST_PIPELINE,
          '--test_suite',
          test_suite,
          '--fix_tests'
      ],
                     exit=False)

      with open(test_suite) as fin:
        self.assertEqual(fin.read(), PASSING_TEST_SUITE)

  def test_create_test(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      test_suite = os.path.join(tmpdir, 'tests.yaml')
      with open(test_suite, 'w') as fout:
        fout.write('')

      main.run_tests([
          '--yaml_pipeline',
          TEST_PIPELINE.replace('ELEMENT', 'x'),
          '--test_suite',
          test_suite,
          '--create_test'
      ],
                     exit=False)

      with open(test_suite) as fin:
        self.assertEqual(
            fin.read(),
            '''
tests:
- mock_outputs: []
  expected_inputs:
  - name: WriteToText
    elements:
    - element: x
'''.lstrip())


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
