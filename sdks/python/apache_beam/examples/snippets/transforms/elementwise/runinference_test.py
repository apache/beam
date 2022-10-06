# coding=utf-8
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

# pytype: skip-file

import re
import unittest
from io import StringIO

import mock
import pytest

from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports, unused-import
try:
  import torch
  from . import runinference
except ImportError:
  raise unittest.SkipTest('PyTorch dependencies are not installed')

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports, unused-import
try:
  from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
except ImportError:
  raise unittest.SkipTest('GCP dependencies are not installed')


def check_torch_keyed_model_handler():
  expected = '''[START torch_keyed_model_handler]
('first_question', PredictionResult(example=tensor([105.]), inference=tensor([523.6982])))
('second_question', PredictionResult(example=tensor([108.]), inference=tensor([538.5867])))
('third_question', PredictionResult(example=tensor([1000.]), inference=tensor([4965.4019])))
('fourth_question', PredictionResult(example=tensor([1013.]), inference=tensor([5029.9180])))
[END torch_keyed_model_handler] '''.splitlines()[1:-1]
  return expected


def check_sklearn_keyed_model_handler(actual):
  expected = '''[START sklearn_keyed_model_handler]
('first_question', PredictionResult(example=[105.0], inference=array([525.])))
('second_question', PredictionResult(example=[108.0], inference=array([540.])))
('third_question', PredictionResult(example=[1000.0], inference=array([5000.])))
('fourth_question', PredictionResult(example=[1013.0], inference=array([5065.])))
[END sklearn_keyed_model_handler] '''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


def check_torch_unkeyed_model_handler():
  expected = '''[START torch_unkeyed_model_handler]
PredictionResult(example=tensor([10.]), inference=tensor([52.2325]))
PredictionResult(example=tensor([40.]), inference=tensor([201.1165]))
PredictionResult(example=tensor([60.]), inference=tensor([300.3724]))
PredictionResult(example=tensor([90.]), inference=tensor([449.2563]))
[END torch_unkeyed_model_handler] '''.splitlines()[1:-1]
  return expected


def check_sklearn_unkeyed_model_handler(actual):
  expected = '''[START sklearn_unkeyed_model_handler]
PredictionResult(example=array([20.], dtype=float32), inference=array([100.], dtype=float32))
PredictionResult(example=array([40.], dtype=float32), inference=array([200.], dtype=float32))
PredictionResult(example=array([60.], dtype=float32), inference=array([300.], dtype=float32))
PredictionResult(example=array([90.], dtype=float32), inference=array([450.], dtype=float32))
[END sklearn_unkeyed_model_handler]  '''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch(
    'apache_beam.examples.snippets.transforms.elementwise.runinference.print',
    str)
class RunInferenceTest(unittest.TestCase):
  def test_sklearn_unkeyed_model_handler(self):
    runinference.sklearn_unkeyed_model_handler(
        check_sklearn_unkeyed_model_handler)

  def test_sklearn_keyed_model_handler(self):
    runinference.sklearn_keyed_model_handler(check_sklearn_keyed_model_handler)


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch('sys.stdout', new_callable=StringIO)
class RunInferenceStdoutTest(unittest.TestCase):
  @pytest.mark.uses_pytorch
  def test_check_torch_keyed_model_handler(self, mock_stdout):
    runinference.torch_keyed_model_handler()
    predicted = mock_stdout.getvalue().splitlines()
    expected = check_torch_keyed_model_handler()
    actual_stdout = [line.split(':')[0] for line in predicted]
    replace_fn = lambda x: re.sub(r"<UnbindBackward\d*>", "<UnbindBackward>", x)
    actual_stdout = [replace_fn(x) for x in actual_stdout]
    expected_stdout = [line.split(':')[0] for line in expected]
    self.assertEqual(actual_stdout, expected_stdout)

  @pytest.mark.uses_pytorch
  def test_check_torch_unkeyed_model_handler(self, mock_stdout):
    runinference.torch_unkeyed_model_handler()
    predicted = mock_stdout.getvalue().splitlines()
    expected = check_torch_unkeyed_model_handler()
    actual_stdout = [line.split(':')[0] for line in predicted]
    replace_fn = lambda x: re.sub(r"<UnbindBackward\d*>", "<UnbindBackward>", x)
    actual_stdout = [replace_fn(x) for x in actual_stdout]
    expected_stdout = [line.split(':')[0] for line in expected]
    self.assertEqual(actual_stdout, expected_stdout)


if __name__ == '__main__':
  unittest.main()
