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
# pylint: disable=ungrouped-imports

import unittest
from io import StringIO

import mock

from apache_beam.testing.test_pipeline import TestPipeline

try:
  # fail when tft is not installed.
  import tensorflow_transform as tft  # pylint: disable=unused-import
  from apache_beam.examples.snippets.transforms.elementwise.mltransform import mltransform_scale_to_0_1
  from apache_beam.examples.snippets.transforms.elementwise.mltransform import mltransform_compute_and_apply_vocabulary
  from apache_beam.examples.snippets.transforms.elementwise.mltransform import mltransform_compute_and_apply_vocabulary_with_scalar
except ImportError:
  raise unittest.SkipTest('tensorflow_transform is not installed.')


def check_mltransform_compute_and_apply_vocab():
  expected = '''[START mltransform_compute_and_apply_vocab]
Row(x=array([4, 1, 0]))
Row(x=array([0, 2, 3]))
  [END mltransform_compute_and_apply_vocab] '''.splitlines()[1:-1]
  return expected


def check_mltransform_scale_to_0_1():
  expected = '''[START mltransform_scale_to_0_1]
Row(x=array([0.       , 0.5714286, 0.2857143], dtype=float32))
Row(x=array([0.42857143, 0.14285715, 1.        ], dtype=float32))
  [END mltransform_scale_to_0_1] '''.splitlines()[1:-1]
  return expected


def check_mltransform_compute_and_apply_vocabulary_with_scalar():
  expected = '''[START mltransform_compute_and_apply_vocabulary_with_scalar]
Row(x=array([4]))
Row(x=array([1]))
Row(x=array([0]))
Row(x=array([0]))
Row(x=array([2]))
Row(x=array([3]))
  [END mltransform_compute_and_apply_vocabulary_with_scalar] '''.splitlines(
  )[1:-1]
  return expected


@mock.patch('apache_beam.Pipeline', TestPipeline)
@mock.patch('sys.stdout', new_callable=StringIO)
class MLTransformStdOutTest(unittest.TestCase):
  def test_mltransform_compute_and_apply_vocab(self, mock_stdout):
    mltransform_compute_and_apply_vocabulary()
    predicted = mock_stdout.getvalue().splitlines()
    expected = check_mltransform_compute_and_apply_vocab()
    self.assertEqual(predicted, expected)

  def test_mltransform_scale_to_0_1(self, mock_stdout):
    mltransform_scale_to_0_1()
    predicted = mock_stdout.getvalue().splitlines()
    expected = check_mltransform_scale_to_0_1()
    self.assertEqual(predicted, expected)

  def test_mltransform_compute_and_apply_vocab_scalar(self, mock_stdout):
    mltransform_compute_and_apply_vocabulary_with_scalar()
    predicted = mock_stdout.getvalue().splitlines()
    expected = check_mltransform_compute_and_apply_vocabulary_with_scalar()
    self.assertEqual(predicted, expected)


if __name__ == '__main__':
  unittest.main()
