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

import shutil
import tempfile
import unittest

import numpy as np
from parameterized import parameterized

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.ml.transforms import base
  from apache_beam.ml.transforms import tft
except ImportError:
  tft = None  # type: ignore[assignment]

if not tft:
  raise unittest.SkipTest('tensorflow_transform is not installed.')

z_score_expected = {'x_mean': 3.5, 'x_var': 2.9166666666666665}


def assert_z_score_artifacts(element):
  element = element.as_dict()
  assert 'x_mean' in element
  assert 'x_var' in element
  assert element['x_mean'] == z_score_expected['x_mean']
  assert element['x_var'] == z_score_expected['x_var']


def assert_ScaleTo01_artifacts(element):
  element = element.as_dict()
  assert 'x_min' in element
  assert 'x_max' in element
  assert element['x_min'] == 1
  assert element['x_max'] == 6


def assert_bucketize_artifacts(element):
  element = element.as_dict()
  assert 'x_quantiles' in element
  assert np.array_equal(
      element['x_quantiles'], np.array([3, 5], dtype=np.float32))


class ScaleZScoreTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_z_score_unbatched(self):
    unbatched_data = [{
        'x': 1
    }, {
        'x': 2
    }, {
        'x': 3
    }, {
        'x': 4
    }, {
        'x': 5
    }, {
        'x': 6
    }]

    with beam.Pipeline() as p:
      unbatched_result = (
          p
          | "unbatchedCreate" >> beam.Create(unbatched_data)
          | "unbatchedMLTransform" >> base.MLTransform(
              artifact_location=self.artifact_location).with_transform(
                  tft.ScaleToZScore(columns=['x'])))
      _ = (unbatched_result | beam.Map(assert_z_score_artifacts))

  def test_z_score_batched(self):
    batched_data = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      batched_result = (
          p
          | "batchedCreate" >> beam.Create(batched_data)
          | "batchedMLTransform" >> base.MLTransform(
              artifact_location=self.artifact_location).with_transform(
                  tft.ScaleToZScore(columns=['x'])))
      _ = (batched_result | beam.Map(assert_z_score_artifacts))


class ScaleTo01Test(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_ScaleTo01_batched(self):
    batched_data = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      batched_result = (
          p
          | "batchedCreate" >> beam.Create(batched_data)
          | "batchedMLTransform" >> base.MLTransform(
              artifact_location=self.artifact_location).with_transform(
                  tft.ScaleTo01(columns=['x'])))
      _ = (batched_result | beam.Map(assert_ScaleTo01_artifacts))

      expected_output = [
          np.array([0, 0.2, 0.4], dtype=np.float32),
          np.array([0.6, 0.8, 1], dtype=np.float32)
      ]
      actual_output = (batched_result | beam.Map(lambda x: x.x))
      assert_that(
          actual_output, equal_to(expected_output, equals_fn=np.array_equal))

  def test_ScaleTo01_unbatched(self):
    unbatched_data = [{
        'x': 1
    }, {
        'x': 2
    }, {
        'x': 3
    }, {
        'x': 4
    }, {
        'x': 5
    }, {
        'x': 6
    }]
    with beam.Pipeline() as p:
      unbatched_result = (
          p
          | "unbatchedCreate" >> beam.Create(unbatched_data)
          | "unbatchedMLTransform" >> base.MLTransform(
              artifact_location=self.artifact_location).with_transform(
                  tft.ScaleTo01(columns=['x'])))

      _ = (unbatched_result | beam.Map(assert_ScaleTo01_artifacts))
      expected_output = (
          np.array([0], dtype=np.float32),
          np.array([0.2], dtype=np.float32),
          np.array([0.4], dtype=np.float32),
          np.array([0.6], dtype=np.float32),
          np.array([0.8], dtype=np.float32),
          np.array([1], dtype=np.float32))
      actual_output = (unbatched_result | beam.Map(lambda x: x.x))
      assert_that(
          actual_output, equal_to(expected_output, equals_fn=np.array_equal))


class BucketizeTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_bucketize_unbatched(self):
    unbatched = [{'x': 1}, {'x': 2}, {'x': 3}, {'x': 4}, {'x': 5}, {'x': 6}]
    with beam.Pipeline() as p:
      unbatched_result = (
          p
          | "unbatchedCreate" >> beam.Create(unbatched)
          | "unbatchedMLTransform" >> base.MLTransform(
              artifact_location=self.artifact_location).with_transform(
                  tft.Bucketize(columns=['x'], num_buckets=3)))
      _ = (unbatched_result | beam.Map(assert_bucketize_artifacts))

      transformed_data = (unbatched_result | beam.Map(lambda x: x.x))
      expected_data = [
          np.array([0]),
          np.array([0]),
          np.array([1]),
          np.array([1]),
          np.array([2]),
          np.array([2])
      ]
      assert_that(
          transformed_data, equal_to(expected_data, equals_fn=np.array_equal))

  def test_bucketize_batched(self):
    batched = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      batched_result = (
          p
          | "batchedCreate" >> beam.Create(batched)
          | "batchedMLTransform" >> base.MLTransform(
              artifact_location=self.artifact_location).with_transform(
                  tft.Bucketize(columns=['x'], num_buckets=3)))
      _ = (batched_result | beam.Map(assert_bucketize_artifacts))

      transformed_data = (
          batched_result
          | "TransformedColumnX" >> beam.Map(lambda ele: ele.x))
      expected_data = [
          np.array([0, 0, 1], dtype=np.int64),
          np.array([1, 2, 2], dtype=np.int64)
      ]
      assert_that(
          transformed_data, equal_to(expected_data, equals_fn=np.array_equal))

  @parameterized.expand([
      (range(1, 10), [4, 7]),
      (range(9, 0, -1), [4, 7]),
      (range(19, 0, -1), [10]),
      (range(1, 100), [25, 50, 75]),
      # similar to the above but with odd number of elements
      (range(1, 100, 2), [25, 51, 75]),
      (range(99, 0, -1), range(10, 100, 10))
  ])
  def test_bucketize_boundaries(self, test_input, expected_boundaries):
    # boundaries are outputted as artifacts for the Bucketize transform.
    data = [{'x': [i]} for i in test_input]
    num_buckets = len(expected_boundaries) + 1
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              artifact_location=self.artifact_location).with_transform(
                  tft.Bucketize(columns=['x'], num_buckets=num_buckets)))
      actual_boundaries = (
          result
          | beam.Map(lambda x: x.as_dict())
          | beam.Map(lambda x: x['x_quantiles']))

      def assert_boundaries(actual_boundaries):
        assert np.array_equal(actual_boundaries, expected_boundaries)

      _ = (actual_boundaries | beam.Map(assert_boundaries))


class ApplyBucketsTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  @parameterized.expand([
      (range(1, 100), [25, 50, 75]),
      (range(1, 100, 2), [25, 51, 75]),
  ])
  def test_apply_buckets(self, test_inputs, bucket_boundaries):
    with beam.Pipeline() as p:
      data = [{'x': [i]} for i in test_inputs]
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              artifact_location=self.artifact_location).with_transform(
                  tft.ApplyBuckets(
                      columns=['x'], bucket_boundaries=bucket_boundaries)))
      expected_output = []
      bucket = 0
      for x in sorted(test_inputs):
        # Increment the bucket number when crossing the boundary
        if (bucket < len(bucket_boundaries) and x >= bucket_boundaries[bucket]):
          bucket += 1
        expected_output.append(np.array([bucket]))

      actual_output = (result | beam.Map(lambda x: x.x))
      assert_that(
          actual_output, equal_to(expected_output, equals_fn=np.array_equal))


class ComputeAndApplyVocabTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_compute_and_apply_vocabulary_unbatched_inputs(self):
    batch_size = 100
    num_instances = batch_size + 1
    input_data = [{
        'x': '%.10i' % i,  # Front-padded to facilitate lexicographic sorting.
    } for i in range(num_instances)]

    expected_data = [{
        'x': (len(input_data) - 1) - i, # Due to reverse lexicographic sorting.
    } for i in range(len(input_data))]

    with beam.Pipeline() as p:
      actual_data = (
          p
          | "Create" >> beam.Create(input_data)
          | "MLTransform" >> base.MLTransform(
              artifact_location=self.artifact_location).with_transform(
                  tft.ComputeAndApplyVocabulary(columns=['x'])))
      actual_data |= beam.Map(lambda x: x.as_dict())

      assert_that(actual_data, equal_to(expected_data))

  def test_compute_and_apply_vocabulary_batched(self):
    batch_size = 100
    num_instances = batch_size + 1
    input_data = [
        {
            'x': ['%.10i' % i, '%.10i' % (i + 1), '%.10i' % (i + 2)],
            # Front-padded to facilitate lexicographic sorting.
        } for i in range(0, num_instances, 3)
    ]

    # since we have 3 elements in a single batch, multiply with 3 for
    # each iteration i on the expected output.
    excepted_data = [
        np.array([(len(input_data) * 3 - 1) - i,
                  (len(input_data) * 3 - 1) - i - 1,
                  (len(input_data) * 3 - 1) - i - 2],
                 dtype=np.int64)  # Front-padded to facilitate lexicographic
        # sorting.
        for i in range(0, len(input_data) * 3, 3)
    ]

    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(input_data)
          | "MLTransform" >> base.MLTransform(
              artifact_location=self.artifact_location).with_transform(
                  tft.ComputeAndApplyVocabulary(columns=['x'])))
      actual_output = (result | beam.Map(lambda x: x.x))
      assert_that(
          actual_output, equal_to(excepted_data, equals_fn=np.array_equal))


class TFIDIFTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_tfidf_batched_compute_vocab_size_during_runtime(self):
    raw_data = [
        dict(x=["I", "like", "pie", "pie", "pie"]),
        dict(x=["yum", "yum", "pie"])
    ]
    with beam.Pipeline() as p:
      transforms = [
          tft.ComputeAndApplyVocabulary(columns=['x']),
          tft.TFIDF(columns=['x'])
      ]
      actual_output = (
          p
          | "Create" >> beam.Create(raw_data)
          | "MLTransform" >> base.MLTransform(
              artifact_location=self.artifact_location, transforms=transforms))
      actual_output |= beam.Map(lambda x: x.as_dict())

      def equals_fn(a, b):
        is_equal = True
        for key, value in a.items():
          value_b = a[key]
          is_equal = is_equal and np.array_equal(value, value_b)
        return is_equal

      expected_output = ([{
          'x': np.array([3, 2, 0, 0, 0]),
          'x_tfidf_weight': np.array([0.6, 0.28109303, 0.28109303],
                                     dtype=np.float32),
          'x_vocab_index': np.array([0, 2, 3], dtype=np.int64)
      },
                          {
                              'x': np.array([1, 1, 0]),
                              'x_tfidf_weight': np.array(
                                  [0.33333334, 0.9369768], dtype=np.float32),
                              'x_vocab_index': np.array([0, 1], dtype=np.int32)
                          }])
      assert_that(actual_output, equal_to(expected_output, equals_fn=equals_fn))


if __name__ == '__main__':
  unittest.main()
