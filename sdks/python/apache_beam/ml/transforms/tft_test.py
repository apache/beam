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

import os
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


class ScaleZScoreTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_z_score(self):
    data = [
        {
            'x': 1
        },
        {
            'x': 2
        },
        {
            'x': 3
        },
        {
            'x': 4
        },
        {
            'x': 5
        },
        {
            'x': 6
        },
    ]

    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ScaleToZScore(columns=['x'])))
      expected_data = [
          np.array([-1.46385], dtype=np.float32),
          np.array([-0.87831], dtype=np.float32),
          np.array([-0.29277], dtype=np.float32),
          np.array([0.29277], dtype=np.float32),
          np.array([0.87831], dtype=np.float32),
          np.array([1.46385], dtype=np.float32),
      ]

      actual_data = (result | beam.Map(lambda x: x.x))
      assert_that(
          actual_data, equal_to(expected_data, equals_fn=np.array_equal))

  def test_z_score_list_data(self):
    list_data = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      list_result = (
          p
          | "listCreate" >> beam.Create(list_data)
          | "listMLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ScaleToZScore(columns=['x'])))

      expected_data = [
          np.array([-1.46385, -0.87831, -0.29277], dtype=np.float32),
          np.array([0.29277, 0.87831, 1.46385], dtype=np.float32)
      ]
      actual_data = (list_result | beam.Map(lambda x: x.x))
      assert_that(
          actual_data, equal_to(expected_data, equals_fn=np.array_equal))


class ScaleTo01Test(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_ScaleTo01_list(self):
    list_data = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      list_result = (
          p
          | "listCreate" >> beam.Create(list_data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ScaleTo01(columns=['x'])))

      expected_output = [
          np.array([0, 0.2, 0.4], dtype=np.float32),
          np.array([0.6, 0.8, 1], dtype=np.float32)
      ]
      actual_output = (list_result | beam.Map(lambda x: x.x))
      assert_that(
          actual_output, equal_to(expected_output, equals_fn=np.array_equal))

  def test_ScaleTo01(self):
    data = [{'x': 1}, {'x': 2}, {'x': 3}, {'x': 4}, {'x': 5}, {'x': 6}]
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ScaleTo01(columns=['x'])))

      expected_output = (
          np.array([0], dtype=np.float32),
          np.array([0.2], dtype=np.float32),
          np.array([0.4], dtype=np.float32),
          np.array([0.6], dtype=np.float32),
          np.array([0.8], dtype=np.float32),
          np.array([1], dtype=np.float32))
      actual_output = (result | beam.Map(lambda x: x.x))
      assert_that(
          actual_output, equal_to(expected_output, equals_fn=np.array_equal))


class ScaleToGaussianTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_gaussian_list_uniform_distribution(self):
    list_data = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      list_result = (
          p
          | "listCreate" >> beam.Create(list_data)
          | "listMLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ScaleToGaussian(columns=['x'])))

      expected_data = [
          np.array([-1.46385, -0.87831, -0.29277], dtype=np.float32),
          np.array([0.29277, 0.87831, 1.46385], dtype=np.float32)
      ]
      actual_data = (list_result | beam.Map(lambda x: x.x))
      assert_that(
          actual_data, equal_to(expected_data, equals_fn=np.array_equal))

  def test_gaussian_list_skewed_distribution(self):
    list_data = [{'x': [1, 2, 4]}, {'x': [8, 16, 32]}]
    with beam.Pipeline() as p:
      list_result = (
          p
          | "listCreate" >> beam.Create(list_data)
          | "listMLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ScaleToGaussian(columns=['x'])))

      expected_data = [
          np.array([-0.87733847, -0.78498703, -0.6002842], dtype=np.float32),
          np.array([-0.23087855, 0.5079328, 1.9855555], dtype=np.float32)
      ]
      actual_data = (list_result | beam.Map(lambda x: x.x))
      assert_that(actual_data, equal_to(expected_data, equals_fn=np.allclose))

  def test_gaussian_uniform(self):
    data = [
        {
            'x': 1
        },
        {
            'x': 2
        },
        {
            'x': 3
        },
        {
            'x': 4
        },
        {
            'x': 5
        },
        {
            'x': 6
        },
    ]

    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ScaleToZScore(columns=['x'])))
      expected_data = [
          np.array([-1.46385], dtype=np.float32),
          np.array([-0.87831], dtype=np.float32),
          np.array([-0.29277], dtype=np.float32),
          np.array([0.29277], dtype=np.float32),
          np.array([0.87831], dtype=np.float32),
          np.array([1.46385], dtype=np.float32),
      ]

      actual_data = (result | beam.Map(lambda x: x.x))
      assert_that(
          actual_data, equal_to(expected_data, equals_fn=np.array_equal))

  def test_gaussian_skewed(self):
    data = [
        {
            'x': 1
        },
        {
            'x': 2
        },
        {
            'x': 4
        },
        {
            'x': 8
        },
        {
            'x': 16
        },
        {
            'x': 32
        },
    ]

    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ScaleToZScore(columns=['x'])))
      expected_data = [
          np.array([-0.87733847], dtype=np.float32),
          np.array([-0.78498703], dtype=np.float32),
          np.array([-0.6002842], dtype=np.float32),
          np.array([-0.23087855], dtype=np.float32),
          np.array([0.5079328], dtype=np.float32),
          np.array([1.9855555], dtype=np.float32),
      ]

      actual_data = (result | beam.Map(lambda x: x.x))
      assert_that(actual_data, equal_to(expected_data, equals_fn=np.allclose))


class BucketizeTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_bucketize(self):
    data = [{'x': 1}, {'x': 2}, {'x': 3}, {'x': 4}, {'x': 5}, {'x': 6}]
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.Bucketize(columns=['x'], num_buckets=3)))

      transformed_data = (result | beam.Map(lambda x: x.x))
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

  def test_bucketize_list(self):
    list_data = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      list_result = (
          p
          | "Create" >> beam.Create(list_data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.Bucketize(columns=['x'], num_buckets=3)))
      transformed_data = (
          list_result
          | "TransformedColumnX" >> beam.Map(lambda ele: ele.x))
      expected_data = [
          np.array([0, 0, 1], dtype=np.int64),
          np.array([1, 2, 2], dtype=np.int64)
      ]
      assert_that(
          transformed_data, equal_to(expected_data, equals_fn=np.array_equal))


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
              write_artifact_location=self.artifact_location).with_transform(
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


class ApplyBucketsWithInterpolationTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  @parameterized.expand([
      ([-1, 9, 10, 11], [10], [0., 0., 1., 1.]),
      ([15, 20, 25], [10, 20], [.5, 1, 1]),
  ])
  def test_apply_buckets(self, test_inputs, bucket_boundaries, expected_values):
    with beam.Pipeline() as p:
      data = [{'x': [i]} for i in test_inputs]
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ApplyBucketsWithInterpolation(
                      columns=['x'], bucket_boundaries=bucket_boundaries)))
      expected_output = []
      for x in expected_values:
        expected_output.append(np.array(x))

      actual_output = (result | beam.Map(lambda x: x.x))
      assert_that(
          actual_output, equal_to(expected_output, equals_fn=np.allclose))


class ComputeAndApplyVocabTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_compute_and_apply_vocabulary_inputs(self):
    num_elements = 100
    num_instances = num_elements + 1
    input_data = [
        {
            'x': '%.10i' %
            i,  # Front-padded to facilitate lexicographic sorting.
        } for i in range(num_instances)
    ]

    expected_data = [
        {
            'x': (len(input_data) - 1) -
            i,  # Due to reverse lexicographic sorting.
        } for i in range(len(input_data))
    ]

    with beam.Pipeline() as p:
      actual_data = (
          p
          | "Create" >> beam.Create(input_data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ComputeAndApplyVocabulary(columns=['x'])))
      actual_data |= beam.Map(lambda x: x.as_dict())

      assert_that(actual_data, equal_to(expected_data))

  def test_compute_and_apply_vocabulary(self):
    num_elements = 100
    num_instances = num_elements + 1
    input_data = [
        {
            'x': ['%.10i' % i, '%.10i' % (i + 1), '%.10i' % (i + 2)],
            # Front-padded to facilitate lexicographic sorting.
        } for i in range(0, num_instances, 3)
    ]

    # since we have 3 elements in a single list, multiply with 3 for
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
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ComputeAndApplyVocabulary(columns=['x'])))
      actual_output = (result | beam.Map(lambda x: x.x))
      assert_that(
          actual_output, equal_to(excepted_data, equals_fn=np.array_equal))

  def test_with_basic_example_list(self):
    data = [{
        'x': ['I', 'like', 'pie'],
    }, {
        'x': ['yum', 'yum', 'pie'],
    }]
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ComputeAndApplyVocabulary(columns=['x'])))
      result = result | beam.Map(lambda x: x.x)
      expected_result = [np.array([3, 2, 1]), np.array([0, 0, 1])]
      assert_that(result, equal_to(expected_result, equals_fn=np.array_equal))

  def test_string_split_with_single_delimiter(self):
    data = [{
        'x': 'I like pie',
    }, {
        'x': 'yum yum pie'
    }]
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ComputeAndApplyVocabulary(
                      columns=['x'], split_string_by_delimiter=' ')))
      result = result | beam.Map(lambda x: x.x)
      expected_result = [np.array([3, 2, 1]), np.array([0, 0, 1])]
      assert_that(result, equal_to(expected_result, equals_fn=np.array_equal))

  def test_string_split_with_multiple_delimiters(self):
    data = [{
        'x': 'I like pie',
    }, {
        'x': 'yum;yum;pie'
    }, {
        'x': 'yum yum pie'
    }]

    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ComputeAndApplyVocabulary(
                      columns=['x'], split_string_by_delimiter=' ;')))
      result = result | beam.Map(lambda x: x.x)
      expected_result = [
          np.array([3, 2, 1]), np.array([0, 0, 1]), np.array([0, 0, 1])
      ]
      assert_that(result, equal_to(expected_result, equals_fn=np.array_equal))

  def test_multiple_columns_with_default_vocab_name(self):
    data = [{
        'x': ['I', 'like', 'pie'], 'y': ['Apach', 'Beam', 'is', 'awesome']
    },
            {
                'x': ['yum', 'yum', 'pie'],
                'y': ['Beam', 'is', 'a', 'unified', 'model']
            }]
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ComputeAndApplyVocabulary(columns=['x', 'y'])))

      expected_data_x = [np.array([3, 2, 1]), np.array([0, 0, 1])]

      expected_data_y = [np.array([6, 1, 0, 4]), np.array([1, 0, 5, 2, 3])]

      actual_data_x = (result | beam.Map(lambda x: x.x))
      actual_data_y = (result | beam.Map(lambda x: x.y))

      assert_that(
          actual_data_x,
          equal_to(expected_data_x, equals_fn=np.array_equal),
          label='x')
      assert_that(
          actual_data_y,
          equal_to(expected_data_y, equals_fn=np.array_equal),
          label='y')
    files = os.listdir(self.artifact_location)
    files.remove(base._ATTRIBUTE_FILE_NAME)
    assert len(files) == 1
    tft_vocab_assets = os.listdir(
        os.path.join(
            self.artifact_location, files[0], 'transform_fn', 'assets'))
    assert len(tft_vocab_assets) == 2

  def test_multiple_columns_with_vocab_name(self):
    data = [{
        'x': ['I', 'like', 'pie'], 'y': ['Apach', 'Beam', 'is', 'awesome']
    },
            {
                'x': ['yum', 'yum', 'pie'],
                'y': ['Beam', 'is', 'a', 'unified', 'model']
            }]
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ComputeAndApplyVocabulary(
                      columns=['x', 'y'], vocab_filename='my_vocab')))

      expected_data_x = [np.array([3, 2, 1]), np.array([0, 0, 1])]

      expected_data_y = [np.array([6, 1, 0, 4]), np.array([1, 0, 5, 2, 3])]

      actual_data_x = (result | beam.Map(lambda x: x.x))
      actual_data_y = (result | beam.Map(lambda x: x.y))

      assert_that(
          actual_data_x,
          equal_to(expected_data_x, equals_fn=np.array_equal),
          label='x')
      assert_that(
          actual_data_y,
          equal_to(expected_data_y, equals_fn=np.array_equal),
          label='y')
    files = os.listdir(self.artifact_location)
    files.remove(base._ATTRIBUTE_FILE_NAME)
    assert len(files) == 1
    tft_vocab_assets = os.listdir(
        os.path.join(
            self.artifact_location, files[0], 'transform_fn', 'assets'))
    assert len(tft_vocab_assets) == 2


class TFIDIFTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_tfidf_compute_vocab_size_during_runtime(self):
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
              write_artifact_location=self.artifact_location,
              transforms=transforms))
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


class ScaleToMinMaxTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_scale_to_min_max(self):
    data = [{
        'x': 4,
    }, {
        'x': 1,
    }, {
        'x': 5,
    }, {
        'x': 2,
    }]
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.ScaleByMinMax(
                      columns=['x'],
                      min_value=-1,
                      max_value=1,
                  ),
              ))
      result = result | beam.Map(lambda x: x.as_dict())
      expected_data = [{
          'x': np.array([0.5], dtype=np.float32)
      }, {
          'x': np.array([-1.0], dtype=np.float32)
      }, {
          'x': np.array([1.0], dtype=np.float32)
      }, {
          'x': np.array([-0.5], dtype=np.float32)
      }]
      assert_that(result, equal_to(expected_data))

  def test_fail_max_value_less_than_min(self):
    with self.assertRaises(ValueError):
      tft.ScaleByMinMax(columns=['x'], min_value=10, max_value=0)


class NGramsTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_ngrams_on_list_separated_words_default_args(self):
    data = [{
        'x': ['I', 'like', 'pie'],
    }, {
        'x': ['yum', 'yum', 'pie']
    }]
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location,
              transforms=[tft.NGrams(columns=['x'])]))
      result = result | beam.Map(lambda x: x.x)
      expected_data = [
          np.array([b'I', b'like', b'pie'], dtype=object),
          np.array([b'yum', b'yum', b'pie'], dtype=object)
      ]
      assert_that(result, equal_to(expected_data, equals_fn=np.array_equal))

  def test_ngrams_on_list_separated_words(self):
    data = [{
        'x': ['I', 'like', 'pie'],
    }, {
        'x': ['yum', 'yum', 'pie']
    }]
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location,
              transforms=[
                  tft.NGrams(
                      columns=['x'], ngram_range=(1, 3), ngrams_separator=' ')
              ]))
      result = result | beam.Map(lambda x: x.x)
      expected_data = [
          np.array(
              [b'I', b'I like', b'I like pie', b'like', b'like pie', b'pie'],
              dtype=object),
          np.array(
              [b'yum', b'yum yum', b'yum yum pie', b'yum', b'yum pie', b'pie'],
              dtype=object)
      ]
      assert_that(result, equal_to(expected_data, equals_fn=np.array_equal))

  def test_with_string_split_delimiter(self):
    data = [{
        'x': 'I like pie',
    }, {
        'x': 'yum yum pie'
    }]
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location,
              transforms=[
                  tft.NGrams(
                      columns=['x'],
                      split_string_by_delimiter=' ',
                      ngram_range=(1, 3),
                      ngrams_separator=' ')
              ]))
      result = result | beam.Map(lambda x: x.x)

      expected_data = [
          np.array(
              [b'I', b'I like', b'I like pie', b'like', b'like pie', b'pie'],
              dtype=object),
          np.array(
              [b'yum', b'yum yum', b'yum yum pie', b'yum', b'yum pie', b'pie'],
              dtype=object)
      ]
      assert_that(result, equal_to(expected_data, equals_fn=np.array_equal))

  def test_with_multiple_string_delimiters(self):
    data = [{
        'x': 'I?like?pie',
    }, {
        'x': 'yum yum pie'
    }]
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location,
              transforms=[
                  tft.NGrams(
                      columns=['x'],
                      split_string_by_delimiter=' ?',
                      ngram_range=(1, 3),
                      ngrams_separator=' ')
              ]))
      result = result | beam.Map(lambda x: x.x)

      expected_data = [
          np.array(
              [b'I', b'I like', b'I like pie', b'like', b'like pie', b'pie'],
              dtype=object),
          np.array(
              [b'yum', b'yum yum', b'yum yum pie', b'yum', b'yum pie', b'pie'],
              dtype=object)
      ]
      assert_that(result, equal_to(expected_data, equals_fn=np.array_equal))


class BagOfWordsTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_bag_of_words_on_list_seperated_words_default_ngrams(self):
    data = [{
        'x': ['I', 'like', 'pie', 'pie', 'pie'],
    }, {
        'x': ['yum', 'yum', 'pie']
    }]

    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location,
              transforms=[tft.BagOfWords(columns=['x'])]))
      result = result | beam.Map(lambda x: x.x)

      expected_data = [
          np.array([b'I', b'like', b'pie'], dtype=object),
          np.array([b'yum', b'pie'], dtype=object)
      ]
      assert_that(result, equal_to(expected_data, equals_fn=np.array_equal))

  def test_bag_of_words_on_list_seperated_words_custom_ngrams(self):
    data = [{
        'x': ['I', 'like', 'pie', 'I', 'like', 'pie'],
    }, {
        'x': ['yum', 'yum', 'pie']
    }]
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location,
              transforms=[
                  tft.BagOfWords(
                      columns=['x'], ngram_range=(1, 3), ngrams_separator=' ')
              ]))
      result = result | beam.Map(lambda x: x.x)

      expected_data = [[
          b'I',
          b'I like',
          b'I like pie',
          b'like',
          b'like pie',
          b'like pie I',
          b'pie',
          b'pie I',
          b'pie I like'
      ], [b'yum', b'yum yum', b'yum yum pie', b'yum pie', b'pie']]
      assert_that(result, equal_to(expected_data, equals_fn=np.array_equal))

  def test_bag_of_words_on_numpy_data(self):
    data = [{
        'x': np.array(['I', 'like', 'pie', 'I', 'like', 'pie'], dtype=object),
    }, {
        'x': np.array(['yum', 'yum', 'pie'], dtype=object)
    }]
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location,
              transforms=[
                  tft.BagOfWords(
                      columns=['x'], ngram_range=(1, 3), ngrams_separator=' ')
              ]))
      result = result | beam.Map(lambda x: x.x)

      expected_data = [[
          b'I',
          b'I like',
          b'I like pie',
          b'like',
          b'like pie',
          b'like pie I',
          b'pie',
          b'pie I',
          b'pie I like'
      ], [b'yum', b'yum yum', b'yum yum pie', b'yum pie', b'pie']]
      assert_that(result, equal_to(expected_data, equals_fn=np.array_equal))

  def test_bag_of_words_on_by_splitting_input_text(self):
    data = [{'x': 'I like pie I like pie'}, {'x': 'yum yum pie'}]
    with beam.Pipeline() as p:
      result = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location,
              transforms=[
                  tft.BagOfWords(
                      columns=['x'],
                      split_string_by_delimiter=' ',
                      ngram_range=(1, 3),
                      ngrams_separator=' ')
              ]))
      result = result | beam.Map(lambda x: x.x)

      expected_data = [[
          b'I',
          b'I like',
          b'I like pie',
          b'like',
          b'like pie',
          b'like pie I',
          b'pie',
          b'pie I',
          b'pie I like'
      ], [b'yum', b'yum yum', b'yum yum pie', b'yum pie', b'pie']]
      assert_that(result, equal_to(expected_data, equals_fn=np.array_equal))

  def test_count_per_key_on_list(self):
    data = [{
        'x': ['I', 'like', 'pie', 'pie', 'pie'],
    }, {
        'x': ['yum', 'yum', 'pie']
    }, {
        'x': ['Banana', 'Banana', 'Apple', 'Apple', 'Apple', 'Apple']
    }]
    with beam.Pipeline() as p:
      _ = (
          p
          | "Create" >> beam.Create(data)
          | "MLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location,
              transforms=[
                  tft.BagOfWords(
                      columns=['x'],
                      compute_word_count=True,
                      key_vocab_filename='my_vocab')
              ]))

    def validate_count_per_key(key_vocab_filename):
      files = os.listdir(self.artifact_location)
      files.remove(base._ATTRIBUTE_FILE_NAME)
      key_vocab_location = os.path.join(
          self.artifact_location,
          files[0],
          'transform_fn/assets',
          key_vocab_filename + '_x')
      with open(key_vocab_location, 'r') as f:
        key_vocab_list = [line.strip() for line in f]
      return key_vocab_list

    expected_data = ['2 yum', '4 Apple', '1 like', '1 I', '4 pie', '2 Banana']
    actual_data = validate_count_per_key('my_vocab')
    self.assertEqual(expected_data, actual_data)


class HashStringsTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_single_bucket(self):
    list_data = [{'x': 'this is a test string'}]
    expected_values = [np.array([0])]
    with beam.Pipeline() as p:
      list_result = (
          p
          | "listCreate" >> beam.Create(list_data)
          | "listMLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.HashStrings(columns=['x'], hash_buckets=1)))
      result = (list_result | beam.Map(lambda x: x.x))
      assert_that(result, equal_to(expected_values, equals_fn=np.array_equal))

  def test_multi_bucket_one_string(self):
    list_data = [{'x': 'this is a test string'}]
    expected_values = [np.array([1])]
    with beam.Pipeline() as p:
      list_result = (
          p
          | "listCreate" >> beam.Create(list_data)
          | "listMLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.HashStrings(columns=['x'], hash_buckets=2)))
      result = (list_result | beam.Map(lambda x: x.x))
      assert_that(result, equal_to(expected_values, equals_fn=np.array_equal))

  def test_one_bucket_multi_string(self):
    list_data = [{'x': ['these', 'are', 'test', 'strings']}]
    expected_values = [np.array([0, 0, 0, 0])]
    with beam.Pipeline() as p:
      list_result = (
          p
          | "listCreate" >> beam.Create(list_data)
          | "listMLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.HashStrings(columns=['x'], hash_buckets=1)))
      result = (list_result | beam.Map(lambda x: x.x))
      assert_that(result, equal_to(expected_values, equals_fn=np.array_equal))

  def test_two_bucket_multi_string(self):
    list_data = [{'x': ['these', 'are', 'test', 'strings']}]
    expected_values = [np.array([1, 0, 1, 0])]
    with beam.Pipeline() as p:
      list_result = (
          p
          | "listCreate" >> beam.Create(list_data)
          | "listMLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.HashStrings(
                      columns=['x'], hash_buckets=2, key=[123, 456])))
      result = (list_result | beam.Map(lambda x: x.x))
      assert_that(result, equal_to(expected_values, equals_fn=np.array_equal))

  def test_multi_buckets_multi_string(self):
    # This is a recreation of one of the TFT test cases from
    # https://github.com/tensorflow/transform/blob/master/tensorflow_transform/mappers_test.py
    list_data = [{'x': ['Cake', 'Pie', 'Sundae']}]
    expected_values = [np.array([6, 5, 6])]
    with beam.Pipeline() as p:
      list_result = (
          p
          | "listCreate" >> beam.Create(list_data)
          | "listMLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.HashStrings(
                      columns=['x'], hash_buckets=11, key=[123, 456])))
      result = (list_result | beam.Map(lambda x: x.x))
      assert_that(result, equal_to(expected_values, equals_fn=np.array_equal))


class DeduplicateTensorPerRowTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_deduplicate(self):
    values = [{
        'x': [b'a', b'b', b'a', b'b'],
    }, {
        'x': [b'b', b'c', b'b', b'c']
    }]

    expected_output = [np.array([b'a', b'b']), np.array([b'b', b'c'])]
    with beam.Pipeline() as p:
      list_result = (
          p
          | "listCreate" >> beam.Create(values)
          | "listMLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.DeduplicateTensorPerRow(columns=['x'])))
      result = (list_result | beam.Map(lambda x: x.x))
      assert_that(result, equal_to(expected_output, equals_fn=np.array_equal))

  def test_deduplicate_no_op(self):
    values = [{
        'x': [b'a', b'b'],
    }, {
        'x': [b'c', b'd']
    }]

    expected_output = [np.array([b'a', b'b']), np.array([b'c', b'd'])]
    with beam.Pipeline() as p:
      list_result = (
          p
          | "listCreate" >> beam.Create(values)
          | "listMLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.DeduplicateTensorPerRow(columns=['x'])))
      result = (list_result | beam.Map(lambda x: x.x))
      assert_that(result, equal_to(expected_output, equals_fn=np.array_equal))

  def test_deduplicate_different_output_sizes(self):
    values = [{
        'x': [b'a', b'b', b'a', b'b'],
    }, {
        'x': [b'c', b'a', b'd', b'd']
    }]

    expected_output = [np.array([b'a', b'b']), np.array([b'c', b'a', b'd'])]
    with beam.Pipeline() as p:
      list_result = (
          p
          | "listCreate" >> beam.Create(values)
          | "listMLTransform" >> base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  tft.DeduplicateTensorPerRow(columns=['x'])))
      result = (list_result | beam.Map(lambda x: x.x))
      assert_that(result, equal_to(expected_output, equals_fn=np.array_equal))


if __name__ == '__main__':
  unittest.main()
