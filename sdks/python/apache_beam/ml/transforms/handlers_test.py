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
import sys
import tempfile
import typing
import unittest
import uuid
from typing import List
from typing import NamedTuple
from typing import Union

import numpy as np
from parameterized import parameterized

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems

# pylint: disable=wrong-import-position, ungrouped-imports
try:
  from apache_beam.ml.transforms import handlers
  from apache_beam.ml.transforms import tft
  from apache_beam.ml.transforms.tft import TFTOperation
  from apache_beam.testing.util import assert_that
  from apache_beam.testing.util import equal_to
  import tensorflow as tf
  from tensorflow_transform.tf_metadata import dataset_metadata
  from tensorflow_transform.tf_metadata import schema_utils
except ImportError:
  tft = None  # type: ignore[assignment]

if not tft:
  raise unittest.SkipTest('tensorflow_transform is not installed.')


class _AddOperation(TFTOperation):
  def apply_transform(self, inputs, output_column_name, **kwargs):
    return {output_column_name: inputs + 1}


class _MultiplyOperation(TFTOperation):
  def apply_transform(self, inputs, output_column_name, **kwargs):
    return {output_column_name: inputs * 10}


class IntType(NamedTuple):
  x: int


class ListIntType(NamedTuple):
  x: List[int]


class NumpyType(NamedTuple):
  x: np.int64


class TFTProcessHandlerTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  @parameterized.expand([
      ({
          'x': 1, 'y': 2
      }, ['x'], {
          'x': 20, 'y': 2
      }),
      ({
          'x': 1, 'y': 2
      }, ['x', 'y'], {
          'x': 20, 'y': 30
      }),
  ])
  def test_tft_operation_preprocessing_fn(
      self, inputs, columns, expected_result):
    add_fn = _AddOperation(columns=columns)
    mul_fn = _MultiplyOperation(columns=columns)
    process_handler = handlers.TFTProcessHandler(
        transforms=[add_fn, mul_fn], artifact_location=self.artifact_location)
    actual_result = process_handler.process_data_fn(inputs)
    self.assertDictEqual(actual_result, expected_result)

  def test_input_type_from_schema_named_tuple_pcoll(self):
    data = [{'x': 1}]
    with beam.Pipeline() as p:
      data = (
          p | beam.Create(data)
          | beam.Map(lambda x: IntType(**x)).with_output_types(IntType))
    element_type = data.element_type
    process_handler = handlers.TFTProcessHandler(
        artifact_location=self.artifact_location)
    inferred_input_type = process_handler._map_column_names_to_types(
        element_type)
    expected_input_type = dict(x=List[int])

    self.assertEqual(inferred_input_type, expected_input_type)

  def test_input_type_from_schema_named_tuple_pcoll_list(self):
    data = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      data = (
          p | beam.Create(data)
          | beam.Map(lambda x: ListIntType(**x)).with_output_types(ListIntType))
    element_type = data.element_type
    process_handler = handlers.TFTProcessHandler(
        artifact_location=self.artifact_location)
    inferred_input_type = process_handler._map_column_names_to_types(
        element_type)
    expected_input_type = dict(x=List[int])
    self.assertEqual(inferred_input_type, expected_input_type)

  def test_input_type_from_row_type_pcoll(self):
    data = [{'x': 1}]
    with beam.Pipeline() as p:
      data = (
          p | beam.Create(data)
          | beam.Map(lambda ele: beam.Row(x=int(ele['x']))))
    element_type = data.element_type
    process_handler = handlers.TFTProcessHandler(
        artifact_location=self.artifact_location)
    inferred_input_type = process_handler._map_column_names_to_types(
        element_type)
    expected_input_type = dict(x=List[int])
    self.assertEqual(inferred_input_type, expected_input_type)

  def test_input_type_from_row_type_pcoll_list(self):
    data = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      data = (
          p | beam.Create(data)
          | beam.Map(lambda ele: beam.Row(x=list(ele['x']))).with_output_types(
              beam.row_type.RowTypeConstraint.from_fields([('x', List[int])])))

    element_type = data.element_type
    process_handler = handlers.TFTProcessHandler(
        artifact_location=self.artifact_location)
    inferred_input_type = process_handler._map_column_names_to_types(
        element_type)
    expected_input_type = dict(x=List[int])
    self.assertEqual(inferred_input_type, expected_input_type)

  def test_input_type_from_named_tuple_pcoll_numpy(self):
    np_data = [{
        'x': np.array([1, 2, 3], dtype=np.int64)
    }, {
        'x': np.array([4, 5, 6], dtype=np.int64)
    }]
    with beam.Pipeline() as p:
      data = (
          p | beam.Create(np_data)
          | beam.Map(lambda x: NumpyType(**x)).with_output_types(NumpyType))
      element_type = data.element_type
      process_handler = handlers.TFTProcessHandler(
          artifact_location=self.artifact_location)
      inferred_input_type = process_handler._map_column_names_to_types(
          element_type)
      expected_type = dict(x=np.int64)
      self.assertEqual(inferred_input_type, expected_type)

  def test_tensorflow_raw_data_metadata_primitive_types(self):
    input_types = dict(x=int, y=float, k=bytes, l=str)
    process_handler = handlers.TFTProcessHandler(
        artifact_location=self.artifact_location)

    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertEqual(
          handlers._default_type_to_tensor_type_map[typ], feature_spec.dtype)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  def test_tensorflow_raw_data_metadata_primitive_types_in_containers(self):
    input_types = dict([("x", List[int]), ("y", List[float]),
                        ("k", List[bytes]), ("l", List[str])])
    process_handler = handlers.TFTProcessHandler(
        artifact_location=self.artifact_location)
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  @unittest.skipIf(sys.version_info < (3, 9), "not supported in python<3.9")
  def test_tensorflow_raw_data_metadata_primitive_native_container_types(self):
    input_types = dict([("x", list[int]), ("y", list[float]),
                        ("k", list[bytes]), ("l", list[str])])
    process_handler = handlers.TFTProcessHandler(
        artifact_location=self.artifact_location)
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  def test_tensorflow_raw_data_metadata_numpy_types(self):
    input_types = dict(x=np.int64, y=np.float32, z=List[np.int64])
    process_handler = handlers.TFTProcessHandler(
        artifact_location=self.artifact_location)
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  def test_tensorflow_raw_data_metadata_union_type_in_single_column(self):
    input_types = dict(x=Union[int, float])
    process_handler = handlers.TFTProcessHandler(
        artifact_location=self.artifact_location)
    with self.assertRaises(TypeError):
      for col_name, typ in input_types.items():
        _ = process_handler._get_raw_data_feature_spec_per_column(
            typ=typ, col_name=col_name)

  def test_tensorflow_raw_data_metadata_dtypes(self):
    input_types = dict(x=np.int32, y=np.float64)
    expected_dtype = dict(x=np.int64, y=np.float32)
    process_handler = handlers.TFTProcessHandler(
        artifact_location=self.artifact_location)
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertEqual(expected_dtype[col_name], feature_spec.dtype)

  def test_tft_process_handler_default_transform_types(self):
    transforms = [
        tft.ScaleTo01(columns=['x']),
        tft.ScaleToZScore(columns=['y']),
        tft.Bucketize(columns=['z'], num_buckets=2),
        tft.ComputeAndApplyVocabulary(columns=['w'])
    ]
    process_handler = handlers.TFTProcessHandler(
        transforms=transforms, artifact_location=self.artifact_location)
    column_type_mapping = (
        process_handler._map_column_names_to_types_from_transforms())
    expected_column_type_mapping = {
        'x': float, 'y': float, 'z': float, 'w': str
    }
    self.assertDictEqual(column_type_mapping, expected_column_type_mapping)

    expected_tft_raw_data_feature_spec = {
        'x': tf.io.VarLenFeature(tf.float32),
        'y': tf.io.VarLenFeature(tf.float32),
        'z': tf.io.VarLenFeature(tf.float32),
        'w': tf.io.VarLenFeature(tf.string)
    }
    actual_tft_raw_data_feature_spec = (
        process_handler.get_raw_data_feature_spec(column_type_mapping))
    self.assertDictEqual(
        actual_tft_raw_data_feature_spec, expected_tft_raw_data_feature_spec)

  def test_tft_process_handler_transformed_data_schema(self):
    process_handler = handlers.TFTProcessHandler(
        artifact_location=self.artifact_location)
    raw_data_feature_spec = {
        'x': tf.io.VarLenFeature(tf.float32),
        'y': tf.io.VarLenFeature(tf.float32),
        'z': tf.io.VarLenFeature(tf.string),
    }
    raw_data_metadata = dataset_metadata.DatasetMetadata(
        schema_utils.schema_from_feature_spec(raw_data_feature_spec))

    expected_transformed_data_schema = {
        'x': typing.Sequence[np.float32],
        'y': typing.Sequence[np.float32],
        'z': typing.Sequence[bytes]
    }

    actual_transformed_data_schema = (
        process_handler._get_transformed_data_schema(raw_data_metadata))
    self.assertDictEqual(
        actual_transformed_data_schema, expected_transformed_data_schema)

  def test_tft_process_handler_verify_artifacts(self):
    with beam.Pipeline() as p:
      raw_data = (
          p
          | beam.Create([{
              'x': np.array([1, 3])
          }, {
              'x': np.array([4, 6])
          }]))
      process_handler = handlers.TFTProcessHandler(
          transforms=[tft.ScaleTo01(columns=['x'])],
          artifact_location=self.artifact_location,
      )
      _ = raw_data | process_handler

      self.assertTrue(
          FileSystems.exists(
              # To check the gcs directory with FileSystems, the dir path must
              # end with /
              os.path.join(
                  self.artifact_location,
                  handlers.RAW_DATA_METADATA_DIR + '/')))
      self.assertTrue(
          FileSystems.exists(
              os.path.join(
                  self.artifact_location,
                  handlers.RAW_DATA_METADATA_DIR,
                  handlers.SCHEMA_FILE)))

    with beam.Pipeline() as p:
      raw_data = (p | beam.Create([{'x': np.array([2, 5])}]))
      process_handler = handlers.TFTProcessHandler(
          artifact_location=self.artifact_location, artifact_mode='consume')
      transformed_data = raw_data | process_handler
      transformed_data |= beam.Map(lambda x: x.x)

      # the previous min is 1 and max is 6. So this should scale by (1, 6)
      assert_that(
          transformed_data,
          equal_to([np.array([0.2, 0.8], dtype=np.float32)],
                   equals_fn=np.array_equal))

  def test_tft_process_handler_unused_column(self):
    data = [
        {
            'x': [5, 1], 'y': 2
        },
        {
            'x': [8, 4], 'y': 5
        },
        {
            'x': [9, 5], 'y': 6
        },
        {
            'x': [10, 6], 'y': 7
        },
        {
            'x': [11, 7], 'y': 8
        },
        {
            'x': [12, 8], 'y': 9
        },
        {
            'x': [13, 9], 'y': 10
        },
        {
            'x': [14, 10], 'y': 11
        },
        {
            'x': [15, 11], 'y': 12
        },
        {
            'x': [16, 12], 'y': 13
        },
        {
            'x': [17, 13], 'y': 14
        },
        {
            'x': [18, 14], 'y': 15
        },
        {
            'x': [19, 15], 'y': 16
        },
        {
            'x': [20, 16], 'y': 17
        },
        {
            'x': [21, 17], 'y': 18
        },
        {
            'x': [22, 18], 'y': 19
        },
        {
            'x': [23, 19], 'y': 20
        },
        {
            'x': [24, 20], 'y': 21
        },
        {
            'x': [25, 21], 'y': 22
        },
    ]
    # pylint: disable=line-too-long
    expected_data = [
        beam.Row(
            x=np.array([0.16666667, 0.], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=2),
        beam.Row(
            x=np.array([0.29166666, 0.125], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=5),
        beam.Row(
            x=np.array([0.33333334, 0.16666667], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=6),
        beam.Row(
            x=np.array([0.375, 0.20833333], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=7),
        beam.Row(
            x=np.array([0.41666666, 0.25], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=8),
        beam.Row(
            x=np.array([0.45833334, 0.29166666], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=9),
        beam.Row(
            x=np.array([0.5, 0.33333334], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=10),
        beam.Row(
            x=np.array([0.5416667, 0.375], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=11),
        beam.Row(
            x=np.array([0.5833333, 0.41666666], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=12),
        beam.Row(
            x=np.array([0.625, 0.45833334], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=13),
        beam.Row(
            x=np.array([0.6666667, 0.5], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=14),
        beam.Row(
            x=np.array([0.7083333, 0.5416667], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=15),
        beam.Row(
            x=np.array([0.75, 0.5833333], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=16),
        beam.Row(
            x=np.array([0.7916667, 0.625], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=17),
        beam.Row(
            x=np.array([0.8333333, 0.6666667], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=18),
        beam.Row(
            x=np.array([0.875, 0.7083333], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=19),
        beam.Row(
            x=np.array([0.9166667, 0.75], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=20),
        beam.Row(
            x=np.array([0.9583333, 0.7916667], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=21),
        beam.Row(
            x=np.array([1., 0.8333333], dtype=np.float32),
            x_max=np.array([25.], dtype=np.float32),
            x_min=np.array([1.], dtype=np.float32),
            y=22),
    ]
    # pylint: enable=line-too-long

    expected_data_x = [row.x for row in expected_data]
    expected_data_y = [row.y for row in expected_data]

    scale_to_0_1_fn = tft.ScaleTo01(columns=['x'])
    with beam.Pipeline() as p:
      raw_data = p | beam.Create(data)
      process_handler = handlers.TFTProcessHandler(
          transforms=[scale_to_0_1_fn],
          artifact_location=self.artifact_location,
      )
      transformed_pcoll = raw_data | process_handler
      transformed_pcoll_x = transformed_pcoll | beam.Map(lambda x: x.x)
      transformed_pcoll_y = transformed_pcoll | beam.Map(lambda x: x.y)
      assert_that(
          transformed_pcoll_x,
          equal_to(expected_data_x, equals_fn=np.array_equal),
          label='transformed data')

      assert_that(
          transformed_pcoll_y,
          equal_to(expected_data_y, equals_fn=np.array_equal),
          label='unused column')

  def test_consume_mode_with_extra_columns_in_the_input(self):
    with beam.Pipeline() as p:
      raw_data = (
          p
          | beam.Create([{
              'x': np.array([1, 3])
          }, {
              'x': np.array([4, 6])
          }]))
      process_handler = handlers.TFTProcessHandler(
          transforms=[tft.ScaleTo01(columns=['x'])],
          artifact_location=self.artifact_location,
      )
      _ = raw_data | process_handler

    test_data = [{
        'x': np.array([2, 5]), 'y': np.array([1, 2]), 'z': 'fake_string'
    },
                 {
                     'x': np.array([1, 10]),
                     'y': np.array([2, 3]),
                     'z': 'fake_string2'
                 }]
    expected_test_data = [{
        'x': np.array([0.2, 0.8], dtype=np.float32),
        'y': np.array([1, 2]),
        'z': 'fake_string'
    },
                          {
                              'x': np.array([0., 1.8], dtype=np.float32),
                              'y': np.array([2, 3]),
                              'z': 'fake_string2'
                          }]

    expected_test_data_x = [row['x'] for row in expected_test_data]
    expected_test_data_y = [row['y'] for row in expected_test_data]
    expected_test_data_z = [row['z'] for row in expected_test_data]
    with beam.Pipeline() as p:
      raw_data = (p | beam.Create(test_data))
      process_handler = handlers.TFTProcessHandler(
          artifact_location=self.artifact_location, artifact_mode='consume')
      transformed_data = raw_data | process_handler

      transformed_data_x = transformed_data | beam.Map(lambda x: x.x)
      transformed_data_y = transformed_data | beam.Map(lambda x: x.y)
      transformed_data_z = transformed_data | beam.Map(lambda x: x.z)

      assert_that(
          transformed_data_x,
          equal_to(expected_test_data_x, equals_fn=np.array_equal),
          label='transformed data')

      assert_that(
          transformed_data_y,
          equal_to(expected_test_data_y, equals_fn=np.array_equal),
          label='unused column: y')

      assert_that(
          transformed_data_z,
          equal_to(expected_test_data_z, equals_fn=np.array_equal),
          label='unused column: z')

  def test_handler_with_same_input_elements(self):
    with beam.Pipeline() as p:
      data = [
          {
              'x': 'I'
          },
          {
              'x': 'love'
          },
          {
              'x': 'Beam'
          },
          {
              'x': 'Beam'
          },
          {
              'x': 'is'
          },
          {
              'x': 'awesome'
          },
      ]
      raw_data = (p | beam.Create(data))
      process_handler = handlers.TFTProcessHandler(
          transforms=[tft.ComputeAndApplyVocabulary(columns=['x'])],
          artifact_location=self.artifact_location,
      )
      transformed_data = raw_data | process_handler

      expected_data = [
          beam.Row(x=np.array([4])),
          beam.Row(x=np.array([1])),
          beam.Row(x=np.array([0])),
          beam.Row(x=np.array([0])),
          beam.Row(x=np.array([2])),
          beam.Row(x=np.array([3])),
      ]

      expected_data_x = [row.x for row in expected_data]
      actual_data_x = transformed_data | beam.Map(lambda x: x.x)

      assert_that(
          actual_data_x,
          equal_to(expected_data_x, equals_fn=np.array_equal),
          label='transformed data')


class TFTProcessHandlerTestWithGCSLocation(TFTProcessHandlerTest):
  def setUp(self) -> None:
    self.artifact_location = self.gcs_artifact_location = os.path.join(
        'gs://temp-storage-for-perf-tests/tft_handler', uuid.uuid4().hex)

  def tearDown(self):
    pass


if __name__ == '__main__':
  unittest.main()
