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

import shutil
import tempfile
import typing
from typing import NamedTuple
from typing import List
from typing import Union

import unittest
import numpy as np
from parameterized import param
from parameterized import parameterized

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=wrong-import-position, ungrouped-imports
try:
  from apache_beam.ml.transforms import base
  from apache_beam.ml.transforms import handlers
  from apache_beam.ml.transforms import tft_transforms
  from apache_beam.ml.transforms.tft_transforms import TFTOperation
  import tensorflow as tf
  from tensorflow_transform.tf_metadata import dataset_metadata
  from tensorflow_transform.tf_metadata import schema_utils
except ImportError:
  tft_transforms = None

if not tft_transforms:
  raise unittest.SkipTest('tensorflow_transform is not installed.')


class _FakeOperation(TFTOperation):
  def __init__(self, name, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.name = name

  def apply(self, inputs, output_column_name, **kwargs):
    return {output_column_name: inputs}


class _AddOperation(TFTOperation):
  def apply(self, inputs, output_column_name, **kwargs):
    return {output_column_name: inputs + 1}


class _MultiplyOperation(TFTOperation):
  def apply(self, inputs, output_column_name, **kwargs):
    return {output_column_name: inputs * 10}


class _FakeOperationWithArtifacts(TFTOperation):
  def apply(self, inputs, output_column_name, **kwargs):
    return {
        **{
            output_column_name: inputs
        },
        **(self.get_artifacts(inputs, 'artifact'))
    }

  def get_artifacts(self, data, col_name):
    return {'artifact': tf.convert_to_tensor([1])}


class UnBatchedIntType(NamedTuple):
  x: int


class BatchedIntType(NamedTuple):
  x: List[int]


class BatchedNumpyType(NamedTuple):
  x: np.int64


class TFTProcessHandlerOnSchemaDataTest(unittest.TestCase):
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
    process_handler = handlers.TFTProcessHandler(transforms=[add_fn, mul_fn])

    actual_result = process_handler.process_data_fn(inputs)
    self.assertDictEqual(actual_result, expected_result)

  def test_preprocessing_fn_with_artifacts(self):
    process_handler = handlers.TFTProcessHandler(
        transforms=[_FakeOperationWithArtifacts(columns=['x'])])
    inputs = {'x': [1, 2, 3]}
    preprocessing_fn = process_handler.process_data_fn
    actual_result = preprocessing_fn(inputs)
    expected_result = {'x': [1, 2, 3], 'artifact': tf.convert_to_tensor([1])}
    self.assertDictEqual(actual_result, expected_result)

  def test_ml_transform_appends_transforms_to_process_handler_correctly(self):
    fake_fn_1 = _FakeOperation(name='fake_fn_1', columns=['x'])
    transforms = [fake_fn_1]
    ml_transform = base.MLTransform(
        transforms=transforms, artifact_location=self.artifact_location)
    ml_transform = ml_transform.with_transform(
        transform=_FakeOperation(name='fake_fn_2', columns=['x']))

    self.assertEqual(len(ml_transform._process_handler.transforms), 2)
    self.assertEqual(
        ml_transform._process_handler.transforms[0].name, 'fake_fn_1')
    self.assertEqual(
        ml_transform._process_handler.transforms[1].name, 'fake_fn_2')

  def test_input_type_from_schema_named_tuple_pcoll_unbatched(self):
    non_batched_data = [{'x': 1}]
    with beam.Pipeline() as p:
      data = (
          p | beam.Create(non_batched_data)
          | beam.Map(lambda x: UnBatchedIntType(**x)).with_output_types(
              UnBatchedIntType))
    element_type = data.element_type
    process_handler = handlers.TFTProcessHandler()
    inferred_input_type = process_handler._map_column_names_to_types(
        element_type)
    expected_input_type = dict(x=List[int])

    self.assertEqual(inferred_input_type, expected_input_type)

  def test_input_type_from_schema_named_tuple_pcoll_batched(self):
    batched_data = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      data = (
          p | beam.Create(batched_data)
          | beam.Map(lambda x: BatchedIntType(**x)).with_output_types(
              BatchedIntType))
    element_type = data.element_type
    process_handler = handlers.TFTProcessHandler()
    inferred_input_type = process_handler._map_column_names_to_types(
        element_type)
    expected_input_type = dict(x=List[int])
    self.assertEqual(inferred_input_type, expected_input_type)

  def test_input_type_from_row_type_pcoll_unbatched(self):
    non_batched_data = [{'x': 1}]
    with beam.Pipeline() as p:
      data = (
          p | beam.Create(non_batched_data)
          | beam.Map(lambda ele: beam.Row(x=int(ele['x']))))
    element_type = data.element_type
    process_handler = handlers.TFTProcessHandler()
    inferred_input_type = process_handler._map_column_names_to_types(
        element_type)
    expected_input_type = dict(x=List[int])
    self.assertEqual(inferred_input_type, expected_input_type)

  def test_input_type_from_row_type_pcoll_batched(self):
    batched_data = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      data = (
          p | beam.Create(batched_data)
          | beam.Map(lambda ele: beam.Row(x=list(ele['x']))).with_output_types(
              beam.row_type.RowTypeConstraint.from_fields([('x', List[int])])))

    element_type = data.element_type
    process_handler = handlers.TFTProcessHandler()
    inferred_input_type = process_handler._map_column_names_to_types(
        element_type)
    expected_input_type = dict(x=List[int])
    self.assertEqual(inferred_input_type, expected_input_type)

  def test_input_type_from_named_tuple_pcoll_batched_numpy(self):
    batched = [{
        'x': np.array([1, 2, 3], dtype=np.int64)
    }, {
        'x': np.array([4, 5, 6], dtype=np.int64)
    }]
    with beam.Pipeline() as p:
      data = (
          p | beam.Create(batched)
          | beam.Map(lambda x: BatchedNumpyType(**x)).with_output_types(
              BatchedNumpyType))
      element_type = data.element_type
      process_handler = handlers.TFTProcessHandler()
      inferred_input_type = process_handler._map_column_names_to_types(
          element_type)
      expected_type = dict(x=np.int64)
      self.assertEqual(inferred_input_type, expected_type)

  def test_tensorflow_raw_data_metadata_primitive_types(self):
    input_types = dict(x=int, y=float, k=bytes, l=str)
    process_handler = handlers.TFTProcessHandler()

    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertEqual(
          handlers._default_type_to_tensor_type_map[typ], feature_spec.dtype)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  def test_tensorflow_raw_data_metadata_primitive_types_in_containers(self):
    input_types = dict([("x", List[int]), ("y", List[float]),
                        ("k", List[bytes]), ("l", List[str])])
    process_handler = handlers.TFTProcessHandler()
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  def test_tensorflow_raw_data_metadata_primitive_native_container_types(self):
    input_types = dict([("x", list[int]), ("y", list[float]),
                        ("k", list[bytes]), ("l", list[str])])
    process_handler = handlers.TFTProcessHandler()
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  def test_tensorflow_raw_data_metadata_numpy_types(self):
    input_types = dict(x=np.int64, y=np.float32, z=List[np.int64])
    process_handler = handlers.TFTProcessHandler()
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  def test_tensorflow_raw_data_metadata_union_type_in_single_column(self):
    input_types = dict(x=Union[int, float])
    process_handler = handlers.TFTProcessHandler()
    with self.assertRaises(TypeError):
      for col_name, typ in input_types.items():
        _ = process_handler._get_raw_data_feature_spec_per_column(
            typ=typ, col_name=col_name)

  def test_tensorflow_raw_data_metadata_dtypes(self):
    input_types = dict(x=np.int32, y=np.float64)
    expected_dtype = dict(x=np.int64, y=np.float32)
    process_handler = handlers.TFTProcessHandler()
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertEqual(expected_dtype[col_name], feature_spec.dtype)

  @parameterized.expand([
      param(
          input_data=[{
              'x': 1,
              'y': 2.0,
          }],
          input_types={
              'x': int, 'y': float
          },
          expected_dtype={
              'x': typing.Sequence[np.float32],
              'y': typing.Sequence[np.float32]
          }),
      param(
          input_data=[{
              'x': np.array([1], dtype=np.int64),
              'y': np.array([2.0], dtype=np.float32)
          }],
          input_types={
              'x': np.int32, 'y': np.float32
          },
          expected_dtype={
              'x': typing.Sequence[np.float32],
              'y': typing.Sequence[np.float32]
          }),
      param(
          input_data=[{
              'x': [1, 2, 3], 'y': [2.0, 3.0, 4.0]
          }],
          input_types={
              'x': List[int], 'y': List[float]
          },
          expected_dtype={
              'x': typing.Sequence[np.float32],
              'y': typing.Sequence[np.float32]
          }),
      param(
          input_data=[{
              'x': [1, 2, 3], 'y': [2.0, 3.0, 4.0]
          }],
          input_types={
              'x': typing.Sequence[int], 'y': typing.Sequence[float]
          },
          expected_dtype={
              'x': typing.Sequence[np.float32],
              'y': typing.Sequence[np.float32]
          }),
      # this fails on Python 3.8 since tpye subscripting is not supported
      # param(
      #     input_data=[{
      #         'x': [1, 2, 3], 'y': [2.0, 3.0, 4.0]
      #     }],
      #     input_types={
      #         'x': list[int], 'y': list[float]
      #     },
      #     expected_dtype={
      #         'x': typing.Sequence[np.float32],
      #         'y': typing.Sequence[np.float32]
      #     }),
  ])
  def test_tft_process_handler_dict_output_pcoll_schema(
      self, input_data, input_types, expected_dtype):
    transforms = [tft_transforms.ScaleTo01(columns=['x'])]
    with beam.Pipeline() as p:
      schema_data = (
          p
          | beam.Create(input_data)
          | beam.Map(lambda x: beam.Row(**x)).with_output_types(
              beam.row_type.RowTypeConstraint.from_fields(
                  list(input_types.items()))))
      transformed_data = (
          schema_data | base.MLTransform(
              artifact_location=self.artifact_location, transforms=transforms))
    for name, typ in transformed_data.element_type._fields:
      if name in expected_dtype:
        self.assertEqual(expected_dtype[name], typ)

  def test_tft_process_handler_fail_for_non_global_windows_in_produce_mode(
      self):
    transforms = [tft_transforms.ScaleTo01(columns=['x'])]

    with beam.Pipeline() as p:
      with self.assertRaises(RuntimeError):
        _ = (
            p
            | beam.Create([{
                'x': 1, 'y': 2.0
            }])
            | beam.WindowInto(beam.window.FixedWindows(1))
            | base.MLTransform(
                transforms=transforms,
                artifact_location=self.artifact_location,
                artifact_mode=base.ArtifactMode.PRODUCE))

  def test_tft_process_handler_on_batched_dict(self):
    transforms = [tft_transforms.ScaleTo01(columns=['x'])]
    batched_data = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      batched_result = (
          p
          | beam.Create(batched_data)
          | base.MLTransform(
              transforms=transforms, artifact_location=self.artifact_location))
      expected_output = [
          np.array([0, 0.2, 0.4], dtype=np.float32),
          np.array([0.6, 0.8, 1], dtype=np.float32)
      ]
      actual_output = (batched_result | beam.Map(lambda x: x.x))
      assert_that(
          actual_output, equal_to(expected_output, equals_fn=np.array_equal))

  def test_tft_process_handler_on_unbatched_dict(self):
    transforms = [tft_transforms.ScaleTo01(columns=['x'])]
    unbatched_data = [{'x': 1}, {'x': 2}]
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create(unbatched_data)
          | base.MLTransform(
              artifact_location=self.artifact_location, transforms=transforms))
      expected_output = [
          np.array([0.], dtype=np.float32), np.array([1.], dtype=np.float32)
      ]
      actual_output = (result | beam.Map(lambda x: x.x))
      assert_that(
          actual_output, equal_to(expected_output, equals_fn=np.array_equal))

  def test_tft_process_handler_default_transform_types(self):
    transforms = [
        tft_transforms.ScaleTo01(columns=['x']),
        tft_transforms.ScaleToZScore(columns=['y']),
        tft_transforms.Bucketize(columns=['z'], num_buckets=2),
        tft_transforms.ComputeAndApplyVocabulary(columns=['w'])
    ]
    process_handler = handlers.TFTProcessHandler(transforms=transforms)
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
    process_handler = handlers.TFTProcessHandler()
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


if __name__ == '__main__':
  unittest.main()
