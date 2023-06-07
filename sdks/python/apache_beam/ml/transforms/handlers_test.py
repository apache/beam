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

import typing
from typing import NamedTuple
from typing import List
from typing import Union

import unittest
import numpy as np
from parameterized import param
from parameterized import parameterized

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=wrong-import-position, ungrouped-imports
try:
  from apache_beam.ml.transforms import base
  from apache_beam.ml.transforms import handlers
  from apache_beam.ml.transforms import tft_transforms
  from apache_beam.ml.transforms.tft_transforms import TFTOperation
  import tensorflow as tf
except ImportError:
  tft_transforms = None

if not tft_transforms:
  raise unittest.SkipTest('tensorflow_transform is not installed.')


class _FakeOperation(TFTOperation):
  def __init__(self, name, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.name = name

  def apply(self, inputs, *args, **kwargs):
    return inputs


class _AddOperation(TFTOperation):
  def apply(self, inputs, *args, **kwargs):
    return inputs + 1


class _MultiplyOperation(TFTOperation):
  def apply(self, inputs, *args, **kwargs):
    return inputs * 10


class _FakeOperationWithArtifacts(TFTOperation):
  def apply(self, inputs, *args, **kwargs):
    return inputs

  def get_artifacts(self, data, col_name):
    return {'artifact': 1}


class UnBatchedIntType(NamedTuple):
  x: int


class BatchedIntType(NamedTuple):
  x: List[int]


class BatchedNumpyType(NamedTuple):
  x: np.int64


class TFTProcessHandlerSchemaTest(unittest.TestCase):
  def setUp(self) -> None:
    self.pipeline = TestPipeline()

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
    process_handler = handlers.TFTProcessHandlerSchema(
        transforms=[add_fn, mul_fn])

    actual_result = process_handler.preprocessing_fn(inputs)
    self.assertDictEqual(actual_result, expected_result)

  def test_tft_operation_save_intermediate_result(self):
    fake_fn_1 = _FakeOperation(
        columns=['x'],
        name='fake_fn_1',
        save_result=True,
        output_name='x_fake_fn_1')
    fake_fn_2 = _FakeOperation(columns=['x'], name='fake_fn_2')
    process_handler = handlers.TFTProcessHandlerSchema(
        transforms=[fake_fn_1, fake_fn_2])

    inputs = {'x': [1, 2, 3]}
    actual_result = process_handler.preprocessing_fn(inputs)
    expected_result = {'x': [1, 2, 3], 'x_fake_fn_1': [1, 2, 3]}
    self.assertDictEqual(actual_result, expected_result)

  def test_preprocessing_fn_with_artifacts(self):
    process_handler = handlers.TFTProcessHandlerSchema(
        transforms=[_FakeOperationWithArtifacts(columns=['x'])])
    inputs = {'x': [1, 2, 3]}
    actual_result = process_handler.preprocessing_fn(inputs)
    expected_result = {'x': [1, 2, 3], 'artifact': 1}
    self.assertDictEqual(actual_result, expected_result)

  def test_ml_transform_appends_transforms_to_process_handler_correctly(self):
    fake_fn_1 = _FakeOperation(name='fake_fn_1', columns=['x'])
    transforms = [fake_fn_1]
    process_handler = handlers.TFTProcessHandlerSchema(transforms=transforms)
    ml_transform = base.MLTransform(process_handler=process_handler)
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
    process_handler = handlers.TFTProcessHandlerSchema()
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
    process_handler = handlers.TFTProcessHandlerSchema()
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
    process_handler = handlers.TFTProcessHandlerSchema()
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
    process_handler = handlers.TFTProcessHandlerSchema()
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
      process_handler = handlers.TFTProcessHandlerSchema()
      inferred_input_type = process_handler._map_column_names_to_types(
          element_type)
      expected_type = dict(x=np.int64)
      self.assertEqual(inferred_input_type, expected_type)

  def test_input_type_non_schema_pcoll(self):
    non_batched_data = [{'x': 1}]
    with beam.Pipeline() as p:
      data = (p | beam.Create(non_batched_data))
    element_type = data.element_type
    process_handler = handlers.TFTProcessHandlerSchema()
    with self.assertRaises(TypeError):
      _ = process_handler._map_column_names_to_types(element_type)

  def test_tensorflow_raw_data_metadata_primitive_types(self):
    input_types = dict(x=int, y=float, k=bytes, l=str)
    process_handler = handlers.TFTProcessHandlerSchema()

    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertEqual(
          handlers._default_type_to_tensor_type_map[typ], feature_spec.dtype)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  def test_tensorflow_raw_data_metadata_primitive_types_in_containers(self):
    input_types = dict([("x", List[int]), ("y", List[float]),
                        ("k", List[bytes]), ("l", List[str])])
    process_handler = handlers.TFTProcessHandlerSchema()
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  def test_tensorflow_raw_data_metadata_primitive_native_container_types(self):
    input_types = dict([("x", list[int]), ("y", list[float]),
                        ("k", list[bytes]), ("l", list[str])])
    process_handler = handlers.TFTProcessHandlerSchema()
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  def test_tensorflow_raw_data_metadata_numpy_types(self):
    input_types = dict(x=np.int64, y=np.float32, z=List[np.int64])
    process_handler = handlers.TFTProcessHandlerSchema()
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  def test_tensorflow_raw_data_metadata_union_type_in_single_column(self):
    input_types = dict(x=Union[int, float])
    process_handler = handlers.TFTProcessHandlerSchema()
    with self.assertRaises(TypeError):
      for col_name, typ in input_types.items():
        _ = process_handler._get_raw_data_feature_spec_per_column(
            typ=typ, col_name=col_name)

  def test_tensorflow_raw_data_metadata_dtypes(self):
    input_types = dict(x=np.int32, y=np.float64)
    expected_dtype = dict(x=np.int64, y=np.float32)
    process_handler = handlers.TFTProcessHandlerSchema()
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
    transforms = [
        tft_transforms.Scale_To_0_1(
            columns=['x'], save_result=True, output_name='x_scaled')
    ]
    process_handler = handlers.TFTProcessHandlerSchema(transforms=transforms)
    with beam.Pipeline() as p:
      schema_data = (
          p
          | beam.Create(input_data)
          | beam.Map(lambda x: beam.Row(**x)).with_output_types(
              beam.row_type.RowTypeConstraint.from_fields(
                  list(input_types.items()))))
      transformed_data = (
          schema_data | base.MLTransform(process_handler=process_handler))
    for name, typ in transformed_data.element_type._fields:
      if name in expected_dtype:
        self.assertEqual(expected_dtype[name], typ)

  def test_tft_process_handler_dict_fail_for_non_schema_pcoll(self):
    transforms = [tft_transforms.Scale_To_0_1(columns=['x'])]
    process_handler = handlers.TFTProcessHandlerSchema(transforms=transforms)
    with beam.Pipeline() as p:
      with self.assertRaises(TypeError):
        _ = (
            p
            | beam.Create([{
                'x': 1, 'y': 2.0
            }])
            | base.MLTransform(process_handler=process_handler))

  def test_tft_process_handler_fail_for_non_global_windows(self):
    transforms = [tft_transforms.Scale_To_0_1(columns=['x'])]
    process_handler = handlers.TFTProcessHandlerSchema(transforms=transforms)
    with beam.Pipeline() as p:
      with self.assertRaises(RuntimeError):
        _ = (
            p
            | beam.Create([{
                'x': 1, 'y': 2.0
            }])
            | beam.WindowInto(beam.window.FixedWindows(1))
            | base.MLTransform(process_handler=process_handler))


if __name__ == '__main__':
  unittest.main()
