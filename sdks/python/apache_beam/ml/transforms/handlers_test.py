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

from typing import NamedTuple
from typing import List
from typing import Union

import unittest

import apache_beam as beam
from apache_beam.ml.transforms import base
from apache_beam.ml.transforms import handlers
from apache_beam.testing.test_pipeline import TestPipeline
import numpy as np
import tensorflow as tf


class _FakeOperation(base._BaseOperation):
  def __init__(self, name, *args, **kwargs):
    self.name = name

  def apply(self, inputs, *args, **kwargs):
    return inputs


class UnBatchedIntType(NamedTuple):
  x: int


class BatchedIntType(NamedTuple):
  x: List[int]


class ProcessHandlerTests(unittest.TestCase):
  def setUp(self) -> None:
    self.pipeline = TestPipeline()

  def test_ml_transform_appends_transforms_to_process_handler_correctly(self):
    fake_fn_1 = _FakeOperation(name='fake_fn_1')
    transforms = [fake_fn_1]
    process_handler = handlers.TFTProcessHandlerDict(transforms=transforms)
    ml_transform = base.MLTransform(process_handler=process_handler)
    ml_transform = ml_transform.with_transform(
        transform=_FakeOperation(name='fake_fn_2'))

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
    process_handler = handlers.TFTProcessHandlerDict()
    inferred_input_type = process_handler.get_input_types(element_type)
    expected_input_type = dict(x=int)

    self.assertEqual(inferred_input_type, expected_input_type)

  def test_input_type_from_schema_named_tuple_pcoll_batched(self):
    batched_data = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      data = (
          p | beam.Create(batched_data)
          | beam.Map(lambda x: BatchedIntType(**x)).with_output_types(
              BatchedIntType))
    element_type = data.element_type
    process_handler = handlers.TFTProcessHandlerDict()
    inferred_input_type = process_handler.get_input_types(element_type)
    expected_input_type = dict(x=List[int])
    self.assertEqual(inferred_input_type, expected_input_type)

  def test_input_type_from_row_type_pcoll_unbatched(self):
    non_batched_data = [{'x': 1}]
    with beam.Pipeline() as p:
      data = (
          p | beam.Create(non_batched_data)
          | beam.Map(lambda ele: beam.Row(x=int(ele['x']))))
    element_type = data.element_type
    process_handler = handlers.TFTProcessHandlerDict()
    inferred_input_type = process_handler.get_input_types(element_type)
    expected_input_type = dict(x=int)
    self.assertEqual(inferred_input_type, expected_input_type)

  def test_input_type_from_row_type_pcoll_batched(self):
    batched_data = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      data = (
          p | beam.Create(batched_data)
          | beam.Map(lambda ele: beam.Row(x=list(ele['x']))).with_output_types(
              beam.row_type.RowTypeConstraint.from_fields([('x', List[int])])))

    element_type = data.element_type
    process_handler = handlers.TFTProcessHandlerDict()
    inferred_input_type = process_handler.get_input_types(element_type)
    expected_input_type = dict(x=List[int])
    self.assertEqual(inferred_input_type, expected_input_type)

  def test_input_type_non_schema_pcoll(self):
    non_batched_data = [{'x': 1}]
    with beam.Pipeline() as p:
      data = (p | beam.Create(non_batched_data))
    element_type = data.element_type
    process_handler = handlers.TFTProcessHandlerDict()
    with self.assertRaises(TypeError):
      _ = process_handler.get_input_types(element_type)

  def test_validate_primitive_input_types(self):
    input_types = dict(x=int, y=float, k=bytes, l=str)
    process_handler = handlers.TFTProcessHandlerDict()
    is_valid = process_handler._validate_input_types(input_types)
    self.assertTrue(is_valid)

  def test_validate_container_primitive_input_types(self):
    input_types = dict([("x", List[int]), ("y", List[float]),
                        ("k", List[bytes]), ("l", List[str])])
    process_handler = handlers.TFTProcessHandlerDict()
    is_valid = process_handler._validate_input_types(input_types)
    self.assertTrue(is_valid)

  def test_validate_numpy_input_types(self):
    input_types = dict(x=np.int32, y=np.float32, k=np.bytes_, l=np.str_)
    process_handler = handlers.TFTProcessHandlerDict()
    is_valid = process_handler._validate_input_types(input_types)
    self.assertTrue(is_valid)

  def test_tensorflow_raw_data_metadata_primitive_types(self):
    input_types = dict(x=int, y=float, k=bytes, l=str)
    process_handler = handlers.TFTProcessHandlerDict()

    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertEqual(
          handlers._default_type_to_tensor_type_map[typ], feature_spec.dtype)
      self.assertIsInstance(feature_spec, tf.io.FixedLenFeature)

  def test_tensorflow_raw_data_metadata_primitive_types_in_containers(self):
    input_types = dict([("x", List[int]), ("y", List[float]),
                        ("k", List[bytes]), ("l", List[str])])
    process_handler = handlers.TFTProcessHandlerDict()
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  def test_tensorflow_raw_data_metadata_primitive_native_container_types(self):
    input_types = dict([("x", list[int]), ("y", list[float]),
                        ("k", list[bytes]), ("l", list[str])])
    process_handler = handlers.TFTProcessHandlerDict()
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  def test_tensorflow_raw_data_metadata_numpy_types(self):
    input_types = dict(x=np.int64, y=np.float32, z=List[np.int64])
    process_handler = handlers.TFTProcessHandlerDict()
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertIsInstance(feature_spec, tf.io.VarLenFeature)

  def test_tensorflow_raw_data_metadata_union_type_in_single_column(self):
    input_types = dict(x=Union[int, float])
    process_handler = handlers.TFTProcessHandlerDict()
    with self.assertRaises(TypeError):
      for col_name, typ in input_types.items():
        _ = process_handler._get_raw_data_feature_spec_per_column(
            typ=typ, col_name=col_name)

  def test_tensorflow_raw_data_metadata_dtypes(self):
    input_types = dict(x=np.int32, y=np.float64)
    expected_dtype = dict(x=np.int64, y=np.float32)
    process_handler = handlers.TFTProcessHandlerDict()
    for col_name, typ in input_types.items():
      feature_spec = process_handler._get_raw_data_feature_spec_per_column(
          typ=typ, col_name=col_name)
      self.assertEqual(expected_dtype[col_name], feature_spec.dtype)


if __name__ == '__main__':
  unittest.main()
