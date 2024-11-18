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
import secrets
import shutil
import tempfile
import time
import typing
import unittest
from collections.abc import Sequence
from typing import Any
from typing import Optional

import numpy as np
from parameterized import param
from parameterized import parameterized

import apache_beam as beam
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.transforms import base
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from apache_beam.ml.transforms import tft
  from apache_beam.ml.transforms.handlers import TFTProcessHandler
  from apache_beam.ml.transforms.tft import TFTOperation
except ImportError:
  tft = None  # type: ignore

try:
  import PIL
  from PIL.Image import Image as PIL_Image
except ImportError:
  PIL = None
  PIL_Image = Any

try:

  class _FakeOperation(TFTOperation):
    def __init__(self, name, *args, **kwargs):
      super().__init__(*args, **kwargs)
      self.name = name

    def apply_transform(self, inputs, output_column_name, **kwargs):
      return {output_column_name: inputs}
except:  # pylint: disable=bare-except
  pass

try:
  from apache_beam.runners.dataflow.internal import apiclient
except ImportError:
  apiclient = None  # type: ignore


class BaseMLTransformTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  @unittest.skipIf(tft is None, 'tft module is not installed.')
  def test_ml_transform_appends_transforms_to_process_handler_correctly(self):
    fake_fn_1 = _FakeOperation(name='fake_fn_1', columns=['x'])
    transforms = [fake_fn_1]
    ml_transform = base.MLTransform(
        transforms=transforms, write_artifact_location=self.artifact_location)
    ml_transform = ml_transform.with_transform(
        transform=_FakeOperation(name='fake_fn_2', columns=['x']))

    self.assertEqual(len(ml_transform.transforms), 2)
    self.assertEqual(ml_transform.transforms[0].name, 'fake_fn_1')
    self.assertEqual(ml_transform.transforms[1].name, 'fake_fn_2')

  @unittest.skipIf(tft is None, 'tft module is not installed.')
  def test_ml_transform_on_dict(self):
    transforms = [tft.ScaleTo01(columns=['x'])]
    data = [{'x': 1}, {'x': 2}]
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create(data)
          | base.MLTransform(
              write_artifact_location=self.artifact_location,
              transforms=transforms))
      expected_output = [
          np.array([0.0], dtype=np.float32),
          np.array([1.0], dtype=np.float32),
      ]
      actual_output = result | beam.Map(lambda x: x.x)
      assert_that(
          actual_output, equal_to(expected_output, equals_fn=np.array_equal))

  @unittest.skipIf(tft is None, 'tft module is not installed.')
  def test_ml_transform_on_list_dict(self):
    transforms = [tft.ScaleTo01(columns=['x'])]
    data = [{'x': [1, 2, 3]}, {'x': [4, 5, 6]}]
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create(data)
          | base.MLTransform(
              transforms=transforms,
              write_artifact_location=self.artifact_location))
      expected_output = [
          np.array([0, 0.2, 0.4], dtype=np.float32),
          np.array([0.6, 0.8, 1], dtype=np.float32),
      ]
      actual_output = result | beam.Map(lambda x: x.x)
      assert_that(
          actual_output, equal_to(expected_output, equals_fn=np.array_equal))

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
              'y': typing.Sequence[np.float32],
          },
      ),
      param(
          input_data=[{
              'x': np.array([1], dtype=np.int64),
              'y': np.array([2.0], dtype=np.float32),
          }],
          input_types={
              'x': np.int32, 'y': np.float32
          },
          expected_dtype={
              'x': typing.Sequence[np.float32],
              'y': typing.Sequence[np.float32],
          },
      ),
      param(
          input_data=[{
              'x': [1, 2, 3], 'y': [2.0, 3.0, 4.0]
          }],
          input_types={
              'x': list[int], 'y': list[float]
          },
          expected_dtype={
              'x': typing.Sequence[np.float32],
              'y': typing.Sequence[np.float32],
          },
      ),
      param(
          input_data=[{
              'x': [1, 2, 3], 'y': [2.0, 3.0, 4.0]
          }],
          input_types={
              'x': typing.Sequence[int],
              'y': typing.Sequence[float],
          },
          expected_dtype={
              'x': typing.Sequence[np.float32],
              'y': typing.Sequence[np.float32],
          },
      ),
  ])
  @unittest.skipIf(tft is None, 'tft module is not installed.')
  def test_ml_transform_dict_output_pcoll_schema(
      self, input_data, input_types, expected_dtype):
    transforms = [tft.ScaleTo01(columns=['x'])]
    with beam.Pipeline() as p:
      schema_data = (
          p
          | beam.Create(input_data)
          | beam.Map(lambda x: beam.Row(**x)).with_output_types(
              beam.row_type.RowTypeConstraint.from_fields(
                  list(input_types.items()))))
      transformed_data = schema_data | base.MLTransform(
          write_artifact_location=self.artifact_location, transforms=transforms)
      for name, typ in transformed_data.element_type._fields:
        if name in expected_dtype:
          self.assertEqual(expected_dtype[name], typ)

  @unittest.skipIf(tft is None, 'tft module is not installed.')
  def test_ml_transform_fail_for_non_global_windows_in_produce_mode(self):
    transforms = [tft.ScaleTo01(columns=['x'])]
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
                write_artifact_location=self.artifact_location,
            ))

  @unittest.skipIf(tft is None, 'tft module is not installed.')
  def test_ml_transform_on_multiple_columns_single_transform(self):
    transforms = [tft.ScaleTo01(columns=['x', 'y'])]
    data = [{'x': [1, 2, 3], 'y': [1.0, 10.0, 20.0]}]
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create(data)
          | base.MLTransform(
              transforms=transforms,
              write_artifact_location=self.artifact_location))
      expected_output_x = [
          np.array([0, 0.5, 1], dtype=np.float32),
      ]
      expected_output_y = [np.array([0, 0.47368422, 1], dtype=np.float32)]
      actual_output_x = result | beam.Map(lambda x: x.x)
      actual_output_y = result | beam.Map(lambda x: x.y)
      assert_that(
          actual_output_x,
          equal_to(expected_output_x, equals_fn=np.array_equal))
      assert_that(
          actual_output_y,
          equal_to(expected_output_y, equals_fn=np.array_equal),
          label='y')

  @unittest.skipIf(tft is None, 'tft module is not installed.')
  def test_ml_transforms_on_multiple_columns_multiple_transforms(self):
    transforms = [
        tft.ScaleTo01(columns=['x']),
        tft.ComputeAndApplyVocabulary(columns=['y'])
    ]
    data = [{'x': [1, 2, 3], 'y': ['a', 'b', 'c']}]
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create(data)
          | base.MLTransform(
              transforms=transforms,
              write_artifact_location=self.artifact_location))
      expected_output_x = [
          np.array([0, 0.5, 1], dtype=np.float32),
      ]
      expected_output_y = [np.array([2, 1, 0])]
      actual_output_x = result | beam.Map(lambda x: x.x)
      actual_output_y = result | beam.Map(lambda x: x.y)

      assert_that(
          actual_output_x,
          equal_to(expected_output_x, equals_fn=np.array_equal))
      assert_that(
          actual_output_y,
          equal_to(expected_output_y, equals_fn=np.array_equal),
          label='actual_output_y')

  @unittest.skipIf(tft is None, 'tft module is not installed.')
  def test_mltransform_with_counter(self):
    transforms = [
        tft.ComputeAndApplyVocabulary(columns=['y']),
        tft.ScaleTo01(columns=['x'])
    ]
    data = [{'x': [1, 2, 3], 'y': ['a', 'b', 'c']}]
    with beam.Pipeline() as p:
      _ = (
          p | beam.Create(data)
          | base.MLTransform(
              transforms=transforms,
              write_artifact_location=self.artifact_location))
    scale_to_01_counter = MetricsFilter().with_name('BeamML_ScaleTo01')
    vocab_counter = MetricsFilter().with_name(
        'BeamML_ComputeAndApplyVocabulary')
    mltransform_counter = MetricsFilter().with_name('BeamML_MLTransform')
    result = p.result
    self.assertEqual(
        result.metrics().query(scale_to_01_counter)['counters'][0].result, 1)
    self.assertEqual(
        result.metrics().query(vocab_counter)['counters'][0].result, 1)
    self.assertEqual(
        result.metrics().query(mltransform_counter)['counters'][0].result, 1)

  def test_non_ptransfrom_provider_class_to_mltransform(self):
    class Add:
      def __call__(self, x):
        return x + 1

    with self.assertRaisesRegex(TypeError, 'transform must be a subclass of'):
      with beam.Pipeline() as p:
        _ = (
            p
            | beam.Create([{
                'x': 1
            }])
            | base.MLTransform(
                write_artifact_location=self.artifact_location).with_transform(
                    Add()))

  def test_read_mode_with_transforms(self):
    with self.assertRaises(ValueError):
      _ = base.MLTransform(
          # fake callable
          transforms=[lambda x: x],
          read_artifact_location=self.artifact_location)


class FakeModel:
  def __call__(self, example: list[str]) -> list[str]:
    for i in range(len(example)):
      if not isinstance(example[i], str):
        raise TypeError('Input must be a string')
      example[i] = example[i][::-1]
    return example


class FakeModelHandler(ModelHandler):
  def run_inference(
      self,
      batch: Sequence[str],
      model: Any,
      inference_args: Optional[dict[str, Any]] = None):
    return model(batch)

  def load_model(self):
    return FakeModel()


class FakeEmbeddingsManager(base.EmbeddingsManager):
  def __init__(self, columns, **kwargs):
    super().__init__(columns=columns, **kwargs)

  def get_model_handler(self) -> ModelHandler:
    FakeModelHandler.__repr__ = lambda x: 'FakeEmbeddingsManager'  # type: ignore[method-assign]
    return FakeModelHandler()

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    return (RunInference(model_handler=base._TextEmbeddingHandler(self)))

  def __repr__(self):
    return 'FakeEmbeddingsManager'


class TextEmbeddingHandlerTest(unittest.TestCase):
  def setUp(self) -> None:
    self.embedding_conig = FakeEmbeddingsManager(columns=['x'])
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self) -> None:
    shutil.rmtree(self.artifact_location)

  def test_handler_with_incompatible_datatype(self):
    text_handler = base._TextEmbeddingHandler(
        embeddings_manager=self.embedding_conig)
    data = [
        ('x', 1),
        ('x', 2),
        ('x', 3),
    ]
    with self.assertRaises(TypeError):
      text_handler.run_inference(data, None, None)

  def test_handler_with_dict_inputs(self):
    data = [
        {
            'x': "Hello world"
        },
        {
            'x': "Apache Beam"
        },
    ]
    expected_data = [{key: value[::-1]
                      for key, value in d.items()} for d in data]
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create(data)
          | base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  self.embedding_conig))
      assert_that(
          result,
          equal_to(expected_data),
      )

  def test_handler_with_batch_sizes(self):
    self.embedding_conig.max_batch_size = 100
    self.embedding_conig.min_batch_size = 10
    data = [
        {
            'x': "Hello world"
        },
        {
            'x': "Apache Beam"
        },
    ] * 100
    expected_data = [{key: value[::-1]
                      for key, value in d.items()} for d in data]
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create(data)
          | base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  self.embedding_conig))
      assert_that(
          result,
          equal_to(expected_data),
      )

  def test_handler_on_multiple_columns(self):
    data = [
        {
            'x': "Hello world", 'y': "Apache Beam", 'z': 'unchanged'
        },
        {
            'x': "Apache Beam", 'y': "Hello world", 'z': 'unchanged'
        },
    ]
    self.embedding_conig.columns = ['x', 'y']
    expected_data = [{
        key: (value[::-1] if key in self.embedding_conig.columns else value)
        for key,
        value in d.items()
    } for d in data]
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create(data)
          | base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  self.embedding_conig))
      assert_that(
          result,
          equal_to(expected_data),
      )

  def test_handler_on_columns_not_exist_in_input_data(self):
    data = [
        {
            'x': "Hello world", 'y': "Apache Beam"
        },
        {
            'x': "Apache Beam", 'y': "Hello world"
        },
    ]
    self.embedding_conig.columns = ['x', 'y', 'a']

    with self.assertRaises(RuntimeError):
      with beam.Pipeline() as p:
        _ = (
            p
            | beam.Create(data)
            | base.MLTransform(
                write_artifact_location=self.artifact_location).with_transform(
                    self.embedding_conig))

  def test_handler_with_list_data(self):
    data = [{
        'x': ['Hello world', 'Apache Beam'],
    }, {
        'x': ['Apache Beam', 'Hello world'],
    }]
    with self.assertRaises(TypeError):
      with beam.Pipeline() as p:
        _ = (
            p
            | beam.Create(data)
            | base.MLTransform(
                write_artifact_location=self.artifact_location).with_transform(
                    self.embedding_conig))

  def test_handler_with_inconsistent_keys(self):
    data = [
        {
            'x': 'foo', 'y': 'bar', 'z': 'baz'
        },
        {
            'x': 'foo2', 'y': 'bar2'
        },
        {
            'x': 'foo3', 'y': 'bar3', 'z': 'baz3'
        },
    ]
    self.embedding_conig.min_batch_size = 2
    with self.assertRaises(RuntimeError):
      with beam.Pipeline() as p:
        _ = (
            p
            | beam.Create(data)
            | base.MLTransform(
                write_artifact_location=self.artifact_location).with_transform(
                    self.embedding_conig))


class FakeImageModel:
  def __call__(self, example: list[PIL_Image]) -> list[PIL_Image]:
    for i in range(len(example)):
      if not isinstance(example[i], PIL_Image):
        raise TypeError('Input must be an Image')
    return example


class FakeImageModelHandler(ModelHandler):
  def run_inference(
      self,
      batch: Sequence[PIL_Image],
      model: Any,
      inference_args: Optional[dict[str, Any]] = None):
    return model(batch)

  def load_model(self):
    return FakeImageModel()


class FakeImageEmbeddingsManager(base.EmbeddingsManager):
  def __init__(self, columns, **kwargs):
    super().__init__(columns=columns, **kwargs)

  def get_model_handler(self) -> ModelHandler:
    FakeModelHandler.__repr__ = lambda x: 'FakeImageEmbeddingsManager'  # type: ignore[method-assign]
    return FakeImageModelHandler()

  def get_ptransform_for_processing(self, **kwargs) -> beam.PTransform:
    return (RunInference(model_handler=base._ImageEmbeddingHandler(self)))

  def __repr__(self):
    return 'FakeImageEmbeddingsManager'


class TestImageEmbeddingHandler(unittest.TestCase):
  def setUp(self) -> None:
    self.embedding_config = FakeImageEmbeddingsManager(columns=['x'])
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self) -> None:
    shutil.rmtree(self.artifact_location)

  @unittest.skipIf(PIL is None, 'PIL module is not installed.')
  def test_handler_with_incompatible_datatype(self):
    image_handler = base._ImageEmbeddingHandler(
        embeddings_manager=self.embedding_config)
    data = [
        ('x', 'hi there'),
        ('x', 'not an image'),
        ('x', 'image_path.jpg'),
    ]
    with self.assertRaises(TypeError):
      image_handler.run_inference(data, None, None)

  @unittest.skipIf(PIL is None, 'PIL module is not installed.')
  def test_handler_with_dict_inputs(self):
    img_one = PIL.Image.new(mode='RGB', size=(1, 1))
    img_two = PIL.Image.new(mode='RGB', size=(1, 1))
    data = [
        {
            'x': img_one
        },
        {
            'x': img_two
        },
    ]
    expected_data = [{key: value for key, value in d.items()} for d in data]
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create(data)
          | base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  self.embedding_config))
      assert_that(
          result,
          equal_to(expected_data),
      )


class TestUtilFunctions(unittest.TestCase):
  def test_list_of_dicts_to_dict_of_lists_normal(self):
    input_list = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]
    expected_output = {'a': [1, 3], 'b': [2, 4]}
    self.assertEqual(
        base._convert_list_of_dicts_to_dict_of_lists(input_list),
        expected_output)

  def test_list_of_dicts_to_dict_of_lists_on_list_inputs(self):
    input_list = [{'a': [1, 2, 10], 'b': 3}, {'a': [1], 'b': 5}]
    expected_output = {'a': [[1, 2, 10], [1]], 'b': [3, 5]}
    self.assertEqual(
        base._convert_list_of_dicts_to_dict_of_lists(input_list),
        expected_output)

  def test_dict_of_lists_to_lists_of_dict_normal(self):
    input_dict = {'a': [1, 3], 'b': [2, 4]}
    expected_output = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]
    self.assertEqual(
        base._convert_dict_of_lists_to_lists_of_dict(input_dict),
        expected_output)

  def test_dict_of_lists_to_lists_of_dict_unequal_length(self):
    input_dict = {'a': [1, 3], 'b': [2]}
    with self.assertRaises(AssertionError):
      base._convert_dict_of_lists_to_lists_of_dict(input_dict)


class TestJsonPickleTransformAttributeManager(unittest.TestCase):
  def setUp(self):
    self.attribute_manager = base._transform_attribute_manager
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  @unittest.skipIf(tft is None, 'tft module is not installed.')
  def test_save_tft_process_handler(self):
    transforms = [
        tft.ScaleTo01(columns=['x']),
        tft.ComputeAndApplyVocabulary(columns=['y'])
    ]
    process_handler = TFTProcessHandler(
        transforms=transforms,
        artifact_location=self.artifact_location,
    )
    self.attribute_manager.save_attributes(
        ptransform_list=[process_handler],
        artifact_location=self.artifact_location,
    )

    files = os.listdir(self.artifact_location)
    self.assertTrue(len(files) == 1)
    self.assertTrue(files[0] == base._ATTRIBUTE_FILE_NAME)

  def test_save_run_inference(self):
    self.attribute_manager.save_attributes(
        ptransform_list=[RunInference(model_handler=FakeModelHandler())],
        artifact_location=self.artifact_location,
    )
    files = os.listdir(self.artifact_location)
    self.assertTrue(len(files) == 1)
    self.assertTrue(files[0] == base._ATTRIBUTE_FILE_NAME)

  def test_save_and_load_run_inference(self):
    ptransform_list = [RunInference(model_handler=FakeModelHandler())]
    self.attribute_manager.save_attributes(
        ptransform_list=ptransform_list,
        artifact_location=self.artifact_location,
    )
    loaded_ptransform_list = self.attribute_manager.load_attributes(
        artifact_location=self.artifact_location,
    )

    self.assertTrue(len(loaded_ptransform_list) == len(ptransform_list))
    self.assertListEqual(
        list(loaded_ptransform_list[0].__dict__.keys()),
        list(ptransform_list[0].__dict__.keys()))

    get_keys = lambda x: list(x.__dict__.keys())
    for i, transform in enumerate(ptransform_list):
      self.assertListEqual(
          get_keys(transform), get_keys(loaded_ptransform_list[i]))
      if hasattr(transform, 'model_handler'):
        model_handler = transform.model_handler
        loaded_model_handler = loaded_ptransform_list[i].model_handler
        self.assertListEqual(
            get_keys(model_handler), get_keys(loaded_model_handler))

  def test_mltransform_to_ptransform_wrapper(self):
    transforms = [
        FakeEmbeddingsManager(columns=['x']),
        FakeEmbeddingsManager(columns=['y', 'z']),
    ]
    ptransform_mapper = base._MLTransformToPTransformMapper(
        transforms=transforms,
        artifact_location=self.artifact_location,
        artifact_mode=None)

    ptransform_list = ptransform_mapper.create_ptransform_list()
    self.assertTrue(len(ptransform_list) == 2)

    self.assertEqual(type(ptransform_list[0]), RunInference)
    expected_columns = [['x'], ['y', 'z']]
    for i in range(len(ptransform_list)):
      self.assertEqual(type(ptransform_list[i]), RunInference)
      self.assertEqual(
          type(ptransform_list[i]._model_handler), base._TextEmbeddingHandler)
      self.assertEqual(
          ptransform_list[i]._model_handler.columns, expected_columns[i])

  @unittest.skipIf(apiclient is None, 'apache_beam[gcp] is not installed.')
  def test_with_gcs_location_with_none_options(self):
    path = f"gs://fake_path_{secrets.token_hex(3)}_{int(time.time())}"
    with self.assertRaises(RuntimeError):
      self.attribute_manager.save_attributes(
          ptransform_list=[], artifact_location=path, options=None)
    with self.assertRaises(RuntimeError):
      self.attribute_manager.save_attributes(
          ptransform_list=[], artifact_location=path)

  def test_with_same_local_artifact_location(self):
    artifact_location = self.artifact_location
    attribute_manager = base._JsonPickleTransformAttributeManager()

    ptransform_list = [RunInference(model_handler=FakeModelHandler())]

    attribute_manager.save_attributes(
        ptransform_list, artifact_location=artifact_location)

    with self.assertRaises(FileExistsError):
      attribute_manager.save_attributes([lambda x: x],
                                        artifact_location=artifact_location)


class MLTransformDLQTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_dlq_with_embeddings(self):
    with beam.Pipeline() as p:
      good, bad = (
          p
          | beam.Create([{
              'x': 1
          },
          {
            'x': 3,
          },
          {
            'x': 'Hello'
          }
          ],
          )
          | base.MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  FakeEmbeddingsManager(
                    columns=['x'])).with_exception_handling())

      good_expected_elements = [{'x': 'olleH'}]

      assert_that(
          good,
          equal_to(good_expected_elements),
          label='good',
      )

      # batching happens in RunInference hence elements
      # are in lists in the bad pcoll.
      bad_expected_elements = [[{'x': 1}], [{'x': 3}]]

      assert_that(
          bad | beam.Map(lambda x: x.element),
          equal_to(bad_expected_elements),
          label='bad',
      )

  def test_mltransform_with_dlq_and_extract_tranform_name(self):
    with beam.Pipeline() as p:
      good, bad = (
        p
        | beam.Create([{
            'x': 1
        },
        {
          'x': 3,
        },
        {
          'x': 'Hello'
        }
        ],
        )
        | base.MLTransform(
            write_artifact_location=self.artifact_location).with_transform(
                FakeEmbeddingsManager(
                  columns=['x'])).with_exception_handling())

      good_expected_elements = [{'x': 'olleH'}]
      assert_that(
          good,
          equal_to(good_expected_elements),
          label='good',
      )

      bad_expected_transform_name = [
          'FakeEmbeddingsManager', 'FakeEmbeddingsManager'
      ]

      assert_that(
          bad | beam.Map(lambda x: x.transform_name),
          equal_to(bad_expected_transform_name),
      )


if __name__ == '__main__':
  unittest.main()
