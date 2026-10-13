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

import contextlib
import importlib.util
import os
import shutil
import sys
import tempfile
import types
import unittest
import uuid

import jsonpickle
import numpy as np
import pytest
from parameterized import parameterized

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.transforms import base
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.transforms.embeddings import huggingface
from apache_beam.testing import test_utils
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=ungrouped-imports
try:
  import torch
  from PIL import Image

  from apache_beam.ml.transforms.embeddings.huggingface import InferenceAPIEmbeddings
  from apache_beam.ml.transforms.embeddings.huggingface import SentenceTransformerEmbeddings
except ImportError:
  SentenceTransformerEmbeddings = None  # type: ignore

# pylint: disable=ungrouped-imports
try:
  import tensorflow_transform as tft

  from apache_beam.ml.transforms.tft import ScaleTo01
except ImportError:
  tft = None

# pylint: disable=ungrouped-imports
try:
  from PIL import Image
except ImportError:
  Image = None  # type: ignore[assignment]

_HF_TOKEN = os.environ.get('HF_INFERENCE_TOKEN')
test_query = "This is a test"
test_query_column = "feature_1"
DEFAULT_MODEL_NAME = "sentence-transformers/all-mpnet-base-v2"
IMAGE_MODEL_NAME = "clip-ViT-B-32"
_parameterized_inputs = [
    ([{
        test_query_column: 'That is a happy person'
    }, {
        test_query_column: 'That is a very happy person'
    }],
     'thenlper/gte-base', [0.11, 0.11]),
    ([{
        test_query_column: test_query,
    }], DEFAULT_MODEL_NAME, [0.13]),
    (
        [{
            test_query_column: 'query: how much protein should a female eat',
        },
         {
             test_query_column: (
                 "passage: As a general guideline, the CDC's "
                 "average requirement of protein for women "
                 "ages 19 to 70 is 46 grams per day. But, "
                 "as you can see from this chart, you'll need "
                 "to increase that if you're expecting or training"
                 " for a marathon. Check out the chart below "
                 "to see how much protein "
                 "you should be eating each day.")
         }],
        'intfloat/e5-base-v2',
        # this model requires inputs to be specified as query: and passage:
        [0.1, 0.1]),
]


@pytest.mark.no_xdist
@unittest.skipIf(
    SentenceTransformerEmbeddings is None,
    'sentence-transformers is not installed.')
class SentenceTransformerEmbeddingsTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp(prefix='sentence_transformers_')
    # this bucket has TTL and will be deleted periodically
    self.gcs_artifact_location = os.path.join(
        'gs://temp-storage-for-perf-tests/sentence_transformers',
        uuid.uuid4().hex)

  def tearDown(self) -> None:
    shutil.rmtree(self.artifact_location)

  def test_sentence_transformer_embeddings(self):
    model_name = DEFAULT_MODEL_NAME
    embedding_config = SentenceTransformerEmbeddings(
        model_name=model_name, columns=[test_query_column])
    with beam.Pipeline() as pipeline:
      result_pcoll = (
          pipeline
          | "CreateData" >> beam.Create([{
              test_query_column: test_query
          }])
          | "MLTransform" >> MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  embedding_config))

      def assert_element(element):
        assert len(element[test_query_column]) == 768

      _ = (result_pcoll | beam.Map(assert_element))

  @unittest.skipIf(tft is None, 'Tensorflow Transform is not installed.')
  def test_embeddings_with_scale_to_0_1(self):
    model_name = DEFAULT_MODEL_NAME
    embedding_config = SentenceTransformerEmbeddings(
        model_name=model_name,
        columns=[test_query_column],
    )
    with beam.Pipeline() as pipeline:
      transformed_pcoll = (
          pipeline
          | "CreateData" >> beam.Create([{
              test_query_column: test_query
          }])
          | "MLTransform" >> MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  embedding_config).with_transform(
                      ScaleTo01(columns=[test_query_column])))

      def assert_element(element):
        assert max(element.feature_1) == 1

      _ = (transformed_pcoll | beam.Map(assert_element))

  @parameterized.expand(_parameterized_inputs)
  def test_embeddings_with_read_artifact_location(
      self, inputs, model_name, output):
    embedding_config = SentenceTransformerEmbeddings(
        model_name=model_name, columns=[test_query_column])

    with beam.Pipeline() as p:
      result_pcoll = (
          p
          | "CreateData" >> beam.Create(inputs)
          | "MLTransform" >> MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  embedding_config))
      max_ele_pcoll = (
          result_pcoll
          | beam.Map(lambda x: round(max(x[test_query_column]), 2)))

      assert_that(max_ele_pcoll, equal_to(output))

    with beam.Pipeline() as p:
      result_pcoll = (
          p
          | "CreateData" >> beam.Create(inputs)
          | "MLTransform" >>
          MLTransform(read_artifact_location=self.artifact_location))
      max_ele_pcoll = (
          result_pcoll
          | beam.Map(lambda x: round(max(x[test_query_column]), 2)))

      assert_that(max_ele_pcoll, equal_to(output))

  def test_sentence_transformer_with_int_data_types(self):
    model_name = DEFAULT_MODEL_NAME
    embedding_config = SentenceTransformerEmbeddings(
        model_name=model_name, columns=[test_query_column])
    with self.assertRaisesRegex(Exception, "Embeddings can only be generated"):
      with beam.Pipeline() as pipeline:
        _ = (
            pipeline
            | "CreateData" >> beam.Create([{
                test_query_column: 1
            }])
            | "MLTransform" >> MLTransform(
                write_artifact_location=self.artifact_location).with_transform(
                    embedding_config))

  @parameterized.expand(_parameterized_inputs)
  def test_with_gcs_artifact_location(self, inputs, model_name, output):
    embedding_config = SentenceTransformerEmbeddings(
        model_name=model_name, columns=[test_query_column])

    with beam.Pipeline() as p:
      result_pcoll = (
          p
          | "CreateData" >> beam.Create(inputs)
          | "MLTransform" >>
          MLTransform(write_artifact_location=self.gcs_artifact_location
                      ).with_transform(embedding_config))
      max_ele_pcoll = (
          result_pcoll
          | beam.Map(lambda x: round(np.max(x[test_query_column]), 2)))

      assert_that(max_ele_pcoll, equal_to(output))

    with beam.Pipeline() as p:
      result_pcoll = (
          p
          | "CreateData" >> beam.Create(inputs)
          | "MLTransform" >>
          MLTransform(read_artifact_location=self.gcs_artifact_location))
      max_ele_pcoll = (
          result_pcoll
          | beam.Map(lambda x: round(np.max(x[test_query_column]), 2)))

      assert_that(max_ele_pcoll, equal_to(output))

  def test_embeddings_with_inference_args(self):
    model_name = DEFAULT_MODEL_NAME

    inference_args = {'convert_to_numpy': False}
    embedding_config = SentenceTransformerEmbeddings(
        model_name=model_name,
        columns=[test_query_column],
        inference_args=inference_args)
    with beam.Pipeline() as pipeline:
      result_pcoll = (
          pipeline
          | "CreateData" >> beam.Create([{
              test_query_column: test_query
          }])
          | "MLTransform" >> MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  embedding_config))

      def assert_element(element):
        assert type(element) == torch.Tensor

      _ = (
          result_pcoll
          | beam.Map(lambda x: x[test_query_column])
          | beam.Map(assert_element))

  def test_mltransform_to_ptransform_with_sentence_transformer(self):
    model_name = ''
    transforms = [
        SentenceTransformerEmbeddings(columns=['x'], model_name=model_name),
        SentenceTransformerEmbeddings(
            columns=['y', 'z'], model_name=model_name)
    ]
    ptransform_mapper = base._MLTransformToPTransformMapper(
        transforms=transforms,
        artifact_location=self.artifact_location,
        artifact_mode=None)

    ptransform_list = ptransform_mapper.create_and_save_ptransform_list()
    self.assertTrue(len(ptransform_list) == 2)

    self.assertEqual(type(ptransform_list[0]), RunInference)
    expected_columns = [['x'], ['y', 'z']]
    for i in range(len(ptransform_list)):
      self.assertEqual(type(ptransform_list[i]), RunInference)
      self.assertEqual(
          type(ptransform_list[i]._model_handler), base._TextEmbeddingHandler)
      self.assertEqual(
          ptransform_list[i]._model_handler.columns, expected_columns[i])
      self.assertEqual(
          ptransform_list[i]._model_handler._underlying.model_name, model_name)
    ptransform_list = (
        base._MLTransformToPTransformMapper.
        load_transforms_from_artifact_location(self.artifact_location))
    for i in range(len(ptransform_list)):
      self.assertEqual(type(ptransform_list[i]), RunInference)
      self.assertEqual(
          type(ptransform_list[i]._model_handler), base._TextEmbeddingHandler)
      self.assertEqual(
          ptransform_list[i]._model_handler.columns, expected_columns[i])
      self.assertEqual(
          ptransform_list[i]._model_handler._underlying.model_name, model_name)

  def generateRandomImage(self, size: int):
    imarray = np.random.rand(size, size, 3) * 255
    return Image.fromarray(imarray.astype('uint8')).convert('RGBA')

  @unittest.skipIf(Image is None, 'Pillow is not installed.')
  def test_sentence_transformer_image_embeddings(self):
    embedding_config = SentenceTransformerEmbeddings(
        model_name=IMAGE_MODEL_NAME,
        columns=[test_query_column],
        image_model=True)
    img = self.generateRandomImage(256)
    with beam.Pipeline() as pipeline:
      result_pcoll = (
          pipeline
          | "CreateData" >> beam.Create([{
              test_query_column: img
          }])
          | "MLTransform" >> MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  embedding_config))

      def assert_element(element):
        assert len(element[test_query_column]) == 512

      _ = (result_pcoll | beam.Map(assert_element))

  def test_sentence_transformer_images_with_str_data_types(self):
    embedding_config = SentenceTransformerEmbeddings(
        model_name=IMAGE_MODEL_NAME,
        columns=[test_query_column],
        image_model=True)
    with self.assertRaisesRegex(Exception, "Embeddings can only be generated"):
      with beam.Pipeline() as pipeline:
        _ = (
            pipeline
            | "CreateData" >> beam.Create([{
                test_query_column: "image.jpg"
            }])
            | "MLTransform" >> MLTransform(
                write_artifact_location=self.artifact_location).with_transform(
                    embedding_config))


class _FakeSentenceTransformer:
  """A stand-in for sentence_transformers.SentenceTransformer."""
  def __init__(self, model_name, **kwargs):
    self.model_name = model_name
    self.kwargs = kwargs
    self.max_seq_length = None


@contextlib.contextmanager
def _fake_sentence_transformers_module():
  """Replaces the sentence_transformers module with a fake one."""
  fake_module = types.ModuleType('sentence_transformers')
  fake_module.SentenceTransformer = _FakeSentenceTransformer
  # Also hides the real sentence_transformers modules, if already imported, and
  # restores them afterwards.
  with test_utils.block_imports('sentence_transformers'):
    sys.modules['sentence_transformers'] = fake_module
    try:
      yield
    finally:
      del sys.modules['sentence_transformers']


def _serialize_and_deserialize(pipeline):
  """Serializes the pipeline, as done during job submission, and deserializes
  it again. Deserializing fails if the pipeline references (i.e. requires) a
  module that can't be imported."""
  return beam.Pipeline.from_runner_api(
      pipeline.to_runner_api(), pipeline.runner, pipeline.options)


class SentenceTransformerJobSubmissionTest(unittest.TestCase):
  """Tests that sentence-transformers is only used when executing pipelines.

  sentence-transformers (and its heavy dependencies, like torch) should only be
  needed for loading the model when the pipeline is executed (for example, on
  the workers), but not for constructing and submitting the pipeline. These
  tests don't require sentence-transformers to be installed.
  """
  def setUp(self):
    self.artifact_location = tempfile.mkdtemp(prefix='sentence_transformers_')

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def _apply_ml_transform(self, pipeline, ml_transform):
    _ = (
        pipeline
        | beam.Create([{
            test_query_column: test_query
        }])
        | ml_transform)

  def test_importing_module_does_not_import_sentence_transformers(self):
    # Executes the module in a new module object, i.e. without replacing the
    # already imported module.
    spec = importlib.util.find_spec(huggingface.__name__)
    with test_utils.block_imports('sentence_transformers') as attempted_imports:
      spec.loader.exec_module(importlib.util.module_from_spec(spec))
    self.assertEqual(attempted_imports, [])

  @parameterized.expand([('text_model', False), ('image_model', True)])
  def test_job_submission_does_not_use_sentence_transformers(
      self, unused_name, image_model):
    with test_utils.block_imports('sentence_transformers') as attempted_imports:
      embedding_config = huggingface.SentenceTransformerEmbeddings(
          model_name=DEFAULT_MODEL_NAME,
          columns=[test_query_column],
          image_model=image_model)
      pipeline = beam.Pipeline()
      self._apply_ml_transform(
          pipeline,
          MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  embedding_config))
      _serialize_and_deserialize(pipeline)
    self.assertEqual(attempted_imports, [])

  def test_reading_artifacts_does_not_use_sentence_transformers(self):
    with test_utils.block_imports('sentence_transformers') as attempted_imports:
      embedding_config = huggingface.SentenceTransformerEmbeddings(
          model_name=DEFAULT_MODEL_NAME, columns=[test_query_column])
      # Artifacts are written when constructing the pipeline.
      self._apply_ml_transform(
          beam.Pipeline(),
          MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  embedding_config))
      pipeline = beam.Pipeline()
      self._apply_ml_transform(
          pipeline, MLTransform(read_artifact_location=self.artifact_location))
      _serialize_and_deserialize(pipeline)
    self.assertEqual(attempted_imports, [])

  def test_loading_model_requires_sentence_transformers(self):
    model_handler = huggingface.SentenceTransformerEmbeddings(
        model_name=DEFAULT_MODEL_NAME,
        columns=[test_query_column]).get_model_handler()
    with test_utils.block_imports('sentence_transformers'):
      with self.assertRaisesRegex(ImportError,
                                  'sentence-transformers is required'):
        model_handler.load_model()

  def test_loading_model_uses_sentence_transformers(self):
    model_handler = huggingface.SentenceTransformerEmbeddings(
        model_name=DEFAULT_MODEL_NAME,
        columns=[test_query_column],
        max_seq_length=128,
        load_model_args={
            'device': 'cpu'
        }).get_model_handler()
    with _fake_sentence_transformers_module():
      model = model_handler.load_model()
    self.assertIsInstance(model, _FakeSentenceTransformer)
    self.assertEqual(model.model_name, DEFAULT_MODEL_NAME)
    self.assertEqual(model.kwargs, {'device': 'cpu'})
    self.assertEqual(model.max_seq_length, 128)

  def test_loading_model_from_artifacts_written_by_older_versions(self):
    model_handler = huggingface.SentenceTransformerEmbeddings(
        model_name=DEFAULT_MODEL_NAME,
        columns=[test_query_column]).get_model_handler()
    # Beam 2.77.0 and earlier stored a reference to the SentenceTransformer
    # class in the artifacts. This is how jsonpickle restores the reference if
    # sentence-transformers is not installed when reading the artifacts.
    with test_utils.block_imports('sentence_transformers'):
      model_handler._model_class = jsonpickle.decode(
          '{"py/type": '
          '"sentence_transformers.SentenceTransformer.SentenceTransformer"}')
    with _fake_sentence_transformers_module():
      model = model_handler.load_model()
    self.assertIsInstance(model, _FakeSentenceTransformer)
    self.assertEqual(model.model_name, DEFAULT_MODEL_NAME)


@unittest.skipIf(not _HF_TOKEN, 'HF_TOKEN environment variable not set.')
class HuggingfaceInferenceAPITest(unittest.TestCase):
  def setUp(self):
    self.artifact_location = tempfile.mkdtemp()
    self.inputs = [{test_query_column: test_query}]

  def tearDown(self):
    shutil.rmtree(self.artifact_location)

  def test_get_api_url_and_when_model_name_not_provided(self):
    with self.assertRaises(ValueError):
      inference_embeddings = InferenceAPIEmbeddings(
          hf_token=_HF_TOKEN,
          columns=[test_query_column],
      )
      _ = inference_embeddings.api_url

  def test_embeddings_with_inference_api(self):
    embedding_config = InferenceAPIEmbeddings(
        hf_token=_HF_TOKEN,
        model_name=DEFAULT_MODEL_NAME,
        columns=[test_query_column],
    )
    expected_output = [0.13]
    with beam.Pipeline() as p:
      result_pcoll = (
          p
          | "CreateData" >> beam.Create(self.inputs)
          | "MLTransform" >> MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  embedding_config))
      max_ele_pcoll = (
          result_pcoll
          | beam.Map(lambda x: round(np.max(x[test_query_column]), 2)))

      assert_that(max_ele_pcoll, equal_to(expected_output))


@unittest.skipIf(not _HF_TOKEN, 'HF_TOKEN environment variable not set.')
class HuggingfaceInferenceAPIGCSLocationTest(HuggingfaceInferenceAPITest):
  def setUp(self):
    self.artifact_location = self.gcs_artifact_location = os.path.join(
        'gs://temp-storage-for-perf-tests/tft_handler', uuid.uuid4().hex)
    self.inputs = [{test_query_column: test_query}]

  def tearDown(self):
    pass


if __name__ == '__main__':
  unittest.main()
