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

import os
import shutil
import tempfile
import unittest
import uuid

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.transforms import base
from apache_beam.ml.transforms.base import MLTransform

try:
  from apache_beam.ml.transforms.embeddings.vertex_ai import VertexAITextEmbeddings
  from apache_beam.ml.transforms.embeddings.vertex_ai import VertexAIImageEmbeddings
  from vertexai.vision_models import Image
except ImportError:
  VertexAITextEmbeddings = None  # type: ignore
  VertexAIImageEmbeddings = None  # type: ignore

# pylint: disable=ungrouped-imports
try:
  import tensorflow_transform as tft
  from apache_beam.ml.transforms.tft import ScaleTo01
except ImportError:
  tft = None

test_query = "This is a test"
test_query_column = "feature_1"
model_name: str = "textembedding-gecko@002"


@unittest.skipIf(
    VertexAITextEmbeddings is None, 'Vertex AI Python SDK is not installed.')
class VertexAIEmbeddingsTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp(prefix='_vertex_ai_test')
    self.gcs_artifact_location = os.path.join(
        'gs://temp-storage-for-perf-tests/vertex_ai', uuid.uuid4().hex)

  def tearDown(self) -> None:
    shutil.rmtree(self.artifact_location)

  def test_vertex_ai_text_embeddings(self):
    embedding_config = VertexAITextEmbeddings(
        model_name=model_name, columns=[test_query_column])
    with beam.Pipeline() as pipeline:
      transformed_pcoll = (
          pipeline
          | "CreateData" >> beam.Create([{
              test_query_column: test_query
          }])
          | "MLTransform" >> MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  embedding_config))

      def assert_element(element):
        assert len(element[test_query_column]) == 768

      _ = (transformed_pcoll | beam.Map(assert_element))

  @unittest.skipIf(tft is None, 'Tensorflow Transform is not installed.')
  def test_embeddings_with_scale_to_0_1(self):
    embedding_config = VertexAITextEmbeddings(
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

  def pipeline_with_configurable_artifact_location(
      self,
      pipeline,
      embedding_config=None,
      read_artifact_location=None,
      write_artifact_location=None):
    if write_artifact_location:
      return (
          pipeline
          | MLTransform(write_artifact_location=write_artifact_location).
          with_transform(embedding_config))
    elif read_artifact_location:
      return (
          pipeline
          | MLTransform(read_artifact_location=read_artifact_location))
    else:
      raise NotImplementedError

  def test_embeddings_with_read_artifact_location(self):
    with beam.Pipeline() as p:
      embedding_config = VertexAITextEmbeddings(
          model_name=model_name, columns=[test_query_column])

      with beam.Pipeline() as p:
        data = (
            p
            | "CreateData" >> beam.Create([{
                test_query_column: test_query
            }]))
        _ = self.pipeline_with_configurable_artifact_location(
            pipeline=data,
            embedding_config=embedding_config,
            write_artifact_location=self.artifact_location)

      with beam.Pipeline() as p:
        data = (
            p
            | "CreateData" >> beam.Create([{
                test_query_column: test_query
            }, {
                test_query_column: test_query
            }]))
        result_pcoll = self.pipeline_with_configurable_artifact_location(
            pipeline=data, read_artifact_location=self.artifact_location)

        def assert_element(element):
          assert round(element, 2) == 0.15

        _ = (
            result_pcoll
            | beam.Map(lambda x: max(x[test_query_column]))
            #  0.14797046780586243
            | beam.Map(assert_element))

  def test_with_int_data_types(self):
    embedding_config = VertexAITextEmbeddings(
        model_name=model_name, columns=[test_query_column])
    with self.assertRaises(TypeError):
      with beam.Pipeline() as pipeline:
        _ = (
            pipeline
            | "CreateData" >> beam.Create([{
                test_query_column: 1
            }])
            | "MLTransform" >> MLTransform(
                write_artifact_location=self.artifact_location).with_transform(
                    embedding_config))

  def test_with_gcs_artifact_location(self):
    with beam.Pipeline() as p:
      embedding_config = VertexAITextEmbeddings(
          model_name=model_name, columns=[test_query_column])

      with beam.Pipeline() as p:
        data = (
            p
            | "CreateData" >> beam.Create([{
                test_query_column: test_query
            }]))
        _ = self.pipeline_with_configurable_artifact_location(
            pipeline=data,
            embedding_config=embedding_config,
            write_artifact_location=self.gcs_artifact_location)

      with beam.Pipeline() as p:
        data = (
            p
            | "CreateData" >> beam.Create([{
                test_query_column: test_query
            }, {
                test_query_column: test_query
            }]))
        result_pcoll = self.pipeline_with_configurable_artifact_location(
            pipeline=data, read_artifact_location=self.gcs_artifact_location)

        def assert_element(element):
          assert round(element, 2) == 0.15

        _ = (
            result_pcoll
            | beam.Map(lambda x: max(x[test_query_column]))
            #  0.14797046780586243
            | beam.Map(assert_element))

  def test_mltransform_to_ptransform_with_vertex(self):
    model_name = 'textembedding-gecko@002'
    transforms = [
        VertexAITextEmbeddings(
            columns=['x'],
            model_name=model_name,
            task_type='RETRIEVAL_DOCUMENT'),
        VertexAITextEmbeddings(
            columns=['y', 'z'], model_name=model_name, task_type='CLUSTERING')
    ]
    ptransform_mapper = base._MLTransformToPTransformMapper(
        transforms=transforms,
        artifact_location=self.artifact_location,
        artifact_mode=None)

    ptransform_list = ptransform_mapper.create_and_save_ptransform_list()
    self.assertTrue(len(ptransform_list) == 2)

    self.assertEqual(type(ptransform_list[0]), RunInference)
    expected_columns = [['x'], ['y', 'z']]
    expected_task_type = ['RETRIEVAL_DOCUMENT', 'CLUSTERING']
    for i in range(len(ptransform_list)):
      self.assertEqual(type(ptransform_list[i]), RunInference)
      self.assertEqual(
          type(ptransform_list[i]._model_handler), base._TextEmbeddingHandler)
      self.assertEqual(
          ptransform_list[i]._model_handler.columns, expected_columns[i])
      self.assertEqual(
          ptransform_list[i]._model_handler._underlying.task_type,
          expected_task_type[i])
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
          ptransform_list[i]._model_handler._underlying.task_type,
          expected_task_type[i])
      self.assertEqual(
          ptransform_list[i]._model_handler._underlying.model_name, model_name)


@unittest.skipIf(
    VertexAIImageEmbeddings is None, 'Vertex AI Python SDK is not installed.')
class VertexAIImageEmbeddingsTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp(prefix='_vertex_ai_image_test')
    self.gcs_artifact_location = os.path.join(
        'gs://temp-storage-for-perf-tests/vertex_ai_image', uuid.uuid4().hex)
    self.model_name = "multimodalembedding"
    self.image_path = "gs://apache-beam-ml/testing/inputs/vertex_images/sunflowers/1008566138_6927679c8a.jpg"  # pylint: disable=line-too-long

  def tearDown(self) -> None:
    shutil.rmtree(self.artifact_location)

  def test_vertex_ai_image_embedding(self):
    embedding_config = VertexAIImageEmbeddings(
        model_name=self.model_name, columns=[test_query_column], dimension=128)
    with beam.Pipeline() as pipeline:
      transformed_pcoll = (
          pipeline | "CreateData" >> beam.Create([{
              test_query_column: Image(gcs_uri=self.image_path)
          }])
          | "MLTransform" >> MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  embedding_config))

      def assert_element(element):
        assert len(element[test_query_column]) == 128

      _ = (transformed_pcoll | beam.Map(assert_element))

  def test_improper_dimension(self):
    with self.assertRaises(ValueError):
      _ = VertexAIImageEmbeddings(
          model_name=self.model_name,
          columns=[test_query_column],
          dimension=127)


if __name__ == '__main__':
  unittest.main()
