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

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.transforms import base
from apache_beam.ml.transforms.base import MLTransform

try:
  from sdks.python.apache_beam.ml.transforms.embeddings.open_ai import OpenAITextEmbeddings
except ImportError:
  OpenAITextEmbeddings = None

# pylint: disable=ungrouped-imports
try:
  import tensorflow_transform as tft
except ImportError:
  tft = None

test_query = "This is a test"
test_query_column = "embedding"
model_name: str = "text-embedding-3-small"


@unittest.skipIf(
    OpenAITextEmbeddings is None, 'OpenAI Python SDK is not installed.')
class OpenAIEmbeddingsTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp(prefix='_openai_test')
    self.api_key = os.environ.get('OPENAI_API_KEY')

  def tearDown(self) -> None:
    shutil.rmtree(self.artifact_location)

  def test_openai_text_embeddings(self):
    embedding_config = OpenAITextEmbeddings(
        model_name=model_name, columns=["embedding"], api_key=self.api_key)
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
        # OpenAI text-embedding-3-small produces 1536-dimensional embeddings
        assert len(element[test_query_column]) == 1536

      _ = (transformed_pcoll | beam.Map(assert_element))

  @unittest.skipIf(tft is None, 'Tensorflow Transform is not installed.')
  def test_embeddings_with_scale_to_0_1(self):
    embedding_config = OpenAITextEmbeddings(
        model_name=model_name,
        columns=[test_query_column],
        api_key=self.api_key,
    )
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
        assert max(element.feature_1) == 1

      _ = (transformed_pcoll | beam.Map(assert_element))

  def test_embeddings_with_dimensions(self):
    embedding_config = OpenAITextEmbeddings(
        model_name=model_name,
        columns=[test_query_column],
        api_key=self.api_key,
        dimensions=512)
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
        # Check that we get 512-dimensional embeddings as requested
        assert len(element[test_query_column]) == 512

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
    embedding_config = OpenAITextEmbeddings(
        model_name=model_name,
        columns=[test_query_column],
        api_key=self.api_key)

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

      # Since we don't know the exact values of the embeddings,
      # we just check that they are within a reasonable range
      def assert_element(element):
        # Embeddings should be normalized and generally small values
        assert -1 <= element <= 1

      _ = (
          result_pcoll
          | beam.Map(lambda x: max(x[test_query_column]))
          | beam.Map(assert_element))

  def test_with_int_data_types(self):
    embedding_config = OpenAITextEmbeddings(
        model_name=model_name,
        columns=[test_query_column],
        api_key=self.api_key)
    with self.assertRaises(Exception):
      with beam.Pipeline() as pipeline:
        _ = (
            pipeline
            | "CreateData" >> beam.Create([{
                test_query_column: 1
            }])
            | "MLTransform" >> MLTransform(
                write_artifact_location=self.artifact_location).with_transform(
                    embedding_config))

  def test_with_artifact_location(self):  # pylint: disable=line-too-long
    """Local artifact location test"""
    secondary_artifact_location = tempfile.mkdtemp(
        prefix='_openai_secondary_test')

    try:
      embedding_config = OpenAITextEmbeddings(
          model_name=model_name,
          columns=[test_query_column],
          api_key=self.api_key)

      with beam.Pipeline() as p:
        data = (
            p
            | "CreateData" >> beam.Create([{
                test_query_column: test_query
            }]))
        _ = self.pipeline_with_configurable_artifact_location(
            pipeline=data,
            embedding_config=embedding_config,
            write_artifact_location=secondary_artifact_location)

      with beam.Pipeline() as p:
        data = (
            p
            | "CreateData" >> beam.Create([{
                test_query_column: test_query
            }, {
                test_query_column: test_query
            }]))
        result_pcoll = self.pipeline_with_configurable_artifact_location(
            pipeline=data, read_artifact_location=secondary_artifact_location)

        def assert_element(element):
          # Embeddings should be normalized and generally small values
          assert -1 <= element <= 1

        _ = (
            result_pcoll
            | beam.Map(lambda x: max(x[test_query_column]))
            | beam.Map(assert_element))
    finally:
      # Clean up the temporary directory
      shutil.rmtree(secondary_artifact_location)

  def test_mltransform_to_ptransform_with_openai(self):  # pylint: disable=line-too-long
    transforms = [
        OpenAITextEmbeddings(
            columns=['x'],
            model_name=model_name,
            api_key=self.api_key,
            dimensions=512),
        OpenAITextEmbeddings(
            columns=['y', 'z'], model_name=model_name, api_key=self.api_key)
    ]
    ptransform_mapper = base._MLTransformToPTransformMapper(
        transforms=transforms,
        artifact_location=self.artifact_location,
        artifact_mode=None)

    ptransform_list = ptransform_mapper.create_and_save_ptransform_list()
    self.assertTrue(len(ptransform_list) == 2)

    self.assertEqual(type(ptransform_list[0]), RunInference)
    expected_columns = [['x'], ['y', 'z']]
    expected_dimensions = [512, None]
    for i in range(len(ptransform_list)):
      self.assertEqual(type(ptransform_list[i]), RunInference)
      self.assertEqual(
          type(ptransform_list[i]._model_handler), base._TextEmbeddingHandler)
      self.assertEqual(
          ptransform_list[i]._model_handler.columns, expected_columns[i])
      self.assertEqual(
          ptransform_list[i]._model_handler._underlying.model_name, model_name)
      if expected_dimensions[i]:
        self.assertEqual(
            ptransform_list[i]._model_handler._underlying.dimensions,
            expected_dimensions[i])

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
      if expected_dimensions[i]:
        self.assertEqual(
            ptransform_list[i]._model_handler._underlying.dimensions,
            expected_dimensions[i])


if __name__ == '__main__':
  unittest.main()
