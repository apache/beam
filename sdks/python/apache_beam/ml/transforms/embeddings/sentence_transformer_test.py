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

# pylint:skip-file

import shutil
import tempfile
import unittest

import apache_beam as beam
from apache_beam.ml.transforms.base import MLTransform
from apache_beam.ml.transforms.embeddings.sentence_transformer import SentenceTransformerEmbeddings

try:
  import tensorflow_transform as tft  # pylint: disbale=unused-import
  from apache_beam.ml.transforms.tft import ScaleTo01
except ImportError:
  tft = None

test_query = "This is a test"
test_query_column = "feature_1"
DEFAULT_MODEL_NAME = "sentence-transformers/all-mpnet-base-v2"


def get_pipeline_wth_embedding_config(
    pipeline: beam.Pipeline, embedding_config, artifact_location):
  transformed_pcoll = (
      pipeline
      | "CreateData" >> beam.Create([{
          test_query_column: test_query
      }])
      | "MLTransform" >> MLTransform(write_artifact_location=artifact_location).
      with_transform(embedding_config))
  return transformed_pcoll


class SentenceTrasformerEmbeddingsTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self) -> None:
    shutil.rmtree(self.artifact_location)

  def test_sentence_transformer_embeddings(self):
    model_name = DEFAULT_MODEL_NAME
    embedding_config = SentenceTransformerEmbeddings(
        model_name=model_name, columns=[test_query_column])
    with beam.Pipeline() as pipeline:
      result_pcoll = get_pipeline_wth_embedding_config(
          pipeline=pipeline,
          embedding_config=embedding_config,
          artifact_location=self.artifact_location)

      def assert_element(element):
        assert len(element[test_query_column]) == 768

      (result_pcoll | beam.Map(assert_element))

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

      (transformed_pcoll | beam.Map(assert_element))

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
      model_name = DEFAULT_MODEL_NAME
      embedding_config = SentenceTransformerEmbeddings(
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
          assert round(element, 2) == 0.13

        (
            result_pcoll
            | beam.Map(lambda x: max(x[test_query_column]))
            #  0.1342099905014038
            | beam.Map(assert_element))

  def test_sentence_transformer_with_int_data_types(self):
    model_name = DEFAULT_MODEL_NAME
    embedding_config = SentenceTransformerEmbeddings(
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
    # artifact_location = 'gs://apache-beam-testing/testing/mltransform_artifacts'
    artifact_location = 'gs://anandinguva-test/artifacts/sentence_transformers'
    with beam.Pipeline() as p:
      model_name = DEFAULT_MODEL_NAME
      embedding_config = SentenceTransformerEmbeddings(
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
            write_artifact_location=artifact_location)

      with beam.Pipeline() as p:
        data = (
            p
            | "CreateData" >> beam.Create([{
                test_query_column: test_query
            }, {
                test_query_column: test_query
            }]))
        result_pcoll = self.pipeline_with_configurable_artifact_location(
            pipeline=data, read_artifact_location=artifact_location)

        def assert_element(element):
          assert round(element, 2) == 0.13

        (
            result_pcoll
            | beam.Map(lambda x: max(x[test_query_column]))
            #  0.1342099905014038
            | beam.Map(assert_element))


if __name__ == '__main__':
  unittest.main()
