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

import shutil
import tempfile
import unittest

import apache_beam as beam
from apache_beam.ml.transforms.base import MLTransform

hub_url = 'https://tfhub.dev/google/LEALLA/LEALLA-small/1'
test_query_column = 'test_query'
test_query = 'This is a test query'

# pylint: disable=ungrouped-imports
try:
  import tensorflow as tf  # disable=unused-import
  from apache_beam.ml.transforms.embeddings.tensorflow_hub import TensorflowHubTextEmbeddings
except ImportError:
  tf = None

try:
  from apache_beam.ml.transforms.tft import ScaleTo01
except ImportError:
  ScaleTo01 = None  # type: ignore


@unittest.skipIf(tf is None, 'Tensorflow is not installed.')
class TFHubEmbeddingsTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp()

  def tearDown(self) -> None:
    shutil.rmtree(self.artifact_location)

  def test_tfhub_text_embeddings(self):
    embedding_config = TensorflowHubTextEmbeddings(
        hub_url=hub_url, columns=[test_query_column])
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
        assert len(element[test_query_column]) == 128

      _ = (transformed_pcoll | beam.Map(assert_element))

  @unittest.skipIf(ScaleTo01 is None, 'Tensorflow Transform is not installed.')
  def test_embeddings_with_scale_to_0_1(self):
    embedding_config = TensorflowHubTextEmbeddings(
        hub_url=hub_url,
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
        assert max(element[test_query_column]) == 1

      _ = (
          transformed_pcoll | beam.Map(lambda x: x.as_dict())
          | beam.Map(assert_element))

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
      embedding_config = TensorflowHubTextEmbeddings(
          hub_url=hub_url, columns=[test_query_column])

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
          assert round(element, 2) == 0.21

        _ = (
            result_pcoll
            | beam.Map(lambda x: max(x[test_query_column]))
            #  0.14797046780586243
            | beam.Map(assert_element))

  def test_with_int_data_types(self):
    embedding_config = TensorflowHubTextEmbeddings(
        hub_url=hub_url, columns=[test_query_column])
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
    artifact_location = 'gs://apache-beam-ml/testing/tensorflow_hub'
    with beam.Pipeline() as p:
      embedding_config = TensorflowHubTextEmbeddings(
          hub_url=hub_url, columns=[test_query_column])

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
          assert round(element, 2) == 0.21

        _ = (
            result_pcoll
            | beam.Map(lambda x: max(x[test_query_column]))
            #  0.14797046780586243
            | beam.Map(assert_element))


if __name__ == '__main__':
  unittest.main()
