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
  from apache_beam.ml.rag.types import Chunk
  from apache_beam.ml.rag.types import Content
  from apache_beam.ml.transforms.embeddings.vertex_ai import VertexAIMultiModalEmbeddings
  from apache_beam.ml.transforms.embeddings.vertex_ai import VertexAITextEmbeddings
  from apache_beam.ml.transforms.embeddings.vertex_ai import VertexAIImageEmbeddings
  from apache_beam.ml.transforms.embeddings.vertex_ai import VertexImage
  from apache_beam.ml.transforms.embeddings.vertex_ai import VertexVideo
  from vertexai.vision_models import Image
  from vertexai.vision_models import Video
  from vertexai.vision_models import VideoSegmentConfig
except ImportError:
  VertexAIMultiModalEmbeddings = None  # type: ignore
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
model_name: str = "text-embedding-005"


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
          assert round(element, 2) == 0.11

        _ = (
            result_pcoll
            | beam.Map(lambda x: max(x[test_query_column]))
            #  0.14797046780586243
            | beam.Map(assert_element))

  def test_with_int_data_types(self):
    embedding_config = VertexAITextEmbeddings(
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
          assert round(element, 2) == 0.11

        _ = (
            result_pcoll
            | beam.Map(lambda x: max(x[test_query_column]))
            #  0.14797046780586243
            | beam.Map(assert_element))

  def test_mltransform_to_ptransform_with_vertex(self):
    model_name = 'text-embedding-005'
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


image_feature_column: str = "img_feature"
text_feature_column: str = "txt_feature"
video_feature_column: str = "vid_feature"


def _make_text_chunk(input: str) -> Chunk:
  return Chunk(content=Content(text=input))


@unittest.skipIf(
    VertexAIMultiModalEmbeddings is None,
    'Vertex AI Python SDK is not installed.')
class VertexAIMultiModalEmbeddingsTest(unittest.TestCase):
  def setUp(self) -> None:
    self.artifact_location = tempfile.mkdtemp(
        prefix='_vertex_ai_multi_modal_test')
    self.gcs_artifact_location = os.path.join(
        'gs://temp-storage-for-perf-tests/vertex_ai_multi_modal',
        uuid.uuid4().hex)
    self.model_name = "multimodalembedding"
    self.image_path = "gs://apache-beam-ml/testing/inputs/vertex_images/sunflowers/1008566138_6927679c8a.jpg"  # pylint: disable=line-too-long
    self.video_path = "gs://cloud-samples-data/vertex-ai-vision/highway_vehicles.mp4"  # pylint: disable=line-too-long
    self.video_segment_config = VideoSegmentConfig(end_offset_sec=1)

  def tearDown(self) -> None:
    shutil.rmtree(self.artifact_location)

  def test_vertex_ai_multimodal_embedding_img_and_text(self):
    embedding_config = VertexAIMultiModalEmbeddings(
        model_name=self.model_name,
        image_column=image_feature_column,
        text_column=text_feature_column,
        dimension=128,
        project="apache-beam-testing",
        location="us-central1")
    with beam.Pipeline() as pipeline:
      transformed_pcoll = (
          pipeline | "CreateData" >> beam.Create([{
              image_feature_column: VertexImage(
                  image_content=Image(gcs_uri=self.image_path)),
              text_feature_column: _make_text_chunk("an image of sunflowers"),
          }])
          | "MLTransform" >> MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  embedding_config))

      def assert_element(element):
        assert len(element[image_feature_column].embedding) == 128
        assert len(
            element[text_feature_column].embedding.dense_embedding) == 128

      _ = (transformed_pcoll | beam.Map(assert_element))

  def test_vertex_ai_multimodal_embedding_video(self):
    embedding_config = VertexAIMultiModalEmbeddings(
        model_name=self.model_name,
        video_column=video_feature_column,
        dimension=1408,
        project="apache-beam-testing",
        location="us-central1")
    with beam.Pipeline() as pipeline:
      transformed_pcoll = (
          pipeline | "CreateData" >> beam.Create([{
              video_feature_column: VertexVideo(
                  video_content=Video(gcs_uri=self.video_path),
                  config=self.video_segment_config)
          }])
          | "MLTransform" >> MLTransform(
              write_artifact_location=self.artifact_location).with_transform(
                  embedding_config))

      def assert_element(element):
        # Videos are returned in VideoEmbedding objects, must unroll
        # for each segment.
        for segment in element[video_feature_column].embeddings:
          assert len(segment.embedding) == 1408

      _ = (transformed_pcoll | beam.Map(assert_element))

  def test_improper_dimension(self):
    with self.assertRaises(ValueError):
      _ = VertexAIMultiModalEmbeddings(
          model_name=self.model_name,
          image_column="fake_img_column",
          dimension=127)

  def test_missing_columns(self):
    with self.assertRaises(ValueError):
      _ = VertexAIMultiModalEmbeddings(
          model_name=self.model_name, dimension=128)

  def test_improper_video_dimension(self):
    with self.assertRaises(ValueError):
      _ = VertexAIMultiModalEmbeddings(
          model_name=self.model_name,
          video_column=video_feature_column,
          dimension=128)


if __name__ == '__main__':
  unittest.main()
