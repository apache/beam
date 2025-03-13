# pylint: skip-file
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

"""Unit tests for visionml."""

# pytype: skip-file

import logging
import unittest

import mock

import apache_beam as beam
from apache_beam.metrics import MetricsFilter
from apache_beam.typehints.decorators import TypeCheckError

# Protect against environments where vision lib is not available.
try:
  from google.cloud.vision import ImageAnnotatorClient
  from google.cloud import vision
  from apache_beam.ml.gcp import visionml
except ImportError:
  ImageAnnotatorClient = None


@unittest.skipIf(
    ImageAnnotatorClient is None, 'Vision dependencies are not installed')
class VisionTest(unittest.TestCase):
  def setUp(self):
    self._mock_client = mock.Mock()
    self._mock_client.batch_annotate_images.return_value = None

    feature_type = vision.Feature.Type.TEXT_DETECTION
    self.features = [
        vision.Feature({
            'type': feature_type, 'max_results': 3, 'model': "builtin/stable"
        })
    ]
    self.img_ctx = vision.ImageContext()
    self.min_batch_size = 1
    self.max_batch_size = 1

  def test_AnnotateImage_URIs(self):
    images_to_annotate = [
        'gs://cloud-samples-data/vision/ocr/sign.jpg',
        'gs://cloud-samples-data/vision/ocr/sign.jpg'
    ]

    expected_counter = len(images_to_annotate)
    with mock.patch.object(visionml,
                           'get_vision_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()
      _ = (
          p
          | "Create data" >> beam.Create(images_to_annotate)
          | "Annotate image" >> visionml.AnnotateImage(
              self.features,
              min_batch_size=self.min_batch_size,
              max_batch_size=self.max_batch_size))
      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('API Calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.result == expected_counter)

  def test_AnnotateImage_URI_with_side_input_context(self):
    images_to_annotate = [
        'gs://cloud-samples-data/vision/ocr/sign.jpg',
        'gs://cloud-samples-data/vision/ocr/sign.jpg'
    ]
    image_contexts = [
        ('gs://cloud-samples-data/vision/ocr/sign.jpg', self.img_ctx),
        ('gs://cloud-samples-data/vision/ocr/sign.jpg', self.img_ctx),
    ]

    expected_counter = len(images_to_annotate)
    with mock.patch.object(visionml,
                           'get_vision_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()
      context_side_input = (p | "Image contexts" >> beam.Create(image_contexts))

      _ = (
          p
          | "Create data" >> beam.Create(images_to_annotate)
          | "Annotate image" >> visionml.AnnotateImage(
              self.features,
              min_batch_size=self.min_batch_size,
              max_batch_size=self.max_batch_size,
              context_side_input=beam.pvalue.AsDict(context_side_input)))
      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('API Calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.result == expected_counter)

  def test_AnnotateImage_b64_content(self):
    base_64_encoded_image = \
      b'YmVnaW4gNjQ0IGNhdC12aWRlby5tcDRNICAgICgmOVQ+NyFNPCMwUi4uZmFrZV92aWRlb'
    images_to_annotate = [
        base_64_encoded_image,
        base_64_encoded_image,
        base_64_encoded_image,
    ]
    expected_counter = len(images_to_annotate)
    with mock.patch.object(visionml,
                           'get_vision_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()
      _ = (
          p
          | "Create data" >> beam.Create(images_to_annotate)
          | "Annotate image" >> visionml.AnnotateImage(
              self.features,
              min_batch_size=self.min_batch_size,
              max_batch_size=self.max_batch_size))
      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('API Calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.result == expected_counter)

  def test_AnnotateImageWithContext_URIs(self):
    images_to_annotate = [
        ('gs://cloud-samples-data/vision/ocr/sign.jpg', self.img_ctx),
        ('gs://cloud-samples-data/vision/ocr/sign.jpg', None),
        ('gs://cloud-samples-data/vision/ocr/sign.jpg', self.img_ctx),
    ]
    batch_size = 5
    expected_counter = 1  # All images should fit in the same batch
    with mock.patch.object(visionml,
                           'get_vision_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()
      _ = (
          p
          | "Create data" >> beam.Create(images_to_annotate)
          | "Annotate image" >> visionml.AnnotateImageWithContext(
              self.features,
              min_batch_size=batch_size,
              max_batch_size=batch_size))
      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('API Calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.result == expected_counter)

  def test_AnnotateImageWithContext_bad_input(self):
    """AnnotateImageWithContext should not accept images without context"""
    images_to_annotate = [
        'gs://cloud-samples-data/vision/ocr/sign.jpg',
        'gs://cloud-samples-data/vision/ocr/sign.jpg'
    ]
    with mock.patch.object(visionml,
                           'get_vision_client',
                           return_value=self._mock_client):
      with self.assertRaises(TypeCheckError):
        p = beam.Pipeline()
        _ = (
            p
            | "Create data" >> beam.Create(images_to_annotate)
            | "Annotate image" >> visionml.AnnotateImageWithContext(
                self.features))
        result = p.run()
        result.wait_until_finish()

  def test_AnnotateImage_bad_input(self):
    images_to_annotate = [123456789, 123456789, 123456789]
    with mock.patch.object(visionml,
                           'get_vision_client',
                           return_value=self._mock_client):
      with self.assertRaises(TypeCheckError):
        p = beam.Pipeline()
        _ = (
            p
            | "Create data" >> beam.Create(images_to_annotate)
            | "Annotate image" >> visionml.AnnotateImage(self.features))
        result = p.run()
        result.wait_until_finish()

  def test_AnnotateImage_URIs_large_batch(self):
    images_to_annotate = [
        'gs://cloud-samples-data/vision/ocr/sign.jpg',
        'gs://cloud-samples-data/vision/ocr/sign.jpg',
        'gs://cloud-samples-data/vision/ocr/sign.jpg',
        'gs://cloud-samples-data/vision/ocr/sign.jpg',
        'gs://cloud-samples-data/vision/ocr/sign.jpg',
        'gs://cloud-samples-data/vision/ocr/sign.jpg',
        'gs://cloud-samples-data/vision/ocr/sign.jpg',
        'gs://cloud-samples-data/vision/ocr/sign.jpg',
        'gs://cloud-samples-data/vision/ocr/sign.jpg',
        'gs://cloud-samples-data/vision/ocr/sign.jpg',
        'gs://cloud-samples-data/vision/ocr/sign.jpg',
    ]

    batch_size = 5
    expected_counter = 3  # All 11 images should fit in 3 batches
    with mock.patch.object(visionml,
                           'get_vision_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()
      _ = (
          p
          | "Create data" >> beam.Create(images_to_annotate)
          | "Annotate image" >> visionml.AnnotateImage(
              self.features,
              max_batch_size=batch_size,
              min_batch_size=batch_size))
      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('API Calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.result == expected_counter)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
