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

from __future__ import absolute_import, unicode_literals

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
    self._mock_client.annotate_image.return_value = None
    feature_type = vision.enums.Feature.Type.TEXT_DETECTION
    self.features = [
        vision.types.Feature(
            type=feature_type, max_results=3, model="builtin/stable")
    ]

  def test_AnnotateImage_URI_with_ImageContext(self):
    img_ctx = vision.types.ImageContext()
    images_to_annotate = [
        ('gs://cloud-samples-data/vision/ocr/sign.jpg', img_ctx)
    ]
    expected_counter = len(images_to_annotate)
    with mock.patch.object(visionml,
                           'get_vision_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()
      _ = (
          p
          | "Create data" >> beam.Create(images_to_annotate)
          | "Annotate image" >> visionml.AnnotateImage(self.features))
      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('API Calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.committed == expected_counter)

  def test_AnnotateImage_URI(self):
    images_to_annotate = ['gs://cloud-samples-data/vision/ocr/sign.jpg']
    expected_counter = len(images_to_annotate)
    with mock.patch.object(visionml,
                           'get_vision_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()
      _ = (
          p
          | "Create data" >> beam.Create(images_to_annotate)
          | "Annotate image" >> visionml.AnnotateImage(self.features))
      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('API Calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.committed == expected_counter)

  def test_AnnotateImage_byte_content(self):
    img_ctx = vision.types.ImageContext()
    base_64_encoded_bytes = \
      (
      b'begin 644 sign.jpg _]C_X  02D9)1@ ! 0$ 2 !(  #_X2EZ fake_image_content',
      img_ctx)

    images_to_annotate = [base_64_encoded_bytes]
    expected_counter = len(images_to_annotate)
    with mock.patch.object(visionml,
                           'get_vision_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()
      _ = (
          p
          | "Create data" >> beam.Create(images_to_annotate)
          | "Annotate image" >> visionml.AnnotateImage(self.features))
      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('API Calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.committed == expected_counter)

  def test_AnnotateImage_byte_content_with_ImageContext(self):
    base_64_encoded_bytes = \
      b'begin 644 sign.jpg _]C_X  02D9)1@ ! 0$ 2 !(  #_X2EZ fake_image_content'

    images_to_annotate = [base_64_encoded_bytes]
    expected_counter = len(images_to_annotate)
    with mock.patch.object(visionml,
                           'get_vision_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()
      _ = (
          p
          | "Create data" >> beam.Create(images_to_annotate)
          | "Annotate image" >> visionml.AnnotateImage(self.features))
      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('API Calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.committed == expected_counter)

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
