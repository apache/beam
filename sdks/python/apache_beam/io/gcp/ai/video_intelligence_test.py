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

"""Unit tests for video_intelligence."""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
from base64 import b64encode

import mock

import apache_beam as beam
from apache_beam.io.gcp.ai import helper, video_intelligence
from apache_beam.metrics import MetricsFilter
from apache_beam.typehints.decorators import TypeCheckError

# Protect against environments where video intelligence lib is not available.
try:
  from google.cloud.videointelligence import VideoIntelligenceServiceClient
  from google.cloud import videointelligence
except ImportError:
  VideoIntelligenceServiceClient = None


@unittest.skipIf(VideoIntelligenceServiceClient is None,
                 'Video intelligence dependencies are not installed')
class VideoIntelligenceTest(unittest.TestCase):
  def setUp(self):
    self._mock_client = mock.Mock()
    self.m2 = mock.Mock()
    self.m2.result.return_value = None
    self._mock_client.annotate_video.return_value = self.m2
    self.features = [videointelligence.enums.Feature.LABEL_DETECTION]

  def test_AnnotateVideo_URIs(self):
    videos_to_annotate = ['gs://cloud-samples-data/video/cat.mp4',
                          'gs://cloud-samples-data/video/cat.mp4']
    expected_counter = len(videos_to_annotate)
    with mock.patch.object(helper, 'get_videointelligence_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()
      _ = (p
           | "Create data" >> beam.Create(videos_to_annotate)
           | "Annotate video" >> video_intelligence.AnnotateVideo(self.features)
           )
      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('API Calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.committed == expected_counter)

  def test_AnnotateVideo_b64_content(self):
    base_64_encoded_video = b64encode(
        b'begin 644 cat-video.mp4M    (&9T>7!M<#0R..fake_video_content')
    videos_to_annotate = [base_64_encoded_video, base_64_encoded_video,
                          base_64_encoded_video]
    expected_counter = len(videos_to_annotate)
    with mock.patch.object(helper, 'get_videointelligence_client',
                           return_value=self._mock_client):
      p = beam.Pipeline()
      _ = (p
           | "Create data" >> beam.Create(videos_to_annotate)
           | "Annotate video" >> video_intelligence.AnnotateVideo(self.features)
           )
      result = p.run()
      result.wait_until_finish()

      read_filter = MetricsFilter().with_name('API Calls')
      query_result = result.metrics().query(read_filter)
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        self.assertTrue(read_counter.committed == expected_counter)

  def test_AnnotateVideo_bad_input(self):
    videos_to_annotate = [123456789, 123456789, 123456789]
    with mock.patch.object(helper, 'get_videointelligence_client',
                           return_value=self._mock_client):
      with self.assertRaises(TypeCheckError):
        p = beam.Pipeline()
        _ = (p
             | "Create data" >> beam.Create(videos_to_annotate)
             | "Annotate video" >> video_intelligence.AnnotateVideo(
                 self.features)
             )
        result = p.run()
        result.wait_until_finish()
