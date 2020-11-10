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

"""An integration test that labels entities appearing in a video and checks
if some expected entities were properly recognized."""

from __future__ import absolute_import
from __future__ import unicode_literals

import unittest

import hamcrest as hc
from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import matches_all

# Protect against environments where Google Cloud VideoIntelligence client is
# not available.
try:
  from apache_beam.ml.gcp.videointelligenceml import AnnotateVideoWithContext
  from google.cloud.videointelligence import enums
  from google.cloud.videointelligence import types
except ImportError:
  AnnotateVideoWithContext = None


def extract_entities_descriptions(response):
  for result in response.annotation_results:
    for segment in result.segment_presence_label_annotations:
      yield segment.entity.description


@attr('IT')
@unittest.skipIf(
    AnnotateVideoWithContext is None, 'GCP dependencies are not installed')
class VideoIntelligenceMlTestIT(unittest.TestCase):
  VIDEO_PATH = 'gs://apache-beam-samples/advanced_analytics/video/' \
               'gbikes_dinosaur.mp4'

  def test_label_detection_with_video_context(self):
    with TestPipeline(is_integration_test=True) as p:
      output = (
          p
          | beam.Create([(
              self.VIDEO_PATH,
              types.VideoContext(
                  label_detection_config=types.LabelDetectionConfig(
                      label_detection_mode=enums.LabelDetectionMode.SHOT_MODE,
                      model='builtin/latest')))])
          | AnnotateVideoWithContext(features=[enums.Feature.LABEL_DETECTION])
          | beam.ParDo(extract_entities_descriptions)
          | beam.combiners.ToList())

      # Search for at least one entity that contains 'bicycle'.
      assert_that(
          output, matches_all([hc.has_item(hc.contains_string('bicycle'))]))


if __name__ == '__main__':
  unittest.main()
