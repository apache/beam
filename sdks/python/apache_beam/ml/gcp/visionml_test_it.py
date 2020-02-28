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

from __future__ import absolute_import

import unittest

from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.ml.gcp.visionml import AnnotateImage
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

try:
  from google.cloud.vision import types
except ImportError:
  types = None


def extract(response):
  for r in response.responses:
    for text_annotation in r.text_annotations:
      yield text_annotation.description


@attr('IT')
class VisionMlTestIT(unittest.TestCase):
  IMAGES_TO_ANNOTATE = ['gs://cloud-samples-data/vision/ocr/sign.jpg']
  IMAGE_CONTEXT = [types.ImageContext(language_hints=['en'])]

  def test_text_detection_with_language_hint(self):
    with TestPipeline(is_integration_test=True) as p:
      contexts = p | 'Create context' >> beam.Create(
          dict(zip(self.IMAGES_TO_ANNOTATE, self.IMAGE_CONTEXT)))

      output = (
          p
          | beam.Create(self.IMAGES_TO_ANNOTATE)
          | AnnotateImage(
              features=[types.Feature(type='TEXT_DETECTION')],
              context_side_input=beam.pvalue.AsDict(contexts))
          | beam.ParDo(extract))

    assert_that(
        output,
        equal_to([
            'WAITING?\nPLEASE\nTURN OFF\nYOUR\nENGINE',
            'WAITING?',
            'PLEASE',
            'TURN',
            'OFF',
            'YOUR',
            'ENGINE'
        ]))


if __name__ == '__main__':
  unittest.main()
