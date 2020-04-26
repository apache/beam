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
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where Google Cloud Natural Language client is
# not available.
try:
  from apache_beam.ml.gcp.naturallanguageml import AnnotateText
  from apache_beam.ml.gcp.naturallanguageml import Document
  from apache_beam.ml.gcp.naturallanguageml import enums
  from apache_beam.ml.gcp.naturallanguageml import types
except ImportError:
  AnnotateText = None


def extract(response):
  yield beam.pvalue.TaggedOutput('language', response.language)
  yield beam.pvalue.TaggedOutput(
      'parts_of_speech',
      [
          enums.PartOfSpeech.Tag(x.part_of_speech.tag).name
          for x in response.tokens
      ])


@attr('IT')
@unittest.skipIf(AnnotateText is None, 'GCP dependencies are not installed')
class NaturalLanguageMlTestIT(unittest.TestCase):
  def test_analyzing_syntax(self):
    with TestPipeline(is_integration_test=True) as p:
      output = (
          p
          | beam.Create([Document('Unified programming model.')])
          | AnnotateText(
              types.AnnotateTextRequest.Features(extract_syntax=True))
          | beam.ParDo(extract).with_outputs('language', 'parts_of_speech'))

      assert_that(output.language, equal_to(['en']), label='verify_language')
      assert_that(
          output.parts_of_speech,
          equal_to([['ADJ', 'NOUN', 'NOUN', 'PUNCT']]),
          label='verify_parts_of_speech')


if __name__ == '__main__':
  unittest.main()
