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

"""Unit tests for Google Cloud Natural Language API transform."""

import unittest

from apache_beam.metrics import MetricsFilter

# Protect against environments where Google Cloud Natural Language client
# is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from google.cloud import language
except ImportError:
  language = None
else:
  from apache_beam.ml.gcp import naturallanguageml
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports


@unittest.skipIf(language is None, 'GCP dependencies are not installed')
class NaturalLanguageMlTest(unittest.TestCase):
  def assertCounterEqual(self, pipeline_result, counter_name, expected):
    metrics = pipeline_result.metrics().query(
        MetricsFilter().with_name(counter_name))
    try:
      counter = metrics['counters'][0]
      self.assertEqual(expected, counter.result)
    except IndexError:
      raise AssertionError('Counter "{}" was not found'.format(counter_name))

  def test_document_source(self):
    document = naturallanguageml.Document('Hello, world!')
    dict_ = naturallanguageml.Document.to_dict(document)
    self.assertTrue('content' in dict_)
    self.assertFalse('gcs_content_uri' in dict_)

    document = naturallanguageml.Document('gs://sample/location', from_gcs=True)
    dict_ = naturallanguageml.Document.to_dict(document)
    self.assertFalse('content' in dict_)
    self.assertTrue('gcs_content_uri' in dict_)


if __name__ == '__main__':
  unittest.main()
