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

"""Integration tests for cross-language transform expansion."""

# pytype: skip-file

import random
import string
import unittest

import pytest

import apache_beam as beam
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.util import GcpSecret
from apache_beam.transforms.util import Secret

try:
  from google.cloud import secretmanager
except ImportError:
  secretmanager = None  # type: ignore[assignment]


class GbekIT(unittest.TestCase):
  def setUp(self):
    if secretmanager is not None:
      self.project_id = 'apache-beam-testing'
      secret_postfix = ''.join(random.choice(string.digits) for _ in range(6))
      self.secret_id = 'gbek_secret_tests_' + secret_postfix
      self.client = secretmanager.SecretManagerServiceClient()
      self.project_path = f'projects/{self.project_id}'
      self.secret_path = f'{self.project_path}/secrets/{self.secret_id}'
      try:
        self.client.get_secret(request={'name': self.secret_path})
      except Exception:
        self.client.create_secret(
            request={
                'parent': self.project_path,
                'secret_id': self.secret_id,
                'secret': {
                    'replication': {
                        'automatic': {}
                    }
                }
            })
        self.client.add_secret_version(
            request={
                'parent': self.secret_path,
                'payload': {
                    'data': Secret.generate_secret_bytes()
                }
            })
      version_name = f'{self.secret_path}/versions/latest'
      self.gcp_secret = GcpSecret(version_name)
      self.secret_option = f'type:GcpSecret;version_name:{version_name}'

  def tearDown(self):
    if secretmanager is not None:
      self.client.delete_secret(request={'name': self.secret_path})

  @pytest.mark.it_postcommit
  @unittest.skipIf(secretmanager is None, 'GCP dependencies are not installed')
  def test_gbk_with_gbek_it(self):
    pipeline = TestPipeline(is_integration_test=True)
    pipeline.options.view_as(SetupOptions).gbek = self.secret_option

    pcoll_1 = pipeline | 'Start 1' >> beam.Create([('a', 1), ('a', 2), ('b', 3),
                                                   ('c', 4)])
    result = (pcoll_1) | beam.GroupByKey()
    sorted_result = result | beam.Map(lambda x: (x[0], sorted(x[1])))
    assert_that(
        sorted_result, equal_to([('a', ([1, 2])), ('b', ([3])), ('c', ([4]))]))

    pipeline.run().wait_until_finish()

  @pytest.mark.it_postcommit
  @unittest.skipIf(secretmanager is None, 'GCP dependencies are not installed')
  def test_combineValues_with_gbek_it(self):
    pipeline = TestPipeline(is_integration_test=True)
    pipeline.options.view_as(SetupOptions).gbek = self.secret_option

    pcoll_1 = pipeline | 'Start 1' >> beam.Create([('a', 1), ('a', 2), ('b', 3),
                                                   ('c', 4)])
    result = (pcoll_1) | beam.CombinePerKey(sum)
    assert_that(result, equal_to([('a', 3), ('b', 3), ('c', 4)]))

    pipeline.run().wait_until_finish()


if __name__ == '__main__':
  unittest.main()
