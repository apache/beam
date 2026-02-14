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

import sys
import unittest
from datetime import datetime

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

try:
  from google.cloud import kms
except ImportError:
  kms = None  # type: ignore[assignment]


class GbekIT(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    if secretmanager is not None:
      cls.project_id = 'apache-beam-testing'
      py_version = f'_py{sys.version_info.major}{sys.version_info.minor}'
      secret_postfix = datetime.now().strftime('%m%d_%H%M%S') + py_version
      cls.secret_id = 'gbekit_secret_tests_' + secret_postfix
      cls.client = secretmanager.SecretManagerServiceClient()
      cls.project_path = f'projects/{cls.project_id}'
      cls.secret_path = f'{cls.project_path}/secrets/{cls.secret_id}'
      try:
        cls.client.get_secret(request={'name': cls.secret_path})
      except Exception:
        cls.client.create_secret(
            request={
                'parent': cls.project_path,
                'secret_id': cls.secret_id,
                'secret': {
                    'replication': {
                        'automatic': {}
                    }
                }
            })
        cls.client.add_secret_version(
            request={
                'parent': cls.secret_path,
                'payload': {
                    'data': Secret.generate_secret_bytes()
                }
            })
      version_name = f'{cls.secret_path}/versions/latest'
      cls.gcp_secret = GcpSecret(version_name)
      cls.secret_option = f'type:GcpSecret;version_name:{version_name}'

    if kms is not None:
      cls.kms_client = kms.KeyManagementServiceClient()
      cls.location_id = 'global'
      py_version = f'_py{sys.version_info.major}{sys.version_info.minor}'
      secret_postfix = datetime.now().strftime('%m%d_%H%M%S') + py_version
      cls.key_ring_id = 'gbekit_key_ring_tests'
      cls.key_ring_path = cls.kms_client.key_ring_path(
          cls.project_id, cls.location_id, cls.key_ring_id)
      try:
        cls.kms_client.get_key_ring(request={'name': cls.key_ring_path})
      except Exception:
        parent = f'projects/{cls.project_id}/locations/{cls.location_id}'
        cls.kms_client.create_key_ring(
            request={
                'parent': parent,
                'key_ring_id': cls.key_ring_id,
            })
      cls.key_id = 'gbekit_key_tests'
      cls.key_path = cls.kms_client.crypto_key_path(
          cls.project_id, cls.location_id, cls.key_ring_id, cls.key_id)
      try:
        cls.kms_client.get_crypto_key(request={'name': cls.key_path})
      except Exception:
        cls.kms_client.create_crypto_key(
            request={
                'parent': cls.key_ring_path,
                'crypto_key_id': cls.key_id,
                'crypto_key': {
                    'purpose': kms.CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT
                }
            })
      cls.hsm_secret_option = (
          f'type:GcpHsmGeneratedSecret;project_id:{cls.project_id};'
          f'location_id:{cls.location_id};key_ring_id:{cls.key_ring_id};'
          f'key_id:{cls.key_id};job_name:{secret_postfix}')

  @classmethod
  def tearDownClass(cls):
    if secretmanager is not None:
      cls.client.delete_secret(request={'name': cls.secret_path})

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
  @unittest.skipIf(kms is None, 'GCP dependencies are not installed')
  def test_gbk_with_gbek_hsm_it(self):
    pipeline = TestPipeline(is_integration_test=True)
    pipeline.options.view_as(SetupOptions).gbek = self.hsm_secret_option

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
