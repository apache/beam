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
import unittest

from parameterized import parameterized

try:
  from apache_beam.transforms.enrichment_handlers.feast_feature_store import \
    FeastFeatureStoreEnrichmentHandler
  from apache_beam.transforms.enrichment_handlers.feast_feature_store_it_test \
    import _entity_row_fn
except ImportError:
  raise unittest.SkipTest(
      'Feast feature store test dependencies are not installed.')


class TestFeastFeatureStoreHandler(unittest.TestCase):
  def setUp(self) -> None:
    self.feature_store_yaml_file = (
        'gs://apache-beam-testing-enrichment/'
        'feast-feature-store/repos/ecommerce/'
        'feature_repo/feature_store.yaml')
    self.feature_service_name = 'demograph_service'

  def test_feature_store_yaml_path_exists(self):
    feature_store_yaml_path = 'gs://apache-beam-testing-enrichment/invalid.yaml'
    with self.assertRaises(ValueError):
      _ = FeastFeatureStoreEnrichmentHandler(
          entity_id='user_id',
          feature_store_yaml_path=feature_store_yaml_path,
          feature_service_name=self.feature_service_name,
      )

  def test_feast_enrichment_no_feature_service(self):
    """Test raising an error in case of no feature service name."""
    with self.assertRaises(ValueError):
      _ = FeastFeatureStoreEnrichmentHandler(
          entity_id='user_id',
          feature_store_yaml_path=self.feature_store_yaml_file,
      )

  @parameterized.expand([('user_id', _entity_row_fn), ('', None)])
  def test_feast_enrichment_invalid_args(self, entity_id, entity_row_fn):
    with self.assertRaises(ValueError):
      _ = FeastFeatureStoreEnrichmentHandler(
          feature_store_yaml_path=self.feature_store_yaml_file,
          entity_id=entity_id,
          entity_row_fn=entity_row_fn,
      )


if __name__ == '__main__':
  unittest.main()
