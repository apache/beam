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

try:
  from apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store \
    import VertexAIFeatureStoreEnrichmentHandler
  from apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store \
    import VertexAIFeatureStoreLegacyEnrichmentHandler
except ImportError:
  raise unittest.SkipTest(
      'VertexAI Feature Store test dependencies '
      'are not installed.')


class TestVertexAIFeatureStoreHandlerInit(unittest.TestCase):
  def test_raise_error_duplicate_api_endpoint_online_store(self):
    with self.assertRaises(ValueError):
      _ = VertexAIFeatureStoreEnrichmentHandler(
          project='project',
          location='location',
          api_endpoint='location@google.com',
          feature_store_name='feature_store',
          feature_view_name='feature_view',
          row_key='row_key',
          client_options={'api_endpoint': 'region@google.com'},
      )

  def test_raise_error_duplicate_api_endpoint_legacy_store(self):
    with self.assertRaises(ValueError):
      _ = VertexAIFeatureStoreLegacyEnrichmentHandler(
          project='project',
          location='location',
          api_endpoint='location@google.com',
          feature_store_id='feature_store',
          entity_type_id='entity_id',
          feature_ids=['feature1', 'feature2'],
          row_key='row_key',
          client_options={'api_endpoint': 'region@google.com'},
      )


if __name__ == '__main__':
  unittest.main()
