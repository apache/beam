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

import logging
import unittest

import mock

import apache_beam as beam
from apache_beam import Row
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml import yaml_enrichment
from apache_beam.yaml.yaml_transform import YamlTransform


class _FakeHandler:
  def __init__(self, **kwargs):
    self.kwargs = kwargs


class _FakeEnrichment(beam.PTransform):
  def __init__(self, source_handler, timeout=30):
    self.source_handler = source_handler
    self.timeout = timeout

  def expand(self, pcoll):
    timeout = self.timeout
    return pcoll | beam.Map(
        lambda x: beam.Row(**x._asdict(), enriched_timeout=int(timeout)))


def _enrichment_options():
  return PipelineOptions(
      pickle_library='cloudpickle', yaml_experimental_features=['Enrichment'])


class EnrichmentTransformTest(unittest.TestCase):
  def test_enrichment_with_bigquery(self):
    input_data = [
        Row(label="item1", rank=0),
        Row(label="item2", rank=1),
    ]
    config = {
        "project": "apache-beam-testing",
        "table_name": "project.database.table",
        "row_restriction_template": "label='item1' or label='item2'",
        "fields": ["label"],
    }

    captured = {}

    def make_handler(**kwargs):
      captured['kwargs'] = kwargs
      return _FakeHandler(**kwargs)

    with (mock.patch.object(yaml_enrichment, 'Enrichment', _FakeEnrichment),
          mock.patch.object(yaml_enrichment,
                            'BigQueryEnrichmentHandler',
                            side_effect=make_handler)):
      with beam.Pipeline(options=_enrichment_options()) as p:
        result = (
            p
            | 'CreateInput' >> beam.Create(input_data)
            | YamlTransform(
                f'''
                type: Enrichment
                config:
                  enrichment_handler: BigQuery
                  handler_config: {config}
                '''))
        assert_that(
            result,
            equal_to([
                Row(label="item1", rank=0, enriched_timeout=30),
                Row(label="item2", rank=1, enriched_timeout=30),
            ]))
    self.assertEqual(captured['kwargs'], config)

  def test_enrichment_other_handlers_and_timeout(self):
    for handler_name, attr_name in [
        ('BigTable', 'BigTableEnrichmentHandler'),
        ('VertexAIFeatureStore', 'VertexAIFeatureStoreEnrichmentHandler'),
        ('FeastFeatureStore', 'FeastFeatureStoreEnrichmentHandler'),
    ]:
      with self.subTest(handler=handler_name):
        with (mock.patch.object(yaml_enrichment, 'Enrichment', _FakeEnrichment),
              mock.patch.object(yaml_enrichment, attr_name, _FakeHandler) as
              mock_cls):
          with beam.Pipeline(options=_enrichment_options()) as p:
            result = (
                p
                | beam.Create([Row(id='k1')])
                | yaml_enrichment.enrichment_transform(
                    enrichment_handler=handler_name,
                    handler_config={'row_key': 'id'},
                    timeout=45))
            assert_that(result, equal_to([Row(id='k1', enriched_timeout=45)]))
          self.assertIs(yaml_enrichment.__dict__[attr_name], mock_cls)

  def test_enrichment_requires_experimental_flag(self):
    with self.assertRaisesRegex(ValueError, 'Enrichment.*experimental'):
      with beam.Pipeline() as p:
        _ = p | beam.Create([Row(id='k1')]) | YamlTransform(
            '''
            type: Enrichment
            config:
              enrichment_handler: BigTable
              handler_config:
                row_key: id
            ''')

  def test_enrichment_missing_gcp_dependencies(self):
    with mock.patch.object(yaml_enrichment, 'Enrichment', None):
      with self.assertRaisesRegex(ValueError, 'gcp dependencies not installed'):
        with beam.Pipeline(options=_enrichment_options()) as p:
          _ = (
              p
              | beam.Create([Row(id='k1')])
              | yaml_enrichment.enrichment_transform(
                  enrichment_handler='BigQuery', handler_config={}))

  def test_enrichment_missing_feast_dependency(self):
    with (mock.patch.object(yaml_enrichment, 'Enrichment', _FakeEnrichment),
          mock.patch.object(yaml_enrichment,
                            'FeastFeatureStoreEnrichmentHandler',
                            None)):
      with self.assertRaisesRegex(ValueError, "requires 'feast' package"):
        with beam.Pipeline(options=_enrichment_options()) as p:
          _ = (
              p
              | beam.Create([Row(id='k1')])
              | yaml_enrichment.enrichment_transform(
                  enrichment_handler='FeastFeatureStore', handler_config={}))

  def test_enrichment_unknown_handler(self):
    with mock.patch.object(yaml_enrichment, 'Enrichment', _FakeEnrichment):
      with self.assertRaisesRegex(
          ValueError, 'Unknown enrichment source: UnsupportedSource'):
        with beam.Pipeline(options=_enrichment_options()) as p:
          _ = (
              p
              | beam.Create([Row(id='k1')])
              | yaml_enrichment.enrichment_transform(
                  enrichment_handler='UnsupportedSource', handler_config={}))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
