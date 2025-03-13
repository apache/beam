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
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml.yaml_transform import YamlTransform


class FakeEnrichmentTransform:
  def __init__(self, enrichment_handler, handler_config, timeout=30):
    self._enrichment_handler = enrichment_handler
    self._handler_config = handler_config
    self._timeout = timeout

  def __call__(self, enrichment_handler, *, handler_config, timeout=30):
    assert enrichment_handler == self._enrichment_handler
    assert handler_config == self._handler_config
    assert timeout == self._timeout
    return beam.Map(lambda x: beam.Row(**x._asdict()))


class EnrichmentTransformTest(unittest.TestCase):
  def test_enrichment_with_bigquery(self):
    input_data = [
        Row(label="item1", rank=0),
        Row(label="item2", rank=1),
    ]

    handler = 'BigQuery'
    config = {
        "project": "apache-beam-testing",
        "table_name": "project.database.table",
        "row_restriction_template": "label='item1' or label='item2'",
        "fields": ["label"]
    }

    with beam.Pipeline() as p:
      with mock.patch('apache_beam.yaml.yaml_enrichment.enrichment_transform',
                      FakeEnrichmentTransform(enrichment_handler=handler,
                                              handler_config=config)):
        input_pcoll = p | 'CreateInput' >> beam.Create(input_data)
        result = input_pcoll | YamlTransform(
            f'''
                    type: Enrichment
                    config:
                        enrichment_handler: {handler}
                        handler_config: {config}
                    ''')
        assert_that(result, equal_to(input_data))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
