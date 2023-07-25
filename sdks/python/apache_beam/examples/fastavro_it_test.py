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

"""End-to-end test for Avro IO's fastavro support.

Writes a configurable number of records to a temporary location with fastavro,
then reads them back in from source, joins the generated records and records
that are read from the source, and verifies they have the same elements.



Usage:

  DataFlowRunner:
    pytest apache_beam/examples/fastavro_it_test.py \
        --test-pipeline-options="
          --runner=TestDataflowRunner
          --project=...
          --region=...
          --staging_location=gs://...
          --temp_location=gs://...
          --output=gs://...
          --sdk_location=...
        "

  DirectRunner:
    pytest apache_beam/examples/fastavro_it_test.py \
      --test-pipeline-options="
        --output=/tmp
        --records=5000
      "
"""

# pytype: skip-file

import json
import logging
import unittest
import uuid

import pytest
from fastavro import parse_schema

from apache_beam.io.avroio import ReadAllFromAvro
from apache_beam.io.avroio import WriteToAvro
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import delete_files
from apache_beam.testing.util import BeamAssertException
from apache_beam.transforms.core import Create
from apache_beam.transforms.core import FlatMap
from apache_beam.transforms.core import Map
from apache_beam.transforms.util import CoGroupByKey

LABELS = ['abc', 'def', 'ghi', 'jkl', 'mno', 'pqr', 'stu', 'vwx']
COLORS = ['RED', 'ORANGE', 'YELLOW', 'GREEN', 'BLUE', 'PURPLE', None]


def record(i):
  return {
      'label': LABELS[i % len(LABELS)],
      'number': i,
      'number_str': str(i),
      'color': COLORS[i % len(COLORS)]
  }


def assertEqual(l, r):
  if l != r:
    raise BeamAssertException('Assertion failed: %s == %s' % (l, r))


def check(element):
  assert element['color'] in COLORS
  assert element['label'] in LABELS
  assertEqual(
      sorted(element.keys()), ['color', 'label', 'number', 'number_str'])


class FastavroIT(unittest.TestCase):

  SCHEMA_STRING = '''
    {"namespace": "example.avro",
     "type": "record",
     "name": "User",
     "fields": [
         {"name": "label", "type": "string"},
         {"name": "number",  "type": ["int", "null"]},
         {"name": "number_str", "type": ["string", "null"]},
         {"name": "color", "type": ["string", "null"]}
     ]
    }
    '''

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.uuid = str(uuid.uuid4())
    self.output = '/'.join([self.test_pipeline.get_option('output'), self.uuid])

  @pytest.mark.it_postcommit
  def test_avro_it(self):
    num_records = self.test_pipeline.get_option('records')
    num_records = int(num_records) if num_records else 1000000
    fastavro_output = '/'.join([self.output, 'fastavro'])

    # Seed a `PCollection` with indices that will each be FlatMap'd into
    # `batch_size` records, to avoid having a too-large list in memory at
    # the outset
    batch_size = self.test_pipeline.get_option('batch-size')
    batch_size = int(batch_size) if batch_size else 10000

    # pylint: disable=bad-option-value
    batches = range(int(num_records / batch_size))

    def batch_indices(start):
      # pylint: disable=bad-option-value
      return range(start * batch_size, (start + 1) * batch_size)

    # A `PCollection` with `num_records` avro records
    records_pcoll = \
        self.test_pipeline \
        | 'create-batches' >> Create(batches) \
        | 'expand-batches' >> FlatMap(batch_indices) \
        | 'create-records' >> Map(record)

    # pylint: disable=expression-not-assigned
    records_pcoll \
    | 'write_fastavro' >> WriteToAvro(
        fastavro_output,
        parse_schema(json.loads(self.SCHEMA_STRING)),
    )
    result = self.test_pipeline.run()
    result.wait_until_finish()
    fastavro_pcoll = self.test_pipeline \
                     | 'create-fastavro' >> Create(['%s*' % fastavro_output]) \
                     | 'read-fastavro' >> ReadAllFromAvro()

    mapped_fastavro_pcoll = fastavro_pcoll | "map_fastavro" >> Map(
        lambda x: (x['number'], x))
    mapped_record_pcoll = records_pcoll | "map_record" >> Map(
        lambda x: (x['number'], x))

    def validate_record(elem):
      v = elem[1]

      def assertEqual(l, r):
        if l != r:
          raise BeamAssertException('Assertion failed: %s == %s' % (l, r))

      assertEqual(sorted(v.keys()), ['fastavro', 'record_pcoll'])
      record_pcoll_values = v['record_pcoll']
      fastavro_values = v['fastavro']
      assertEqual(record_pcoll_values, fastavro_values)
      assertEqual(len(record_pcoll_values), 1)

    {
        "record_pcoll": mapped_record_pcoll, "fastavro": mapped_fastavro_pcoll
    } | CoGroupByKey() | Map(validate_record)

    result = self.test_pipeline.run()
    result.wait_until_finish()

    self.addCleanup(delete_files, [self.output])
    assert result.state == PipelineState.DONE


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
