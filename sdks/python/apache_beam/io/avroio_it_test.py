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

"""End-to-end test for Avro IO's.

Integration test for Avro IO. Using both the avro and fastavro
libraries, this test writes avro files, reads them back in,
and combines the different fields.

Usage:

  DataFlowRunner:
    python setup.py nosetests --tests apache_beam.io.avroio_it_test\
        --test-pipeline-options="
          --runner=TestDataflowRunner
          --project=...
          --staging_location=gs://...
          --temp_location=gs://...
          --output=gs://...
          --sdk_location=...
        "

  DirectRunner:
    python setup.py nosetests --tests apache_beam.io.avroio_it_test \
      --test-pipeline-options="
        --output=/tmp
      "
"""

from __future__ import absolute_import
from __future__ import division

import json
import logging
import os
import sys
import unittest
import uuid

from fastavro import parse_schema
from nose.plugins.attrib import attr

from apache_beam.io.avroio import ReadFromAvro
from apache_beam.io.avroio import WriteToAvro
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import delete_files
from apache_beam.testing.util import BeamAssertException
from apache_beam.transforms import CombineGlobally
from apache_beam.transforms.combiners import Count
from apache_beam.transforms.combiners import Mean
from apache_beam.transforms.combiners import ToList
from apache_beam.transforms.core import Create
from apache_beam.transforms.core import FlatMap
from apache_beam.transforms.core import Map

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from avro.schema import Parse  # avro-python3 library for python3
except ImportError:
  from avro.schema import parse as Parse  # avro library for python2
# pylint: enable=wrong-import-order, wrong-import-position

LABELS = ['abc', 'def', 'ghi', 'jkl', 'mno', 'pqr', 'stu', 'vwx']
COLORS = ['RED', 'ORANGE', 'YELLOW', 'GREEN', 'BLUE', 'PURPLE', None]


def record(i):
  return {
      'label': LABELS[i % len(LABELS)],
      'number': i,
      'number_str': str(i),
      'color': COLORS[i % len(COLORS)]
  }


class AvroITTestBase(object):

  def setUp(self):
    self.SCHEMA_STRING = '''
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
    self.uuid = str(uuid.uuid4())

  @attr('IT')
  def avro_it_test(self):
    write_pipeline = TestPipeline(is_integration_test=True)
    read_pipeline = TestPipeline(is_integration_test=True)
    output = '/'.join([write_pipeline.get_option('output'),
                       self.uuid, 'avro-files'])
    files_output = '/'.join([output, 'avro'])

    num_records = write_pipeline.get_option('records')
    num_records = int(num_records) if num_records else 10000

    # Seed a `PCollection` with indices that will each be FlatMap'd into
    # `batch_size` records, to avoid having a too-large list in memory at
    # the outset
    batch_size = write_pipeline.get_option('batch-size')
    batch_size = int(batch_size) if batch_size else 1000

    assert batch_size < num_records
    # pylint: disable=range-builtin-not-iterating
    batches = range(int(num_records / batch_size))

    def batch_indices(start):
      # pylint: disable=range-builtin-not-iterating
      return range(start * batch_size, (start + 1) * batch_size)

    multiplier = 10
    assert (multiplier % 2 == 0), \
      "Use an even multiplier to not compare floats."

    # A `PCollection` with `num_records` avro records
    _ = (write_pipeline
         | 'CreateBatches' >> Create(batches)
         | 'ExpandBatches' >> FlatMap(batch_indices)
         | 'Multiply' >> Map(lambda x: multiplier * x)
         | 'CreateRecords' >> Map(record)
         | 'WriteAvro' >> WriteToAvro(files_output,
                                      self.SCHEMA,
                                      file_name_suffix=".avro",
                                      use_fastavro=self.use_fastavro)
        )

    write_result = write_pipeline.run()
    write_result.wait_until_finish()
    assert write_result.state == PipelineState.DONE

    def check(x, mean):
      if not x == mean:
        raise BeamAssertException('Assertion failed: %s == %s' % (x, mean))

    def check_distr(lst):
      if not len(set(lst)) == 1:
        raise BeamAssertException(
            'Labels should be equally distributed: %s ' % (lst))

    def check_concat(concatenated_string):
      split_concatenated_string = concatenated_string.split(" ")
      for i in range(len(batches) * batch_size):
        if str(multiplier * i) not in split_concatenated_string:
          raise BeamAssertException(
              'String should be in concatenation: %s' % str(multiplier * i))

    def check_for_none(lst):
      if not None in lst:
        raise BeamAssertException('None value was not preserved: %s ' % (lst))

    def concat_strings(values):
      return " ".join(values)

    read_pcol = (read_pipeline
                 | 'ReadAvro' >> ReadFromAvro(files_output + '*')
                )

    _ = (read_pcol
         | 'ExtractLabel' >> Map(lambda x: x['label'])
         | 'MakeLabelsKV' >> Map(lambda x: (x, 1))
         | 'CountLabels' >> Count.PerKey()
         | 'ExtractLabelCounts' >> Map(lambda x: x[1])
         | 'LabelsToList' >> ToList()
         | 'CheckEqualDistributionLabels' >> Map(lambda x: check_distr(x))
        )

    _ = (read_pcol
         | 'ExtractNumber' >> Map(lambda x: x['number'])
         | 'CalculateMean' >> Mean.Globally()
         | 'MeanIsCorrect' >> Map(
             lambda x: check(x, multiplier/2 *
                             ((len(batches) * batch_size) - 1)))
        )

    _ = (read_pcol
         | 'ExtractNumberStr' >> Map(lambda x: x['number_str'])
         | 'Concatenate' >> CombineGlobally(concat_strings)
         | 'CheckConcatenation' >> Map(lambda x: check_concat(x))
        )

    _ = (read_pcol
         | 'ExtractNumberColor' >> Map(lambda x: x['color'])
         | 'MakeColorsKV' >> Map(lambda x: (x, 1))
         | 'CountColors' >> Count.PerKey()
         | 'ExtractColorCounts' >> Map(lambda x: x[0])
         | 'ColorsToList' >> ToList()
         | 'CheckPreservationOfNone' >> Map(lambda x: check_for_none(x))
        )

    self.addCleanup(delete_files, [output])
    read_result = read_pipeline.run()
    read_result.wait_until_finish()


@unittest.skipIf(sys.version_info[0] >= 3 and
                 os.environ.get('RUN_SKIPPED_PY3_TESTS') != '1',
                 'Due to a known issue in avro-python3 package, this '
                 'test is skipped until BEAM-6522 is addressed. ')
class AvroITTest(AvroITTestBase, unittest.TestCase):
  def setUp(self):
    super(AvroITTest, self).setUp()
    self.SCHEMA = Parse(self.SCHEMA_STRING)
    self.use_fastavro = False


class FastAvroITTest(AvroITTestBase, unittest.TestCase):
  def setUp(self):
    super(FastAvroITTest, self).setUp()
    self.SCHEMA = parse_schema(json.loads(self.SCHEMA_STRING))
    self.use_fastavro = True


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
