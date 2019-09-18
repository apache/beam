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

""" Integration test for GCP Bigtable testing."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import datetime
import logging
import random
import string
import sys
import unittest

import apache_beam as beam
import apache_beam.io.gcp.bigtableio as bigtableio
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.combiners import Count

from nose.plugins.attrib import attr

try:
  from google.cloud.bigtable import Client
  from google.cloud.bigtable import column_family
  from google.cloud.bigtable import row
except ImportError:
  Client = None

PROJECT_ID = ''
INSTANCE_ID = ''
TABLE_ID = ''
JOB_NAME = ''
COLUMN_FAMILY_ID = 'cf1'
LETTERS_AND_DIGITS = string.ascii_letters + string.digits


class GenerateTestRows(beam.PTransform):
  """ A PTransform to generate dummy rows to write to a Bigtable Table.

  A PTransform that generates a list of `DirectRow`
  and writes it to a Bigtable Table.
  """
  def __init__(self):
    super(self.__class__, self).__init__()
    self.beam_options = {'project_id': PROJECT_ID,
                         'instance_id': INSTANCE_ID,
                         'table_id': TABLE_ID}

  def _generate(self):
    for i in range(ROW_COUNT):
      key = "key_%s" % ('{0:012}'.format(i))
      test_row = row.DirectRow(row_key=key)
      value = ''.join(
          random.choice(LETTERS_AND_DIGITS) for _ in range(CELL_SIZE))
      for j in range(COLUMN_COUNT):
        test_row.set_cell(column_family_id=COLUMN_FAMILY_ID,
                          column=('field%s' % j).encode('utf-8'),
                          value=value,
                          timestamp=datetime.datetime.now())
      yield test_row

  def expand(self, pvalue):
    return (pvalue
            | beam.Create(self._generate())
            | bigtableio.WriteToBigTable(
                project_id=self.beam_options['project_id'],
                instance_id=self.beam_options['instance_id'],
                table_id=self.beam_options['table_id']))


@unittest.skipIf(Client is None, 'GCP Bigtable dependencies are not installed')
class BigtableIOTest(unittest.TestCase):
  """ Bigtable IO Connector Test

  This tests the connector both ways, first writing rows to a new table,
  then reading them and comparing the counters
  """
  def setUp(self):
    logging.info('\nProject ID:  {}'.format(PROJECT_ID))
    logging.info('\nInstance ID: {}'.format(INSTANCE_ID))
    logging.info('\nTable ID:    {}'.format(TABLE_ID))

    self.result = None
    client = Client(project=PROJECT_ID, admin=True)
    instance = client.instance(instance_id=INSTANCE_ID)
    self.table = instance.table(table_id=TABLE_ID)

    if not self.table.exists():
      column_families = {COLUMN_FAMILY_ID: column_family.MaxVersionsGCRule(2)}
      self.table.create(column_families=column_families)
      logging.info('Table %s has been created!', TABLE_ID)

  @attr('IT')
  def test_bigtable_io(self):
    pipeline_args = sys.argv[1:]

    job = '{}-write'.format(JOB_NAME)
    p_options = PipelineOptions(pipeline_args)
    p_options.view_as(GoogleCloudOptions).job_name = job
    # for key, value in p_options.get_all_options().items():
    #   print('{:32s}: {}'.format(key, value))

    p = beam.Pipeline(options=p_options)
    _ = (p | 'Write Test Rows' >> GenerateTestRows())

    self.result = p.run()
    self.result.wait_until_finish()

    assert self.result.state == PipelineState.DONE

    if not hasattr(self.result, 'has_job') or self.result.has_job:
      query_result = self.result.metrics().query(
          MetricsFilter().with_name('Written Row'))
      if query_result['counters']:
        read_counter = query_result['counters'][0]
        logging.info('Number of Rows written: %d', read_counter.committed)
        assert read_counter.committed == ROW_COUNT

    job = '{}-read'.format(JOB_NAME)
    p_options = PipelineOptions(pipeline_args)
    p_options.view_as(GoogleCloudOptions).job_name = job
    p = beam.Pipeline(options=p_options)

    count = (p
             | 'Bigtable Read' >> bigtableio.ReadFromBigTable(PROJECT_ID,
                                                              INSTANCE_ID,
                                                              TABLE_ID,
                                                              b'')
             | 'Count Rows' >> Count.Globally())
    self.result = p.run()
    self.result.wait_until_finish()
    assert_that(count, equal_to([ROW_COUNT]))


if __name__ == '__main__':
  parser = argparse.ArgumentParser()

  parser.add_argument('--project', type=str)
  parser.add_argument('--instance', type=str)
  parser.add_argument('--table', type=str, default='test-table')
  parser.add_argument('--job_name', type=str, default='bigtableio-it-test')
  parser.add_argument('--row_count', type=int, default=10000)
  parser.add_argument('--column_count', type=int, default=10)
  parser.add_argument('--cell_size', type=int, default=100)
  parser.add_argument('--log_level', type=int, default=logging.INFO)

  args, argv = parser.parse_known_args()

  PROJECT_ID = args.project
  INSTANCE_ID = args.instance
  TABLE_ID = args.table
  ROW_COUNT = args.row_count
  COLUMN_COUNT = args.column_count
  CELL_SIZE = args.cell_size
  JOB_NAME = args.job_name

  # TABLE_ID = 'test-table-10k-20190918-141848'

  logging.getLogger().setLevel(args.log_level)

  suite = unittest.TestSuite()
  suite.addTest(BigtableIOTest('test_bigtable_io'))
  unittest.TextTestRunner(verbosity=2).run(suite)
