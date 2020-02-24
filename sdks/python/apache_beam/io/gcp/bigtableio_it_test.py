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
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
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


import apache_beam.io.gcp.bigtableio as bigtableio


@unittest.skipIf(Client is None, 'GCP Bigtable dependencies are not installed')
class BigtableIOTest(unittest.TestCase):
  """ Bigtable IO Connector Test

  This tests the connector both ways, first writing rows to a new table,
  then reading them and comparing the counters
  """
  def setUp(self):
    logging.info('\nProject ID:  %s' % PROJECT_ID)
    logging.info('\nInstance ID: %s' % INSTANCE_ID)
    logging.info('\nTable ID:    %s' % TABLE_ID)

    client = Client(project=PROJECT_ID, admin=True)
    instance = client.instance(instance_id=INSTANCE_ID)
    self.table = instance.table(table_id=TABLE_ID)

    column_families = {COLUMN_FAMILY_ID: column_family.MaxVersionsGCRule(2)}
    self.table.create(column_families=column_families)
    logging.info('Table "%s" has been created!', TABLE_ID)

  def test_write(self, options):
    pass

  @attr('IT')
  def test_bigtable_io(self):

    p_options = PipelineOptions(
      project=PROJECT_ID,
      instance=INSTANCE_ID,
      table=TABLE_ID,
      runner='dataflow',
      # runner='direct',
      region='us-central1',
      staging_location=STAGING_LOCATION,
      temp_location=TEMP_LOCATION,
      setup_file=SETUP_FILE,
      extra_package=EXTRA_PACKAGE,
      num_workers=NUM_WORKERS,
      autoscaling_algorithm='NONE',
      disk_size_gb=50,
      job_name=JOB_NAME,
      log_level=LOG_LEVEL,
    )
    if True:
      print('Starting WRITE test...')
      p_options.view_as(GoogleCloudOptions).job_name = '{}-write'.format(JOB_NAME)
      for key, value in p_options.get_all_options().items():
        logging.info('Pipeline option {:32s} : {}'.format(key, value))

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
      print('WRITE test complete.')

    print('Starting READ test...')
    p_options.view_as(GoogleCloudOptions).job_name = '{}-read'.format(JOB_NAME)
    for key, value in p_options.get_all_options().items():
      logging.debug('Pipeline option {:32s} : {}'.format(key, value))

    print('ROW_COUNT = {}'.format(ROW_COUNT))
    print('READ test complete.')


if __name__ == '__main__':
  parser = argparse.ArgumentParser()

  parser.add_argument('--project', type=str)
  parser.add_argument('--instance', type=str)
  parser.add_argument('--table', type=str)
  parser.add_argument('--job_name', type=str, default='bigtableio-read-it-test')
  parser.add_argument('--row_count', type=int, default=10000)
  # parser.add_argument('--column_count', type=int, default=10)
  # parser.add_argument('--cell_size', type=int, default=100)
  parser.add_argument('--log_level', type=int, default=logging.INFO)
  parser.add_argument('--log_dir', type=str, default='C:\\cbt\\')
  parser.add_argument('--runner', type=str, default='dataflow')

  args, argv = parser.parse_known_args()

  PROJECT_ID = args.project
  INSTANCE_ID = args.instance
  TABLE_ID = args.table
  ROW_COUNT = args.row_count
  # COLUMN_COUNT = args.column_count
  # CELL_SIZE = args.cell_size
  JOB_NAME = args.job_name


  logging.getLogger().setLevel(args.log_level)
  # logging.getLogger().setLevel(LOG_LEVEL)

  # logging.basicConfig(filename='{}{}.log'.format(args.log_dir, TABLE_ID),
  #                     filemode='w',
  #                     level=logging.DEBUG)

  # # Forward all logs to a file
  # fh = logging.FileHandler(
  #   filename='{}test_log_{}_RUNNER={}.log'.format(args.log_dir,
  #                                                 TABLE_ID[-15:],
  #                                                 args.runner))
  # fh.setLevel(logging.DEBUG)
  # logger.addHandler(fh)

  test_suite = unittest.TestSuite()
  test_suite.addTest(BigtableIOTest('test_bigtable_io'))
  unittest.TextTestRunner(verbosity=2).run(test_suite)
