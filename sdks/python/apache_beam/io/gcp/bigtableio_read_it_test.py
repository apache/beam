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
import logging
import unittest

import apache_beam as beam
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineState

from nose.plugins.attrib import attr

try:
  from google.cloud.bigtable import Client
  from google.cloud.bigtable import column_family
  from google.cloud.bigtable import row
except ImportError:
  Client = None

from bigtableio_read import ReadFromBigtable


@unittest.skipIf(Client is None, 'GCP Bigtable dependencies are not installed')
class BigtableReadTest(unittest.TestCase):
  """ Bigtable Read Connector Test

  This tests the connector both ways, first writing rows to a new table,
  then reading them and comparing the counters
  """
  def setUp(self):
    logging.info('\nProject ID:  %s' % PROJECT_ID)
    logging.info('\nInstance ID: %s' % INSTANCE_ID)
    logging.info('\nTable ID:    %s' % TABLE_ID)

  @attr('IT')
  def test_bigtable_read(self):

    p_options = PipelineOptions(
      project=PROJECT_ID,
      instance=INSTANCE_ID,
      table=TABLE_ID,
      runner=RUNNER,
      region=REGION,
      staging_location=STAGING_LOCATION,
      temp_location=TEMP_LOCATION,
      setup_file=SETUP_FILE,
      extra_package=EXTRA_PACKAGE,
      num_workers=NUM_WORKERS,
      autoscaling_algorithm=AUTOSCALING_ALGORITHM,
      disk_size_gb=DISK_SIZE_GB,
      job_name=JOB_NAME,
      log_level=LOG_LEVEL,
    )
    p_options.view_as(SetupOptions).save_smain_session = True

    for key, value in p_options.get_all_options().items():
      logging.info('Pipeline option {:32s} : {}'.format(key, value))

    logging.info('Attempting to read from CBT table "{}"...'.format(TABLE_ID))

    p = beam.Pipeline(options=p_options)

    _ = (p | 'Read Test' >> ReadFromBigtable(project_id=PROJECT_ID,
                                             instance_id=INSTANCE_ID,
                                             table_id=TABLE_ID))

    self.result = p.run()
    self.result.wait_until_finish()
    assert self.result.state == PipelineState.DONE

    query_result = self.result.metrics()\
      .query(MetricsFilter().with_name('Rows Read'))
    if query_result['counters']:
      read_counter = query_result['counters'][0]
      final_count = read_counter.committed
      assert final_count == ROW_COUNT
      logging.info('{} rows were read successfully'.format(final_count))

    logging.info('ROW_COUNT = {}'.format(ROW_COUNT))
    logging.info('DONE!')

    print('READ test complete.')


if __name__ == '__main__':
  parser = argparse.ArgumentParser()

  parser.add_argument('--project', type=str)
  parser.add_argument('--instance', type=str)
  parser.add_argument('--table', type=str)
  parser.add_argument('--region', type=str)
  parser.add_argument('--setup_file', type=str)
  parser.add_argument('--extra_package', type=str)
  parser.add_argument('--staging_location', type=str)
  parser.add_argument('--temp_location', type=str)

  parser.add_argument('--runner', type=str, default='dataflow')
  parser.add_argument('--num_workers', type=int, default=50)
  parser.add_argument('--autoscaling_algorithm', type=str, default='NONE')
  parser.add_argument('--disk_size_gb', type=int, default=50)

  parser.add_argument('--job_name', type=str, default='bigtable-read-it-test')
  parser.add_argument('--row_count', type=int)
  parser.add_argument('--log_level', type=int, default=logging.INFO)
  parser.add_argument('--log_dir', type=str)

  args, argv = parser.parse_known_args()

  PROJECT_ID = args.project
  INSTANCE_ID = args.instance
  TABLE_ID = args.table
  REGION = args.region
  STAGING_LOCATION = args.staging_location
  TEMP_LOCATION = args.temp_location

  RUNNER = args.runner
  NUM_WORKERS = args.num_workers
  AUTOSCALING_ALGORITHM = args.autoscaling_algorithm
  DISK_SIZE_GB = args.disk_size_gb

  ROW_COUNT = args.row_count
  SETUP_FILE = args.setup_file
  EXTRA_PACKAGE = args.extra_package

  JOB_NAME = args.job_name
  LOG_LEVEL = args.log_level
  LOG_DIR = args.log_dir

  logging.getLogger().setLevel(LOG_LEVEL)

  # [OPTIONAL] Uncomment the following lines to save the log data into a file
  # if LOG_DIR:
  #   # logging.basicConfig(filename='{}{}.log'.format(args.log_dir, TABLE_ID),
  #   #                     filemode='w',
  #   #                     level=logging.DEBUG)
  #
  #   # Forward all logs to a file
  #   fh = logging.FileHandler(
  #     filename='{}test_log_{}_RUNNER={}.log'.format(args.log_dir,
  #                                                   TABLE_ID[-15:],
  #                                                   args.runner))
  #   fh.setLevel(logging.DEBUG)
  #   logging.getLogger().addHandler(fh)

  test_suite = unittest.TestSuite()
  test_suite.addTest(BigtableReadTest('test_bigtable_read'))
  unittest.TextTestRunner(verbosity=2).run(test_suite)
