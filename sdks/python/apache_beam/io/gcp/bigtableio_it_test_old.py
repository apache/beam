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
# import apache_beam.io.gcp.bigtableio as bigtableio
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


# class GenerateTestRows(beam.PTransform):
#   """ A PTransform to generate dummy rows to write to a Bigtable Table.
#
#   A PTransform that generates a list of `DirectRow`
#   and writes it to a Bigtable Table.
#   """
#   def __init__(self):
#     super(self.__class__, self).__init__()
#     self.beam_options = {'project_id': PROJECT_ID,
#                          'instance_id': INSTANCE_ID,
#                          'table_id': TABLE_ID}
#
#   def _generate(self):
#     for i in range(ROW_COUNT):
#       key = "key_%s" % ('{0:012}'.format(i))
#       test_row = row.DirectRow(row_key=key)
#       # test_row_str = 'TEST_ROW[{}] : ['.format(i+1)
#       value = ''.join(
#           random.choice(LETTERS_AND_DIGITS) for _ in range(CELL_SIZE))
#       for j in range(COLUMN_COUNT):
#         test_row.set_cell(column_family_id=COLUMN_FAMILY_ID,
#                           column=('field%s' % j).encode('utf-8'),
#                           value=value,
#                           timestamp=datetime.datetime.now())
#         # test_row_str += value + ', '
#       # logger.debug('{}]'.format(test_row_str[:-2]))
#       yield test_row
#
#   def expand(self, pvalue):
#     return (pvalue
#             | beam.Create(self._generate())
#             | bigtableio.WriteToBigTable(
#                 project_id=self.beam_options['project_id'],
#                 instance_id=self.beam_options['instance_id'],
#                 table_id=self.beam_options['table_id']))
#
# class ReadTestRows(beam.PTransform):
#   def __init__(self):
#     # super(self.__class__, self).__init__()
#     self._beam_options = {'project_id': PROJECT_ID,
#                          'instance_id': INSTANCE_ID,
#                          'table_id': TABLE_ID}
#     self.table = Client(project=self._beam_options['project_id']) \
#       .instance(self._beam_options['instance_id']) \
#       .table(self._beam_options['table_id'])
#
#   def _read_rows(self):
#     for test_row in self.table.read_rows():
#       yield test_row
#
#   def expand(self, pvalue):
#     return (pvalue
#             | beam.Create(self._read_rows())
#             | bigtableio.ReadFromBT(
#                 project_id=self._beam_options['project_id'],
#                 instance_id=self._beam_options['instance_id'],
#                 table_id=self._beam_options['table_id']))


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

    # self.result = None
    #
    # client = Client(project=PROJECT_ID, admin=True)
    # instance = client.instance(instance_id=INSTANCE_ID)
    # self.table = instance.table(table_id=TABLE_ID)
    # self.skip_write_test = False
    #
    # if self.table.exists():
    #   self.skip_write_test = True
    #   logging.info('Table "%s" already exists. Skipping the WRITE test...', TABLE_ID)
    # else:
    #   column_families = {COLUMN_FAMILY_ID: column_family.MaxVersionsGCRule(2)}
    #   self.table.create(column_families=column_families)
    #   logging.info('Table "%s" has been created!', TABLE_ID)
    # # self.skip_write_test = True

  def test_write(self, options):
    pass

  @attr('IT')
  def test_bigtable_io(self):
    # pipeline_args = sys.argv[1:]

    # p_options = PipelineOptions(pipeline_args)

    # TEMPORARY OVERRIDES --- TO BE REMOVED!!! #########################################################################

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
    # END OF TEMPORARY OVERRIDES #######################################################################################

    p_options.view_as(SetupOptions).save_main_session = True

    # if self.skip_write_test:
    #   print('WRITE Test skipped.')
    # else:
    #   print('Starting WRITE test...')
    #   job = '{}-write'.format(JOB_NAME)
    #   p_options.view_as(GoogleCloudOptions).job_name = job
    #   for key, value in p_options.get_all_options().items():
    #     logging.debug('Pipeline option {:32s} : {}'.format(key, value))
    #
    #   p = beam.Pipeline(options=p_options)
    #   _ = (p | 'Write Test Rows' >> GenerateTestRows())
    #
    #   self.result = p.run()
    #   self.result.wait_until_finish()
    #   assert self.result.state == PipelineState.DONE
    #
    #   if not hasattr(self.result, 'has_job') or self.result.has_job:
    #     query_result = self.result.metrics().query(
    #         MetricsFilter().with_name('Written Row'))
    #     if query_result['counters']:
    #       read_counter = query_result['counters'][0]
    #       logging.info('Number of Rows written: %d', read_counter.committed)
    #       assert read_counter.committed == ROW_COUNT
    #   print('WRITE test complete.')

    print('Starting READ test...')
    p_options.view_as(GoogleCloudOptions).job_name = '{}-read'.format(JOB_NAME)
    for key, value in p_options.get_all_options().items():
      logging.debug('Pipeline option {:32s} : {}'.format(key, value))

    # p = beam.Pipeline(options=p_options)

    # New code #########################################################################################################
    # _ = (p | 'Read Test Rows' >> ReadTestRows())

    with beam.Pipeline(options=p_options) as p:
      for key, value in p.options.get_all_options().items():
        logging.info('Pipeline option {:32s} = "{}"'.format(key, value))
      # import apache_beam.io.gcp.bigtableio as bigtableio
      import bigtableio
      _ = (p | 'CBT Read Test' >> bigtableio.ReadFromBigTable(project_id=PROJECT_ID,
                                                              instance_id=INSTANCE_ID,
                                                              table_id=TABLE_ID))
      self.result = p.run()
      self.result.wait_until_finish()
      assert self.result.state == PipelineState.DONE

      if True: # not hasattr(self.result, 'has_job') or self.result.has_job:
        query_result = self.result.metrics().query(
          MetricsFilter().with_name('Rows Read'))
        if query_result['counters']:
          read_counter = query_result['counters'][0]
          logging.info('Number of Rows written: %d', read_counter.committed)
          assert read_counter.committed == ROW_COUNT
      ####################################################################################################################

    # count = (p
    #          | 'Bigtable Read' >> bigtableio.ReadFromBigTable(PROJECT_ID,
    #                                                           INSTANCE_ID,
    #                                                           TABLE_ID,
    #                                                           b'')
    #          | 'Count Rows' >> Count.Globally())
    #
    # self.result = p.run()
    # self.result.wait_until_finish()
    # assert self.result.state == PipelineState.DONE
    # assert_that(count, equal_to([ROW_COUNT]))

    print('ROW_COUNT = {}'.format(ROW_COUNT))
    print('READ test complete.')


if __name__ == '__main__':
  # parser = argparse.ArgumentParser()
  #
  # parser.add_argument('--project', type=str)
  # parser.add_argument('--instance', type=str)
  # parser.add_argument('--table', type=str, default='test-table')
  # parser.add_argument('--job_name', type=str, default='bigtableio-it-test')
  # parser.add_argument('--row_count', type=int, default=10000)
  # parser.add_argument('--column_count', type=int, default=10)
  # parser.add_argument('--cell_size', type=int, default=100)
  # parser.add_argument('--log_level', type=int, default=logging.INFO)
  # parser.add_argument('--log_dir', type=str, default='C:\\cbt\\')
  # parser.add_argument('--runner', type=str, default='direct')
  #
  # args, argv = parser.parse_known_args()
  #
  # PROJECT_ID = args.project
  # INSTANCE_ID = args.instance
  # TABLE_ID = args.table
  # # TABLE_ID = 'test-table-10k-20190918-141848'
  # ROW_COUNT = args.row_count
  # COLUMN_COUNT = args.column_count
  # CELL_SIZE = args.cell_size
  # JOB_NAME = args.job_name

  # TEMPORARY OVERRIDES --- TO BE REMOVED!!! #########################################################################
  import time

  PROJECT_ID = 'grass-clump-479'
  INSTANCE_ID = 'bigtableio-test'
  TIME_STAMP = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S')
  TABLE_ID = 'test-table-1M-20191206-213807'  # 6 keys, 1,000,000 rows
  ROW_COUNT = 1000000  # 'Rows Read' = 1000000
  ROW_COUNT_K = ROW_COUNT // 1000
  JOB_NAME = 'bigtableio-it-test-{}k-rows-{}'.format(
    ROW_COUNT_K,
    TIME_STAMP
  )
  NUM_WORKERS = 50
  STAGING_LOCATION = 'gs://mf2199/stage'
  TEMP_LOCATION = 'gs://mf2199/temp'
  SETUP_FILE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\setup.py'
  EXTRA_PACKAGE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\dist\\bigtableio-0.3.125.tar.gz'
  LOG_LEVEL = logging.INFO
  # END OF TEMPORARY OVERRIDES #######################################################################################

  # logging.getLogger().setLevel(args.log_level)
  logging.getLogger().setLevel(LOG_LEVEL)

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
