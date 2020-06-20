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

""" Integration test for GC Bigtable connector [read]."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import unittest

from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.io.gcp.experimental.bigtableio import ReadFromBigtable
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.runner import PipelineState

try:
  from google.cloud.bigtable import Client
except ImportError:
  Client = None


@unittest.skipIf(Client is None, 'GC Bigtable dependencies are not installed')
class BigtableReadTest(unittest.TestCase):
  """ Bigtable Read Connector Test

  This tests the ReadFromBigtable connector class via reading rows from
  a Bigtable table and comparing the `Rows Read` metrics with the row
  count known a priori.
  """
  def setUp(self):
    logging.info('\nProject ID:  %s', options['project'])
    logging.info('\nInstance ID: %s', options['instance'])
    logging.info('\nTable ID:    %s', options['table'])

    self._p_options = PipelineOptions(**options)
    self._p_options.view_as(SetupOptions).save_main_session = True

    # [OPTIONAL] Uncomment this to allow logging the pipeline options
    # for key, value in self.p_options.get_all_options().items():
    #   logging.info('Pipeline option {:32s} : {}'.format(key, value))

  @attr('IT')
  def test_bigtable_read(self):
    logging.info(
        'Reading table "%s" of %d rows...',
        options['table'],
        options['row_count'])

    p = beam.Pipeline(options=self._p_options)
    _ = (
        p | 'Read Test' >> ReadFromBigtable(
            project_id=options['project'],
            instance_id=options['instance'],
            table_id=options['table'],
            filter_=options['filter']))
    self.result = p.run()
    self.result.wait_until_finish()
    assert self.result.state == PipelineState.DONE

    query_result = self.result.metrics().query(
        MetricsFilter().with_name('Rows Read'))

    if query_result['counters']:
      read_counter = query_result['counters'][0]
      final_count = read_counter.committed
      assert final_count == options['row_count']
      logging.info(
          '%d out of %d rows were read successfully.',
          final_count,
          options['row_count'])

    logging.info('DONE!')


def parse_commane_line_arguments():
  parser = argparse.ArgumentParser()

  parser.add_argument('--project', type=str)
  parser.add_argument('--instance', type=str)
  parser.add_argument('--table', type=str)
  parser.add_argument('--filter', type=str, default=None)
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

  args, _ = parser.parse_known_args()

  return {
      'project': args.project,
      'instance': args.instance,
      'table': args.table,
      'filter': args.filter,
      'region': args.region,
      'staging_location': args.staging_location,
      'temp_location': args.temp_location,
      'setup_file': args.setup_file,
      'extra_package': args.extra_package,
      'runner': args.runner,
      'num_workers': args.num_workers,
      'autoscaling_algorithm': args.autoscaling_algorithm,
      'disk_size_gb': args.disk_size_gb,
      'row_count': args.row_count,
      'job_name': args.job_name,
      'log_level': args.log_level,
      'log_directory': args.log_dir,
  }


def setup_log_file():
  if options['log_directory']:
    # logging.basicConfig(
    #   filename='{}{}.log'.format(options['log_directory'], options['table']),
    #   filemode='w',
    #   level=logging.DEBUG
    # )

    # Forward all the logs to a file
    fh = logging.FileHandler(
        filename='{}test_log_{}_RUNNER={}.log'.format(
            options['log_directory'], options['table'][-15:],
            options['runner']))
    fh.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(fh)


if __name__ == '__main__':
  options = parse_commane_line_arguments()

  logging.getLogger().setLevel(options['log_level'])
  # [OPTIONAL] Uncomment to save logs into a file
  # setup_log_file()

  # test_suite = unittest.TestSuite()
  # test_suite.addTest(BigtableReadTest('test_bigtable_read'))
  # unittest.TextTestRunner(verbosity=2).run(test_suite)
  unittest.main()
