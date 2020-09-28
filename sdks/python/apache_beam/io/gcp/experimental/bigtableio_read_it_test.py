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

from apache_beam.io.gcp.experimental.bigtableio import ReadFromBigtable
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.test_pipeline import TestPipeline
from nose.plugins.attrib import attr


class BigtableReadTest(unittest.TestCase):
  """ Bigtable Read Connector Test

  This tests the ReadFromBigtable connector class via reading rows from
  a Bigtable table and comparing the `Rows Read` metrics with the row
  count known a priori.
  """
  def setUp(self):
    self.log_level = logging.INFO
    logging.getLogger().setLevel(self.log_level)

  @attr('IT')
  def test_bigtable_read(self):
    logging.info(
        'Reading table "%s" of %d rows...' %
        (options['table'], options['row_count']))

    p = TestPipeline(is_integration_test=True)
    project = p.get_pipeline_options().get_all_options()['project']

    logging.info('\nProject ID:  %s' % project)
    logging.info('\nInstance ID: %s' % options['instance'])
    logging.info('\nTable ID:    %s' % options['table'])

    _ = (
        p | 'Read Test' >> ReadFromBigtable(
            project_id=project,
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
          '%d out of %d rows were read successfully.' %
          (final_count, options['row_count']))

    logging.info('DONE!')


if __name__ == '__main__':
  parser = argparse.ArgumentParser()

  parser.add_argument('--instance', type=str)
  parser.add_argument('--table', type=str)
  parser.add_argument('--filter', type=str, default=None)
  parser.add_argument('--row_count', type=int)

  args, argv = parser.parse_known_args()

  options = {
      'instance': args.instance,
      'table': args.table,
      'filter': args.filter,
      'row_count': args.row_count,
  }

  test_suite = unittest.TestSuite()
  test_suite.addTest(BigtableReadTest('test_bigtable_read'))
  unittest.TextTestRunner(verbosity=2).run(test_suite)
