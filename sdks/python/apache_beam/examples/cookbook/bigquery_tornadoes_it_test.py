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

"""End-to-end test for Bigquery tornadoes example."""

from __future__ import absolute_import

import logging
import time
import unittest
from builtins import round

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.examples.cookbook import bigquery_tornadoes
from apache_beam.io.gcp.tests import utils
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryMatcher
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline


class BigqueryTornadoesIT(unittest.TestCase):

  # Enable nose tests running in parallel
  _multiprocess_can_split_ = True

  # The default checksum is a SHA-1 hash generated from sorted rows reading
  # from expected Bigquery table.
  DEFAULT_CHECKSUM = 'd860e636050c559a16a791aff40d6ad809d4daf0'

  @attr('IT')
  def test_bigquery_tornadoes_it(self):
    test_pipeline = TestPipeline(is_integration_test=True)

    # Set extra options to the pipeline for test purpose
    project = test_pipeline.get_option('project')

    dataset = 'BigQueryTornadoesIT'
    table = 'monthly_tornadoes_%s' % int(round(time.time() * 1000))
    output_table = '.'.join([dataset, table])
    query = 'SELECT month, tornado_count FROM `%s`' % output_table

    pipeline_verifiers = [PipelineStateMatcher(),
                          BigqueryMatcher(
                              project=project,
                              query=query,
                              checksum=self.DEFAULT_CHECKSUM)]
    extra_opts = {'output': output_table,
                  'on_success_matcher': all_of(*pipeline_verifiers)}

    # Register cleanup before pipeline execution.
    # Note that actual execution happens in reverse order.
    self.addCleanup(utils.delete_bq_table, project, dataset, table)

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    bigquery_tornadoes.run(
        test_pipeline.get_full_options_as_args(**extra_opts))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
