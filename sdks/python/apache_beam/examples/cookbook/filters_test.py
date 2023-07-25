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

"""Test for the filters example."""

# pytype: skip-file

import logging
import time
import unittest

import pytest
from hamcrest.core.core.allof import all_of

import apache_beam as beam
from apache_beam.examples.cookbook import filters
from apache_beam.io.gcp.tests import utils
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryMatcher
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class FiltersTest(unittest.TestCase):

  # The default checksum is a SHA-1 hash generated from sorted rows reading
  # from expected Bigquery table.
  DEFAULT_CHECKSUM = '813b6da1624334732fad4467d74a7c8a62559c6b'

  # Note that 'removed' should be projected away by the pipeline
  input_data = [
      {
          'year': 2010, 'month': 1, 'day': 1, 'mean_temp': 3, 'removed': 'a'
      },
      {
          'year': 2012, 'month': 1, 'day': 2, 'mean_temp': 3, 'removed': 'a'
      },
      {
          'year': 2011, 'month': 1, 'day': 3, 'mean_temp': 5, 'removed': 'a'
      },
      {
          'year': 2013, 'month': 2, 'day': 1, 'mean_temp': 3, 'removed': 'a'
      },
      {
          'year': 2011, 'month': 3, 'day': 3, 'mean_temp': 5, 'removed': 'a'
      },
  ]

  def _get_result_for_month(self, pipeline, month):
    rows = (pipeline | 'create' >> beam.Create(self.input_data))
    results = filters.filter_cold_days(rows, month)
    return results

  def test_basics(self):
    """Test that the correct result is returned for a simple dataset."""
    with TestPipeline() as p:
      results = self._get_result_for_month(p, 1)
      assert_that(
          results,
          equal_to([{
              'year': 2010, 'month': 1, 'day': 1, 'mean_temp': 3
          }, {
              'year': 2012, 'month': 1, 'day': 2, 'mean_temp': 3
          }]))

  def test_basic_empty(self):
    """Test that the correct empty result is returned for a simple dataset."""
    with TestPipeline() as p:
      results = self._get_result_for_month(p, 3)
      assert_that(results, equal_to([]))

  def test_basic_empty_missing(self):
    """Test that the correct empty result is returned for a missing month."""
    with TestPipeline() as p:
      results = self._get_result_for_month(p, 4)
      assert_that(results, equal_to([]))

  @pytest.mark.examples_postcommit
  def test_filters_output_bigquery_matcher(self):
    test_pipeline = TestPipeline(is_integration_test=True)

    # Set extra options to the pipeline for test purpose
    project = test_pipeline.get_option('project')

    dataset = 'FiltersTestIT'
    table = 'cold_days_%s' % int(round(time.time() * 1000))
    output_table = '.'.join([dataset, table])
    query = 'SELECT year, month, day, mean_temp FROM `%s`' % output_table

    pipeline_verifiers = [
        PipelineStateMatcher(),
        BigqueryMatcher(
            project=project, query=query, checksum=self.DEFAULT_CHECKSUM)
    ]
    extra_opts = {
        'output': output_table,
        'on_success_matcher': all_of(*pipeline_verifiers)
    }

    # Register cleanup before pipeline execution.
    # Note that actual execution happens in reverse order.
    self.addCleanup(utils.delete_bq_table, project, dataset, table)

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    filters.run(test_pipeline.get_full_options_as_args(**extra_opts))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
