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

"""A Dataflow job that counts the number of rows in a BQ table.

   Can be configured to simulate slow reading for a given number of rows.
"""

# pytype: skip-file

import logging
import unittest

import pytest
from hamcrest.core.core.allof import all_of

from apache_beam.io.gcp import bigquery_io_read_pipeline
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline


class BigqueryIOReadIT(unittest.TestCase):

  DEFAULT_DATASET = "big_query_import_export"
  DEFAULT_TABLE_PREFIX = "export_"
  NUM_RECORDS = {
      "empty": 0,
      "1M": 10592,
      "1G": 11110839,
      "1T": 11110839000,
  }

  def run_bigquery_io_read_pipeline(self, input_size, beam_bq_source=False):
    test_pipeline = TestPipeline(is_integration_test=True)
    pipeline_verifiers = [
        PipelineStateMatcher(),
    ]
    extra_opts = {
        'input_table': self.DEFAULT_DATASET + "." + self.DEFAULT_TABLE_PREFIX +
        input_size,
        'num_records': self.NUM_RECORDS[input_size],
        'beam_bq_source': str(beam_bq_source),
        'on_success_matcher': all_of(*pipeline_verifiers)
    }
    bigquery_io_read_pipeline.run(
        test_pipeline.get_full_options_as_args(**extra_opts))

  @pytest.mark.it_postcommit
  def test_bigquery_read_custom_1M_python(self):
    self.run_bigquery_io_read_pipeline('1M', True)

  @pytest.mark.it_postcommit
  def test_bigquery_read_1M_python(self):
    self.run_bigquery_io_read_pipeline('1M')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
