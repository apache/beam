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

"""End-to-end tests for the taxiride examples."""

# pytype: skip-file

import logging
import os
import unittest
import uuid

import pandas as pd
import pytest

from apache_beam.examples.dataframe import taxiride
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.testing.test_pipeline import TestPipeline


class TaxirideIT(unittest.TestCase):
  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.outdir = (
        self.test_pipeline.get_option('temp_location') + '/taxiride_it-' +
        str(uuid.uuid4()))
    self.output_path = os.path.join(self.outdir, 'output.csv')

  def tearDown(self):
    FileSystems.delete([self.outdir + '/'])

  @pytest.mark.it_postcommit
  def test_aggregation(self):
    taxiride.run_aggregation_pipeline(
        self.test_pipeline,
        'gs://apache-beam-samples/nyc_taxi/2018/*.csv',
        self.output_path)

    # Verify
    expected = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__),
            'data',
            'taxiride_2018_aggregation_truth.csv'),
        comment='#')
    expected = expected.sort_values('DOLocationID').reset_index(drop=True)

    def read_csv(path):
      with FileSystems.open(path) as fp:
        return pd.read_csv(fp)

    result = pd.concat(
        read_csv(metadata.path) for metadata in FileSystems.match(
            [f'{self.output_path}*'])[0].metadata_list)
    result = result.sort_values('DOLocationID').reset_index(drop=True)

    pd.testing.assert_frame_equal(expected, result)

  @pytest.mark.it_postcommit
  def test_enrich(self):
    # Standard workers OOM with the enrich pipeline
    self.test_pipeline.get_pipeline_options().view_as(
        WorkerOptions).machine_type = 'e2-highmem-2'

    taxiride.run_enrich_pipeline(
        self.test_pipeline,
        'gs://apache-beam-samples/nyc_taxi/2018/*.csv',
        self.output_path)

    # Verify
    expected = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__), 'data',
            'taxiride_2018_enrich_truth.csv'),
        comment='#')
    expected = expected.sort_values('Borough').reset_index(drop=True)

    def read_csv(path):
      with FileSystems.open(path) as fp:
        return pd.read_csv(fp)

    result = pd.concat(
        read_csv(metadata.path) for metadata in FileSystems.match(
            [f'{self.output_path}*'])[0].metadata_list)
    result = result.sort_values('Borough').reset_index(drop=True)

    pd.testing.assert_frame_equal(expected, result)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
