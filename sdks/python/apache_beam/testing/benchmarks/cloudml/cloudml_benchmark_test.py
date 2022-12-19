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
import os

import unittest

import apache_beam.testing.benchmarks.cloudml.cloudml_benchmark_constants_lib as lib
from apache_beam.testing.benchmarks.cloudml.pipelines import workflow
from apache_beam.testing.test_pipeline import TestPipeline

_INPUT_GCS_BUCKET_ROOT = 'gs://cloud-ml-benchmark-data-us-east/cloudml/'
_CRITEO_FEATURES_FILE = 'testdata/criteo/expected/features.tfrecord.gz'


class CloudMLTFTBenchmarkTest(unittest.TestCase):
  """
  TODOs:
  1. Add pipeline names
  2. Make sure shuffle flags are applied correctly.
  3. Add assertions where its applicable.
  """
  def test_cloudml_criteo_small(self):
    test_pipeline = TestPipeline(is_integration_test=False)
    extra_opts = {}
    extra_opts['input'] = os.path.join(
        _INPUT_GCS_BUCKET_ROOT, lib.INPUT_CRITEO_SMALL)
    extra_opts['output'] = '/tmp/cloudML'
    extra_opts['benchmark_type'] = 'tft'
    extra_opts['classifier'] = 'criteo'
    # extra_opts['timeout'] = 3600
    extra_opts['frequency_threshold'] = lib.FREQUENCY_THRESHOLD
    extra_opts['shuffle'] = lib.ENABLE_SHUFFLE

    workflow.run(test_pipeline.get_full_options_as_args(**extra_opts))
    # Add assertion

  def test_cloudml_benchmark_criteo_small(self):
    test_pipeline = TestPipeline(is_integration_test=False)
    extra_opts = {}
    extra_opts['input'] = os.path.join(
        _INPUT_GCS_BUCKET_ROOT, lib.INPUT_CRITEO_SMALL)
    extra_opts['benchmark_type'] = 'tft'
    extra_opts['classifier'] = 'criteo'
    extra_opts['frequency_threshold'] = 0
    extra_opts['output'] = '/tmp/cloudML/1'
    workflow.run(test_pipeline.get_full_options_as_args(**extra_opts))
    # add assertion

  def test_cloudml_benchmark_cirteo_no_shuffle_10GB(self):
    test_pipeline = TestPipeline(is_integration_test=False)
    extra_opts = {}
    extra_opts['input'] = os.path.join(
        _INPUT_GCS_BUCKET_ROOT, lib.INPUT_CRITEO_10GB)
    extra_opts['benchmark_type'] = 'tft'
    extra_opts['classifier'] = 'criteo'
    extra_opts['frequency_threshold'] = 0
    extra_opts['output'] = '/tmp/cloudML/1'
    # verify shuffle and shuffle service pipeline option
    extra_opts['shuffle'] = False
    extra_opts['timeout'] = 5400

    workflow.run(test_pipeline.get_full_options_as_args(**extra_opts))

  def test_cloudml_benchmark_criteo_10GB(self):
    extra_opts = {}
    extra_opts['input'] = os.path.join(
        _INPUT_GCS_BUCKET_ROOT, lib.INPUT_CRITEO_10GB)
    extra_opts['benchmark_type'] = 'tft'
    extra_opts['classifier'] = 'criteo'
    extra_opts['frequency_threshold'] = 0
    extra_opts['output'] = '/tmp/cloudML/1'
    extra_opts['timeout'] = 5400

  def test_cloud_ml_benchmark_criteo_fixed_10GB(self):
    extra_opts = {}
    extra_opts['input'] = os.path.join(
        _INPUT_GCS_BUCKET_ROOT, lib.INPUT_CRITEO_10GB)
    extra_opts['benchmark_type'] = 'tft'
    extra_opts['classifier'] = 'criteo'
    extra_opts['frequency_threshold'] = 0
    extra_opts['output'] = '/tmp/cloudML/1'
    extra_opts['timeout'] = 5400
    extra_opts['num_workers'] = 50


if __name__ == '__main__':
  unittest.main()
