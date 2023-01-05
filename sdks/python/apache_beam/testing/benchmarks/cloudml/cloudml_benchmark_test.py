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
import uuid

import pytest

try:
  import apache_beam.testing.benchmarks.cloudml.cloudml_benchmark_constants_lib as lib
  from apache_beam.testing.benchmarks.cloudml.pipelines import workflow
  from apache_beam.testing.test_pipeline import TestPipeline
except ImportError:  # pylint: disable=bare-except
  raise unittest.SkipTest('Dependencies are not installed')

_INPUT_GCS_BUCKET_ROOT = 'gs://apache-beam-ml/datasets/cloudml/criteo'
_CRITEO_FEATURES_FILE = 'testdata/criteo/expected/features.tfrecord.gz'
_OUTPUT_GCS_BUCKET_ROOT = 'gs://temp-storage-for-end-to-end-tests/tft/'


@pytest.mark.uses_tft
class CloudMLTFTBenchmarkTest(unittest.TestCase):
  def test_cloudml_benchmark_criteo_small(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    extra_opts = {}
    extra_opts['input'] = os.path.join(
        _INPUT_GCS_BUCKET_ROOT, lib.INPUT_CRITEO_SMALL)
    extra_opts['benchmark_type'] = 'tft'
    extra_opts['classifier'] = 'criteo'
    extra_opts['frequency_threshold'] = 0
    extra_opts['output'] = os.path.join(
        _OUTPUT_GCS_BUCKET_ROOT, uuid.uuid4().hex)
    workflow.run(test_pipeline.get_full_options_as_args(**extra_opts))

  def test_cloudml_benchmark_cirteo_no_shuffle_10GB(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    extra_opts = {}
    extra_opts['input'] = os.path.join(
        _INPUT_GCS_BUCKET_ROOT, lib.INPUT_CRITEO_10GB)
    extra_opts['benchmark_type'] = 'tft'
    extra_opts['classifier'] = 'criteo'
    extra_opts['frequency_threshold'] = 0
    extra_opts['output'] = os.path.join(
        _OUTPUT_GCS_BUCKET_ROOT, uuid.uuid4().hex)
    extra_opts['shuffle'] = False
    workflow.run(test_pipeline.get_full_options_as_args(**extra_opts))

  def test_cloudml_benchmark_criteo_10GB(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    extra_opts = {}
    extra_opts['input'] = os.path.join(
        _INPUT_GCS_BUCKET_ROOT, lib.INPUT_CRITEO_10GB)
    extra_opts['benchmark_type'] = 'tft'
    extra_opts['classifier'] = 'criteo'
    extra_opts['frequency_threshold'] = 0
    extra_opts['output'] = os.path.join(
        _OUTPUT_GCS_BUCKET_ROOT, uuid.uuid4().hex)
    workflow.run(test_pipeline.get_full_options_as_args(**extra_opts))

  def test_cloud_ml_benchmark_criteo_fixed_workers_10GB(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    extra_opts = {}
    extra_opts['input'] = os.path.join(
        _INPUT_GCS_BUCKET_ROOT, lib.INPUT_CRITEO_10GB)
    extra_opts['benchmark_type'] = 'tft'
    extra_opts['classifier'] = 'criteo'
    extra_opts['frequency_threshold'] = 0
    extra_opts['output'] = os.path.join(
        _OUTPUT_GCS_BUCKET_ROOT, uuid.uuid4().hex)
    extra_opts['num_workers'] = 50
    extra_opts['machine_type'] = 'n1-standard-4'
    workflow.run(test_pipeline.get_full_options_as_args(**extra_opts))


if __name__ == '__main__':
  unittest.main()
