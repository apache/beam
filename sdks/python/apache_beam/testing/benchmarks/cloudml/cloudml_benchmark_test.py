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
import time
import unittest
import uuid

import pytest

try:
  import apache_beam.testing.benchmarks.cloudml.cloudml_benchmark_constants_lib as lib
  from apache_beam.testing.benchmarks.cloudml.pipelines import workflow
  from apache_beam.testing.load_tests.load_test_metrics_utils import InfluxDBMetricsPublisherOptions
  from apache_beam.testing.load_tests.load_test_metrics_utils import MetricsReader
  from apache_beam.testing.test_pipeline import TestPipeline
except ImportError:  # pylint: disable=bare-except
  raise unittest.SkipTest('Dependencies are not installed')

_INPUT_GCS_BUCKET_ROOT = 'gs://apache-beam-ml/datasets/cloudml/criteo'
_OUTPUT_GCS_BUCKET_ROOT = 'gs://temp-storage-for-end-to-end-tests/tft/'


def _publish_metrics(pipeline, metric_value, metrics_table, metric_name):
  influx_options = InfluxDBMetricsPublisherOptions(
      metrics_table,
      pipeline.get_option('influx_db_name'),
      pipeline.get_option('influx_hostname'),
      os.getenv('INFLUXDB_USER'),
      os.getenv('INFLUXDB_USER_PASSWORD'),
  )
  metric_reader = MetricsReader(
      project_name=pipeline.get_option('project'),
      bq_table=metrics_table,
      bq_dataset=pipeline.get_option('metrics_dataset'),
      publish_to_bq=True,
      influxdb_options=influx_options,
  )
  metric_reader.publish_values([(
      metric_name,
      metric_value,
  )])


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
    start_time = time.time()
    workflow.run(test_pipeline.get_full_options_as_args(**extra_opts))
    end_time = time.time()

    metrics_table = 'cloudml_benchmark_criteo_small'
    _publish_metrics(
        pipeline=test_pipeline,
        metric_value=end_time - start_time,
        metrics_table=metrics_table,
        metric_name='runtime_sec')

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
    start_time = time.time()
    workflow.run(test_pipeline.get_full_options_as_args(**extra_opts))
    end_time = time.time()

    metrics_table = 'cloudml_benchmark_cirteo_no_shuffle_10GB'
    _publish_metrics(
        pipeline=test_pipeline,
        metric_value=end_time - start_time,
        metrics_table=metrics_table,
        metric_name='runtime_sec')

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
    start_time = time.time()
    workflow.run(test_pipeline.get_full_options_as_args(**extra_opts))
    end_time = time.time()

    metrics_table = 'cloudml_benchmark_criteo_10GB'
    _publish_metrics(
        pipeline=test_pipeline,
        metric_value=end_time - start_time,
        metrics_table=metrics_table,
        metric_name='runtime_sec')


if __name__ == '__main__':
  unittest.main()
