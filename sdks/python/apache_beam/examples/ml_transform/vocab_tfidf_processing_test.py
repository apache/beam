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

import logging
import os
import time
import unittest
import uuid

import pytest

try:
  from apache_beam.examples.ml_transform import vocab_tfidf_processing
  from apache_beam.testing.load_tests.load_test_metrics_utils import InfluxDBMetricsPublisherOptions
  from apache_beam.testing.load_tests.load_test_metrics_utils import MetricsReader
  from apache_beam.testing.test_pipeline import TestPipeline
except ImportError:  # pylint: disable=bare-except
  raise unittest.SkipTest('tensorflow_transform is not installed.')


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
class LargeMovieReviewDatasetProcessTest(unittest.TestCase):
  def test_process_large_movie_review_dataset(self):
    input_data_dir = 'gs://apache-beam-ml/datasets/aclImdb'
    artifacts_location = os.path.join(
        'gs://temp-storage-for-end-to-end-tests/tft/artifacts',
        uuid.uuid4().hex)
    output_data_dir = os.path.join(
        'gs://temp-storage-for-end-to-end-tests/tft/output', uuid.uuid4().hex)
    extra_opts = {
        'input_data_dir': input_data_dir,
        'output_data_dir': output_data_dir,
        'artifacts_location': artifacts_location,
    }

    test_pipeline = TestPipeline(is_integration_test=True)
    start_time = time.time()
    _ = vocab_tfidf_processing.run(
        test_pipeline.get_full_options_as_args(
            **extra_opts, save_main_session=False),
    )
    end_time = time.time()
    metrics_table = 'ml_transform_large_movie_review_dataset_process_metrics'
    _publish_metrics(
        pipeline=test_pipeline,
        metric_value=end_time - start_time,
        metrics_table=metrics_table,
        metric_name='runtime_sec')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
