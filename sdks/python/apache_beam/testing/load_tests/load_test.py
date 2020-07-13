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

# pylint: disable-all
# pytype: skip-file
# pylint: skip-file

from __future__ import absolute_import

import argparse
import json
import logging
import os
import sys

from apache_beam.metrics import MetricsFilter
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.load_tests.load_test_metrics_utils import InfluxDBMetricsPublisherOptions
from apache_beam.testing.load_tests.load_test_metrics_utils import MetricsReader
from apache_beam.testing.test_pipeline import TestPipeline


class LoadTestOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--publish_to_big_query',
        type=cls._str_to_boolean,
        help='Publishes pipeline metrics to BigQuery table.')
    parser.add_argument(
        '--metrics_dataset',
        help='A BigQuery dataset where metrics should be'
        'written.')
    parser.add_argument(
        '--metrics_table',
        help='A BigQuery table where metrics should be '
        'written.')
    parser.add_argument(
        '--influx_measurement',
        help='An InfluxDB measurement where metrics should be published to. '
        'Measurement can be thought of as a SQL table. If empty, reporting to '
        'InfluxDB will be disabled.')
    parser.add_argument(
        '--influx_db_name',
        help='InfluxDB database name. If empty, reporting to InfluxDB will be '
        'disabled.')
    parser.add_argument(
        '--influx_hostname',
        help='Hostname to connect to InfluxDB. Defaults to '
        '"http://localhost:8086".',
        default='http://localhost:8086')
    parser.add_argument(
        '--input_options',
        type=json.loads,
        help='Input specification of SyntheticSource.')
    parser.add_argument(
        '--timeout_ms',
        type=int,
        default=0,
        help='Waiting time for the completion of the pipeline in milliseconds.'
        'Defaults to waiting forever.')

  @staticmethod
  def _str_to_boolean(value):
    try:
      return bool(['false', 'true'].index(value.lower()))
    except ValueError:
      raise argparse.ArgumentTypeError(
          '"true" or "false" expected, got "{}" '
          'instead.'.format(value))


class LoadTest(object):
  """Base class for all integration and performance tests which export
  metrics to external databases: BigQuery or/and InfluxDB.

  Refer to :class:`~apache_beam.testing.load_tests.LoadTestOptions` for more
  information on the required pipeline options.

  If using InfluxDB with Basic HTTP authentication enabled, provide the
  following environment options: `INFLUXDB_USER` and `INFLUXDB_USER_PASSWORD`.
  """
  def __init__(self, metrics_namespace=None):
    # Be sure to set blocking to false for timeout_ms to work properly
    self.pipeline = TestPipeline(
        is_integration_test=True,
        blocking=False,
        options=self.parse_pipeline_options())

    assert not self.pipeline.blocking

    options = self.pipeline.get_pipeline_options().view_as(LoadTestOptions)
    self.timeout_ms = options.timeout_ms
    self.input_options = options.input_options

    if metrics_namespace:
      self.metrics_namespace = metrics_namespace
    else:
      self.metrics_namespace = options.metrics_table \
        if options.metrics_table else 'default'

    publish_to_bq = options.publish_to_big_query
    if publish_to_bq is None:
      logging.info(
          'Missing --publish_to_big_query option. Metrics will not '
          'be published to BigQuery.')
    if options.input_options is None:
      logging.error('--input_options argument is required.')
      sys.exit(1)

    gcloud_options = self.pipeline.get_pipeline_options().view_as(
        GoogleCloudOptions)
    self.project_id = gcloud_options.project

    self._metrics_monitor = MetricsReader(
        publish_to_bq=publish_to_bq,
        project_name=self.project_id,
        bq_table=options.metrics_table,
        bq_dataset=options.metrics_dataset,
        namespace=self.metrics_namespace,
        influxdb_options=InfluxDBMetricsPublisherOptions(
            options.influx_measurement,
            options.influx_db_name,
            options.influx_hostname,
            os.getenv('INFLUXDB_USER'),
            os.getenv('INFLUXDB_USER_PASSWORD')),
        # Apply filter to prevent system metrics from being published
        filters=MetricsFilter().with_namespace(self.metrics_namespace))

  def test(self):
    """An abstract method where the pipeline definition should be put."""
    pass

  def cleanup(self):
    """An abstract method that executes after the test method."""
    pass

  def run(self):
    try:
      self.test()
      if not hasattr(self, 'result'):
        self.result = self.pipeline.run()
        # Defaults to waiting forever, unless timeout_ms has been set
        self.result.wait_until_finish(duration=self.timeout_ms)
      self._metrics_monitor.publish_metrics(self.result)
    finally:
      self.cleanup()

  def parse_synthetic_source_options(self, options=None):
    if not options:
      options = self.input_options
    return {
        'numRecords': options.get('num_records'),
        'keySizeBytes': options.get('key_size'),
        'valueSizeBytes': options.get('value_size'),
        'hotKeyFraction': options.get('hot_key_fraction', 0),
        'numHotKeys': options.get('num_hot_keys', 0),
        'bundleSizeDistribution': {
            'type': options.get('bundle_size_distribution_type', 'const'),
            'param': options.get('bundle_size_distribution_param', 0)
        },
        'forceNumInitialBundles': options.get('force_initial_num_bundles', 0)
    }

  def parse_pipeline_options(self):
    # parser = argparse.ArgumentParser(description='Parse pipeline options.')
    # parser.add_argument('--test-pipeline-options', type=str)
    # parser.add_argument(
    #     '--runtime_type_check',
    #     dest='runtime_type_check',
    #     default=False,
    #     type=bool)
    # args = parser.parse_args()
    return PipelineOptions(runtime_type_check=True)

  def get_option_or_default(self, opt_name, default=0):
    """Returns a testing option or a default value if it was not provided.

    The returned value is cast to the type of the default value.
    """
    option = self.pipeline.get_option(
        opt_name, bool_option=type(default) == bool)
    if option is None:
      return default
    try:
      return type(default)(option)
    except:
      raise
