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

# pytype: skip-file

from __future__ import absolute_import

import json
import logging

from apache_beam.metrics import MetricsFilter
from apache_beam.testing.load_tests.load_test_metrics_utils import MetricsReader
from apache_beam.testing.test_pipeline import TestPipeline


class LoadTest(object):
  def __init__(self):
    self.pipeline = TestPipeline(is_integration_test=True)
    self.input_options = json.loads(
        self.pipeline.get_option('input_options') or '{}')
    self.project_id = self.pipeline.get_option('project')
    self.metrics_namespace = self.pipeline.get_option('metrics_table')

    publish_to_bq = self._str_to_boolean('publish_to_big_query')
    if publish_to_bq is None:
      logging.info('Missing --publish_to_big_query option. Metrics will not '
                   'be published to BigQuery.')

    self._metrics_monitor = MetricsReader(
        publish_to_bq=publish_to_bq,
        project_name=self.project_id,
        bq_table=self.metrics_namespace,
        bq_dataset=self.pipeline.get_option('metrics_dataset'),
        # Apply filter to prevent system metrics from being published
        filters=MetricsFilter().with_namespace(self.metrics_namespace))

  def _str_to_boolean(self, opt_name):
    value = self.pipeline.get_option(opt_name)
    if value is None:
      return value
    try:
      return bool(['false', 'true'].index(value.lower()))
    except ValueError:
      raise ValueError('{}: "true" or "false" expected, got "{}" instead.'
                       .format(opt_name, value))

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
        self.result.wait_until_finish()
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

  def get_option_or_default(self, opt_name, default=0):
    """Returns a pipeline option or a default value if it was not provided.

    The returned value is converted to an integer.
    """
    option = self.pipeline.get_option(opt_name)
    try:
      return int(option)
    except TypeError:
      return default
