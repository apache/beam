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
import unittest

from apache_beam.metrics import MetricsFilter
from apache_beam.testing.load_tests.load_test_metrics_utils import MetricsReader
from apache_beam.testing.test_pipeline import TestPipeline


class LoadTest(unittest.TestCase):
  def parseTestPipelineOptions(self, options=None):
    if not options:
      options = self.input_options

    return {
        'numRecords': options.get('num_records'),
        'keySizeBytes': options.get('key_size'),
        'valueSizeBytes': options.get('value_size'),
        'hotKeyFraction': options.get('hot_key_fraction', 0),
        'numHotKeys': options.get('num_hot_keys', 0),
        'bundleSizeDistribution': {
            'type': options.get(
                'bundle_size_distribution_type', 'const'
            ),
            'param': options.get('bundle_size_distribution_param', 0)
        },
        'forceNumInitialBundles': options.get(
            'force_initial_num_bundles', 0
        )
    }

  def setUp(self, pipeline_options=None):
    self.pipeline = TestPipeline(pipeline_options)
    input = self.pipeline.get_option('input_options') or '{}'
    self.input_options = json.loads(input)
    self.project_id = self.pipeline.get_option('project')

    self.metrics_dataset = self.pipeline.get_option('metrics_dataset')
    self.metrics_namespace = self.pipeline.get_option('metrics_table')

    self.metrics_monitor = MetricsReader(
        publish_to_bq=self.pipeline.get_option('publish_to_big_query') ==
        'true',
        project_name=self.project_id,
        bq_table=self.metrics_namespace,
        bq_dataset=self.metrics_dataset,
        # Apply filter to prevent system metrics from being published
        filters=MetricsFilter().with_namespace(self.metrics_namespace)
    )

  def tearDown(self):
    if not hasattr(self, 'result'):
      self.result = self.pipeline.run()
      self.result.wait_until_finish()

    if self.metrics_monitor:
      self.metrics_monitor.publish_metrics(self.result)

  def get_option_or_default(self, opt_name, default=0):
    """Returns a pipeline option or a default value if it was not provided.

    The returned value is converted to an integer.
    """
    option = self.pipeline.get_option(opt_name)
    try:
      return int(option)
    except TypeError:
      return default
    except ValueError as exc:
      self.fail(str(exc))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
