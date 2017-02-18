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
"""Unit tests for the apiclient module."""

import unittest

from mock import Mock

from apache_beam.metrics.cells import DistributionData
from apache_beam.runners.google_cloud_dataflow.dataflow_runner import DataflowRunner
from apache_beam.runners.google_cloud_dataflow.internal import apiclient
from apache_beam.runners.google_cloud_dataflow.internal.clients import dataflow
from apache_beam.utils.pipeline_options import PipelineOptions


class UtilTest(unittest.TestCase):

  @unittest.skip("Enable once BEAM-1080 is fixed.")
  def test_create_application_client(self):
    pipeline_options = PipelineOptions()
    apiclient.DataflowApplicationClient(
        pipeline_options,
        DataflowRunner.BATCH_ENVIRONMENT_MAJOR_VERSION)

  def test_default_job_name(self):
    job_name = apiclient.Job.default_job_name(None)
    regexp = 'beamapp-.*-[0-9]{10}-[0-9]{6}'
    self.assertRegexpMatches(job_name, regexp)

  def test_split_int(self):
    number = 12345
    split_number = apiclient.to_split_int(number)
    self.assertEqual((split_number.lowBits, split_number.highBits),
                     (number, 0))
    shift_number = number << 32
    split_number = apiclient.to_split_int(shift_number)
    self.assertEqual((split_number.lowBits, split_number.highBits),
                     (0, number))

  def test_translate_distribution(self):
    metric_update = dataflow.CounterUpdate()
    distribution_update = DistributionData(16, 2, 1, 15)
    apiclient.translate_distribution(distribution_update, metric_update)
    self.assertEqual(metric_update.distribution.min.lowBits,
                     distribution_update.min)
    self.assertEqual(metric_update.distribution.max.lowBits,
                     distribution_update.max)
    self.assertEqual(metric_update.distribution.sum.lowBits,
                     distribution_update.sum)
    self.assertEqual(metric_update.distribution.count.lowBits,
                     distribution_update.count)

  def test_translate_means(self):
    metric_update = dataflow.CounterUpdate()
    accumulator = Mock()
    accumulator.sum = 16
    accumulator.count = 2
    apiclient.MetricUpdateTranslators.translate_scalar_mean_int(accumulator,
                                                                metric_update)
    self.assertEqual(metric_update.integerMean.sum.lowBits, accumulator.sum)
    self.assertEqual(metric_update.integerMean.count.lowBits, accumulator.count)

    accumulator.sum = 16.0
    accumulator.count = 2
    apiclient.MetricUpdateTranslators.translate_scalar_mean_float(accumulator,
                                                                  metric_update)
    self.assertEqual(metric_update.floatingPointMean.sum, accumulator.sum)
    self.assertEqual(
        metric_update.floatingPointMean.count.lowBits, accumulator.count)


if __name__ == '__main__':
  unittest.main()
