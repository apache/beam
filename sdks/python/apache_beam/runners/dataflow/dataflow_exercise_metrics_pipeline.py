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

"""A word-counting workflow."""

from __future__ import absolute_import

import time

from hamcrest.library.number.ordering_comparison import greater_than

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.testing.metric_result_matchers import DistributionMatcher
from apache_beam.testing.metric_result_matchers import MetricResultMatcher

SLEEP_TIME_SECS = 1
INPUT = [0, 0, 0, 100]
METRIC_NAMESPACE = ('apache_beam.runners.dataflow.'
                    'dataflow_exercise_metrics_pipeline.UserMetricsDoFn')


def common_metric_matchers():
  """MetricResult matchers common to all tests."""
  # TODO(ajamato): Matcher for the 'metrics' step's ElementCount.
  # TODO(ajamato): Matcher for the 'metrics' step's MeanByteCount.
  # TODO(ajamato): Matcher for the start and finish exec times.
  # TODO(ajamato): Matcher for a gauge metric once implemented in dataflow.
  matchers = [
      # User Counter Metrics.
      MetricResultMatcher(
          name='total_values',
          namespace=METRIC_NAMESPACE,
          step='metrics',
          attempted=sum(INPUT),
          committed=sum(INPUT)
      ),
      MetricResultMatcher(
          name='ExecutionTime_StartBundle',
          step='metrics',
          attempted=greater_than(0),
          committed=greater_than(0)
      ),
      MetricResultMatcher(
          name='ExecutionTime_ProcessElement',
          step='metrics',
          attempted=greater_than(0),
          committed=greater_than(0)
      ),
      MetricResultMatcher(
          name='ExecutionTime_FinishBundle',
          step='metrics',
          attempted=greater_than(0),
          committed=greater_than(0)
      )
  ]

  pcoll_names = [
      'GroupByKey/Reify-out0',
      'GroupByKey/Read-out0',
      'map_to_common_key-out0',
      'GroupByKey/GroupByWindow-out0',
      'GroupByKey/Read-out0',
      'GroupByKey/Reify-out0'
  ]
  for name in pcoll_names:
    matchers.extend([
        MetricResultMatcher(
            name='ElementCount',
            labels={
                'output_user_name': name,
                'original_name': '%s-ElementCount' % name
            },
            attempted=greater_than(0),
            committed=greater_than(0)
        ),
        MetricResultMatcher(
            name='MeanByteCount',
            labels={
                'output_user_name': name,
                'original_name': '%s-MeanByteCount' % name
            },
            attempted=greater_than(0),
            committed=greater_than(0)
        ),
    ])
  return matchers


def fn_api_metric_matchers():
  """MetricResult matchers with adjusted step names for the FN API DF test."""
  matchers = common_metric_matchers()
  return matchers


def legacy_metric_matchers():
  """MetricResult matchers with adjusted step names for the legacy DF test."""
  # TODO(ajamato): Move these to the common_metric_matchers once implemented
  # in the FN API.
  matchers = common_metric_matchers()
  matchers.extend([
      # User distribution metric, legacy DF only.
      MetricResultMatcher(
          name='distribution_values',
          namespace=METRIC_NAMESPACE,
          step='metrics',
          attempted=DistributionMatcher(
              sum_value=sum(INPUT),
              count_value=len(INPUT),
              min_value=min(INPUT),
              max_value=max(INPUT)
          ),
          committed=DistributionMatcher(
              sum_value=sum(INPUT),
              count_value=len(INPUT),
              min_value=min(INPUT),
              max_value=max(INPUT)
          ),
      ),
      # Element count and MeanByteCount for a User ParDo.
      MetricResultMatcher(
          name='ElementCount',
          labels={
              'output_user_name': 'metrics-out0',
              'original_name': 'metrics-out0-ElementCount'
          },
          attempted=greater_than(0),
          committed=greater_than(0)
      ),
      MetricResultMatcher(
          name='MeanByteCount',
          labels={
              'output_user_name': 'metrics-out0',
              'original_name': 'metrics-out0-MeanByteCount'
          },
          attempted=greater_than(0),
          committed=greater_than(0)
      ),
  ])
  return matchers


class UserMetricsDoFn(beam.DoFn):
  """Parse each line of input text into words."""

  def __init__(self):
    self.total_metric = Metrics.counter(self.__class__, 'total_values')
    self.dist_metric = Metrics.distribution(
        self.__class__, 'distribution_values')
    # TODO(ajamato): Add a verifier for gauge once it is supported by the SDKs
    # and runners.
    self.latest_metric = Metrics.gauge(self.__class__, 'latest_value')

  def start_bundle(self):
    time.sleep(SLEEP_TIME_SECS)

  def process(self, element):
    """Returns the processed element and increments the metrics."""
    elem_int = int(element)
    self.total_metric.inc(elem_int)
    self.dist_metric.update(elem_int)
    self.latest_metric.set(elem_int)
    time.sleep(SLEEP_TIME_SECS)
    return [elem_int]

  def finish_bundle(self):
    time.sleep(SLEEP_TIME_SECS)


def apply_and_run(pipeline):
  """Given an initialized Pipeline applies transforms and runs it."""
  _ = (pipeline
       | beam.Create(INPUT)
       | 'metrics' >> (beam.ParDo(UserMetricsDoFn()))
       | 'map_to_common_key' >> beam.Map(lambda x: ('key', x))
       | beam.GroupByKey()
       | 'm_out' >> beam.FlatMap(lambda x: [
           1, 2, 3, 4, 5,
           beam.pvalue.TaggedOutput('once', x),
           beam.pvalue.TaggedOutput('twice', x),
           beam.pvalue.TaggedOutput('twice', x)])
      )
  result = pipeline.run()
  result.wait_until_finish()
  return result
