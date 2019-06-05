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

"""Unit tests for the metric_result_matchers."""

from __future__ import absolute_import

import unittest

from hamcrest import assert_that as hc_assert_that
from hamcrest import anything
from hamcrest import equal_to
from hamcrest.core.core.isnot import is_not
from hamcrest.library.number.ordering_comparison import greater_than
from hamcrest.library.text.isequal_ignoring_case import equal_to_ignoring_case

from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.cells import DistributionResult
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricResult
from apache_beam.metrics.metricbase import MetricName
from apache_beam.testing.metric_result_matchers import DistributionMatcher
from apache_beam.testing.metric_result_matchers import MetricResultMatcher

EVERYTHING_DISTRIBUTION = {
    'namespace': 'myNamespace',
    'name': 'myName',
    'step': 'myStep',
    'attempted': {
        'distribution': {
            'sum': 12,
            'count': 5,
            'min': 0,
            'max': 6,
        }
    },
    'committed': {
        'distribution': {
            'sum': 12,
            'count': 5,
            'min': 0,
            'max': 6,
        }
    },
    'labels' : {
        'pcollection': 'myCollection',
        'myCustomKey': 'myCustomValue'
    }
}

EVERYTHING_COUNTER = {
    'namespace': 'myNamespace',
    'name': 'myName',
    'step': 'myStep',
    'attempted': {
        'counter': 42
    },
    'committed': {
        'counter': 42
    },
    'labels': {
        'pcollection': 'myCollection',
        'myCustomKey': 'myCustomValue'
    }
}


def _create_metric_result(data_dict):
  step = data_dict['step'] if 'step' in data_dict else ''
  labels = data_dict['labels'] if 'labels' in data_dict else dict()
  values = {}
  for key in ['attempted', 'committed']:
    if key in data_dict:
      if 'counter' in data_dict[key]:
        values[key] = data_dict[key]['counter']
      elif 'distribution' in data_dict[key]:
        distribution = data_dict[key]['distribution']
        values[key] = DistributionResult(DistributionData(
            distribution['sum'],
            distribution['count'],
            distribution['min'],
            distribution['max'],
        ))
  attempted = values['attempted'] if 'attempted' in values else None
  committed = values['committed'] if 'committed' in values else None

  metric_name = MetricName(data_dict['namespace'], data_dict['name'])
  metric_key = MetricKey(step, metric_name, labels)
  return MetricResult(metric_key, committed, attempted)


class MetricResultMatchersTest(unittest.TestCase):

  def test_matches_all_for_counter(self):
    metric_result = _create_metric_result(EVERYTHING_COUNTER)
    matcher = MetricResultMatcher(
        namespace='myNamespace',
        name='myName',
        step='myStep',
        labels={
            'pcollection': 'myCollection',
            'myCustomKey': 'myCustomValue'
        },
        attempted=42,
        committed=42
    )
    hc_assert_that(metric_result, matcher)

  def test_matches_none_for_counter(self):
    metric_result = _create_metric_result(EVERYTHING_COUNTER)
    matcher = MetricResultMatcher(
        namespace=is_not(equal_to('invalidNamespace')),
        name=is_not(equal_to('invalidName')),
        step=is_not(equal_to('invalidStep')),
        labels={
            is_not(equal_to('invalidPcollection')): anything(),
            is_not(equal_to('invalidCustomKey')): is_not(equal_to(
                'invalidCustomValue'))
        },
        attempted=is_not(equal_to(1000)),
        committed=is_not(equal_to(1000)))
    hc_assert_that(metric_result, matcher)

  def test_matches_all_for_distribution(self):
    metric_result = _create_metric_result(EVERYTHING_DISTRIBUTION)
    matcher = MetricResultMatcher(
        namespace='myNamespace',
        name='myName',
        step='myStep',
        labels={
            'pcollection': 'myCollection',
            'myCustomKey': 'myCustomValue'
        },
        committed=DistributionMatcher(
            sum_value=12,
            count_value=5,
            min_value=0,
            max_value=6
        ),
        attempted=DistributionMatcher(
            sum_value=12,
            count_value=5,
            min_value=0,
            max_value=6
        ),
    )
    hc_assert_that(metric_result, matcher)

  def test_matches_none_for_distribution(self):
    metric_result = _create_metric_result(EVERYTHING_DISTRIBUTION)
    matcher = MetricResultMatcher(
        namespace=is_not(equal_to('invalidNamespace')),
        name=is_not(equal_to('invalidName')),
        step=is_not(equal_to('invalidStep')),
        labels={
            is_not(equal_to('invalidPcollection')): anything(),
            is_not(equal_to('invalidCustomKey')): is_not(equal_to(
                'invalidCustomValue'))
        },
        committed=is_not(DistributionMatcher(
            sum_value=120,
            count_value=50,
            min_value=100,
            max_value=60
        )),
        attempted=is_not(DistributionMatcher(
            sum_value=120,
            count_value=50,
            min_value=100,
            max_value=60
        )),
    )
    hc_assert_that(metric_result, matcher)

  def test_matches_key_but_not_value(self):
    metric_result = _create_metric_result(EVERYTHING_COUNTER)
    matcher = is_not(MetricResultMatcher(
        labels={
            'pcollection': 'invalidCollection'
        }))
    hc_assert_that(metric_result, matcher)

  def test_matches_counter_with_custom_matchers(self):
    metric_result = _create_metric_result(EVERYTHING_COUNTER)
    matcher = is_not(MetricResultMatcher(
        namespace=equal_to_ignoring_case('MYNAMESPACE'),
        name=equal_to_ignoring_case('MYNAME'),
        step=equal_to_ignoring_case('MYSTEP'),
        labels={
            equal_to_ignoring_case('PCOLLECTION') :
                equal_to_ignoring_case('MYCUSTOMVALUE'),
            'myCustomKey': equal_to_ignoring_case('MYCUSTOMVALUE')
        },
        committed=greater_than(0),
        attempted=greater_than(0)
    ))
    hc_assert_that(metric_result, matcher)

  def test_matches_distribution_with_custom_matchers(self):
    metric_result = _create_metric_result(EVERYTHING_DISTRIBUTION)
    matcher = is_not(MetricResultMatcher(
        namespace=equal_to_ignoring_case('MYNAMESPACE'),
        name=equal_to_ignoring_case('MYNAME'),
        step=equal_to_ignoring_case('MYSTEP'),
        labels={
            equal_to_ignoring_case('PCOLLECTION') :
                equal_to_ignoring_case('MYCUSTOMVALUE'),
            'myCustomKey': equal_to_ignoring_case('MYCUSTOMVALUE')
        },
        committed=is_not(DistributionMatcher(
            sum_value=greater_than(-1),
            count_value=greater_than(-1),
            min_value=greater_than(-1),
            max_value=greater_than(-1)
        )),
        attempted=is_not(DistributionMatcher(
            sum_value=greater_than(-1),
            count_value=greater_than(-1),
            min_value=greater_than(-1),
            max_value=greater_than(-1)
        )),
    ))
    hc_assert_that(metric_result, matcher)

  def test_counter_does_not_match_distribution_and_doesnt_crash(self):
    metric_result = _create_metric_result(EVERYTHING_COUNTER)
    matcher = is_not(MetricResultMatcher(
        committed=DistributionMatcher(
            sum_value=120,
            count_value=50,
            min_value=100,
            max_value=60
        ),
        attempted=DistributionMatcher(
            sum_value=120,
            count_value=50,
            min_value=100,
            max_value=60
        ),
    ))
    hc_assert_that(metric_result, matcher)

  def test_distribution_does_not_match_counter_and_doesnt_crash(self):
    metric_result = _create_metric_result(EVERYTHING_DISTRIBUTION)
    matcher = is_not(MetricResultMatcher(
        attempted=42,
        committed=42
    ))
    hc_assert_that(metric_result, matcher)


if __name__ == '__main__':
  unittest.main()
