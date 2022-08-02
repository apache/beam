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

"""MetricResult matchers for validating metrics in PipelineResults.

example usage:
::

    result = my_pipeline.run()
    all_metrics = result.metrics().all_metrics()

    matchers = [
      MetricResultMatcher(
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
    ]
    errors = metric_result_matchers.verify_all(all_metrics, matchers)
    self.assertFalse(errors, errors)

"""

# pytype: skip-file

from hamcrest import equal_to
from hamcrest.core import string_description
from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.matcher import Matcher

from apache_beam.metrics.cells import DistributionResult


def _matcher_or_equal_to(value_or_matcher):
  """Pass-thru for matchers, and wraps value inputs in an equal_to matcher."""
  if value_or_matcher is None:
    return None
  if isinstance(value_or_matcher, Matcher):
    return value_or_matcher
  return equal_to(value_or_matcher)


class MetricResultMatcher(BaseMatcher):
  """A PyHamcrest matcher that validates counter MetricResults."""
  def __init__(
      self,
      namespace=None,
      name=None,
      step=None,
      labels=None,
      attempted=None,
      committed=None,
      sum_value=None,
      count_value=None,
      min_value=None,
      max_value=None,
  ):
    self.namespace = _matcher_or_equal_to(namespace)
    self.name = _matcher_or_equal_to(name)
    self.step = _matcher_or_equal_to(step)
    self.attempted = _matcher_or_equal_to(attempted)
    self.committed = _matcher_or_equal_to(committed)
    labels = labels or {}
    self.label_matchers = {}
    for (k, v) in labels.items():
      self.label_matchers[_matcher_or_equal_to(k)] = _matcher_or_equal_to(v)

  def _matches(self, metric_result):
    if self.namespace is not None and not self.namespace.matches(
        metric_result.key.metric.namespace):
      return False
    if self.name and not self.name.matches(metric_result.key.metric.name):
      return False
    if self.step and not self.step.matches(metric_result.key.step):
      return False
    if (self.attempted is not None and
        not self.attempted.matches(metric_result.attempted)):
      return False
    if (self.committed is not None and
        not self.committed.matches(metric_result.committed)):
      return False
    for (k_matcher, v_matcher) in self.label_matchers.items():
      matched_keys = [
          key for key in metric_result.key.labels.keys()
          if k_matcher.matches(key)
      ]
      matched_key = matched_keys[0] if matched_keys else None
      if not matched_key:
        return False
      label_value = metric_result.key.labels[matched_key]
      if not v_matcher.matches(label_value):
        return False
    return True

  def describe_to(self, description):
    if self.namespace:
      description.append_text(' namespace: ')
      self.namespace.describe_to(description)
    if self.name:
      description.append_text(' name: ')
      self.name.describe_to(description)
    if self.step:
      description.append_text(' step: ')
      self.step.describe_to(description)
    for (k_matcher, v_matcher) in self.label_matchers.items():
      description.append_text(' (label_key: ')
      k_matcher.describe_to(description)
      description.append_text(' label_value: ')
      v_matcher.describe_to(description)
      description.append_text('). ')
    if self.attempted is not None:
      description.append_text(' attempted: ')
      self.attempted.describe_to(description)
    if self.committed is not None:
      description.append_text(' committed: ')
      self.committed.describe_to(description)

  def describe_mismatch(self, metric_result, mismatch_description):
    mismatch_description.append_text("was").append_value(metric_result)


class DistributionMatcher(BaseMatcher):
  """A PyHamcrest matcher that validates counter distributions."""
  def __init__(
      self, sum_value=None, count_value=None, min_value=None, max_value=None):
    self.sum_value = _matcher_or_equal_to(sum_value)
    self.count_value = _matcher_or_equal_to(count_value)
    self.min_value = _matcher_or_equal_to(min_value)
    self.max_value = _matcher_or_equal_to(max_value)

  def _matches(self, distribution_result):
    if not isinstance(distribution_result, DistributionResult):
      return False
    if self.sum_value and not self.sum_value.matches(distribution_result.sum):
      return False
    if self.count_value and not self.count_value.matches(
        distribution_result.count):
      return False
    if self.min_value and not self.min_value.matches(distribution_result.min):
      return False
    if self.max_value and not self.max_value.matches(distribution_result.max):
      return False
    return True

  def describe_to(self, description):
    if self.sum_value:
      description.append_text(' sum_value: ')
      self.sum_value.describe_to(description)
    if self.count_value:
      description.append_text(' count_value: ')
      self.count_value.describe_to(description)
    if self.min_value:
      description.append_text(' min_value: ')
      self.min_value.describe_to(description)
    if self.max_value:
      description.append_text(' max_value: ')
      self.max_value.describe_to(description)

  def describe_mismatch(self, distribution_result, mismatch_description):
    mismatch_description.append_text('was').append_value(distribution_result)


def verify_all(all_metrics, matchers):
  """Verified that every matcher matches a metric result in all_metrics."""
  errors = []
  matched_metrics = []
  for matcher in matchers:
    matched_metrics = [mr for mr in all_metrics if matcher.matches(mr)]
    if not matched_metrics:
      errors.append(
          'Unable to match metrics for matcher %s' %
          (string_description.tostring(matcher)))
  if errors:
    errors.append(
        '\nActual MetricResults:\n' + '\n'.join([str(mr)
                                                 for mr in all_metrics]))
  return ''.join(errors)
