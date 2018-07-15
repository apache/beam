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

from __future__ import absolute_import

import unittest

import hamcrest as hc

from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.cells import DistributionResult
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricResult
from apache_beam.metrics.execution import MetricUpdates
from apache_beam.metrics.metricbase import MetricName
from apache_beam.runners.direct.direct_metrics import DirectMetrics


class DirectMetricsTest(unittest.TestCase):
  name1 = MetricName('namespace1', 'name1')
  name2 = MetricName('namespace1', 'name2')
  name3 = MetricName('namespace2', 'name1')

  bundle1 = object() # For this test, any object can be a bundle
  bundle2 = object()

  def test_combiner_functions(self):
    metrics = DirectMetrics()
    counter = metrics._counters['anykey']
    counter.commit_logical(self.bundle1, 5)
    self.assertEqual(counter.extract_committed(), 5)
    with self.assertRaises(TypeError):
      counter.commit_logical(self.bundle1, None)

    distribution = metrics._distributions['anykey']
    distribution.commit_logical(self.bundle1, DistributionData(4, 1, 4, 4))
    self.assertEqual(distribution.extract_committed(),
                     DistributionResult(DistributionData(4, 1, 4, 4)))

    with self.assertRaises(AttributeError):
      distribution.commit_logical(self.bundle1, None)

  def test_commit_logical_no_filter(self):
    metrics = DirectMetrics()
    metrics.commit_logical(
        self.bundle1,
        MetricUpdates(
            counters={MetricKey('step1', self.name1): 5,
                      MetricKey('step1', self.name2): 8},
            distributions={
                MetricKey('step1', self.name1): DistributionData(8, 2, 3, 5)}))

    metrics.commit_logical(
        self.bundle1,
        MetricUpdates(
            counters={MetricKey('step2', self.name1): 7,
                      MetricKey('step1', self.name2): 4},
            distributions={
                MetricKey('step1', self.name1): DistributionData(4, 1, 4, 4)}))

    results = metrics.query()
    hc.assert_that(
        results['counters'],
        hc.contains_inanyorder(*[
            MetricResult(MetricKey('step1', self.name2), 12, 0),
            MetricResult(MetricKey('step2', self.name1), 7, 0),
            MetricResult(MetricKey('step1', self.name1), 5, 0)]))
    hc.assert_that(
        results['distributions'],
        hc.contains_inanyorder(
            MetricResult(MetricKey('step1', self.name1),
                         DistributionResult(
                             DistributionData(12, 3, 3, 5)),
                         DistributionResult(
                             DistributionData(0, 0, None, None)))))

  def test_apply_physical_no_filter(self):
    metrics = DirectMetrics()
    metrics.update_physical(object(),
                            MetricUpdates(
                                counters={MetricKey('step1', self.name1): 5,
                                          MetricKey('step1', self.name3): 8}))

    metrics.update_physical(object(),
                            MetricUpdates(
                                counters={MetricKey('step2', self.name1): 7,
                                          MetricKey('step1', self.name3): 4}))
    results = metrics.query()
    hc.assert_that(results['counters'],
                   hc.contains_inanyorder(*[
                       MetricResult(MetricKey('step1', self.name1), 0, 5),
                       MetricResult(MetricKey('step1', self.name3), 0, 12),
                       MetricResult(MetricKey('step2', self.name1), 0, 7)]))

    metrics.commit_physical(object(), MetricUpdates())
    results = metrics.query()
    hc.assert_that(results['counters'],
                   hc.contains_inanyorder(*[
                       MetricResult(MetricKey('step1', self.name1), 0, 5),
                       MetricResult(MetricKey('step1', self.name3), 0, 12),
                       MetricResult(MetricKey('step2', self.name1), 0, 7)]))

  def test_apply_physical_logical(self):
    metrics = DirectMetrics()
    dist_zero = DistributionData(0, 0, None, None)
    metrics.update_physical(
        object(),
        MetricUpdates(
            counters={MetricKey('step1', self.name1): 7,
                      MetricKey('step1', self.name2): 5,
                      MetricKey('step2', self.name1): 1},
            distributions={MetricKey('step1', self.name1):
                           DistributionData(3, 1, 3, 3),
                           MetricKey('step2', self.name3):
                           DistributionData(8, 2, 4, 4)}))
    results = metrics.query()
    hc.assert_that(results['counters'],
                   hc.contains_inanyorder(*[
                       MetricResult(MetricKey('step1', self.name1), 0, 7),
                       MetricResult(MetricKey('step1', self.name2), 0, 5),
                       MetricResult(MetricKey('step2', self.name1), 0, 1)]))
    hc.assert_that(results['distributions'],
                   hc.contains_inanyorder(*[
                       MetricResult(
                           MetricKey('step1', self.name1),
                           DistributionResult(dist_zero),
                           DistributionResult(DistributionData(3, 1, 3, 3))),
                       MetricResult(
                           MetricKey('step2', self.name3),
                           DistributionResult(dist_zero),
                           DistributionResult(DistributionData(8, 2, 4, 4)))]))

    metrics.commit_physical(
        object(),
        MetricUpdates(
            counters={MetricKey('step1', self.name1): -3,
                      MetricKey('step2', self.name1): -5},
            distributions={MetricKey('step1', self.name1):
                           DistributionData(8, 4, 1, 5),
                           MetricKey('step2', self.name2):
                           DistributionData(8, 8, 1, 1)}))
    results = metrics.query()
    hc.assert_that(results['counters'],
                   hc.contains_inanyorder(*[
                       MetricResult(MetricKey('step1', self.name1), 0, 4),
                       MetricResult(MetricKey('step1', self.name2), 0, 5),
                       MetricResult(MetricKey('step2', self.name1), 0, -4)]))
    hc.assert_that(results['distributions'],
                   hc.contains_inanyorder(*[
                       MetricResult(
                           MetricKey('step1', self.name1),
                           DistributionResult(dist_zero),
                           DistributionResult(DistributionData(11, 5, 1, 5))),
                       MetricResult(
                           MetricKey('step2', self.name3),
                           DistributionResult(dist_zero),
                           DistributionResult(DistributionData(8, 2, 4, 4))),
                       MetricResult(
                           MetricKey('step2', self.name2),
                           DistributionResult(dist_zero),
                           DistributionResult(DistributionData(8, 8, 1, 1)))]))

    metrics.commit_logical(
        object(),
        MetricUpdates(
            counters={MetricKey('step1', self.name1): 3,
                      MetricKey('step1', self.name2): 5,
                      MetricKey('step2', self.name1): -3},
            distributions={MetricKey('step1', self.name1):
                           DistributionData(11, 5, 1, 5),
                           MetricKey('step2', self.name2):
                           DistributionData(8, 8, 1, 1),
                           MetricKey('step2', self.name3):
                           DistributionData(4, 1, 4, 4)}))

    results = metrics.query()
    hc.assert_that(results['counters'],
                   hc.contains_inanyorder(*[
                       MetricResult(MetricKey('step1', self.name1), 3, 4),
                       MetricResult(MetricKey('step1', self.name2), 5, 5),
                       MetricResult(MetricKey('step2', self.name1), -3, -4)]))
    hc.assert_that(results['distributions'],
                   hc.contains_inanyorder(*[
                       MetricResult(
                           MetricKey('step1', self.name1),
                           DistributionResult(DistributionData(11, 5, 1, 5)),
                           DistributionResult(DistributionData(11, 5, 1, 5))),
                       MetricResult(
                           MetricKey('step2', self.name3),
                           DistributionResult(DistributionData(4, 1, 4, 4)),
                           DistributionResult(DistributionData(8, 2, 4, 4))),
                       MetricResult(
                           MetricKey('step2', self.name2),
                           DistributionResult(DistributionData(8, 8, 1, 1)),
                           DistributionResult(DistributionData(8, 8, 1, 1)))]))


if __name__ == '__main__':
  unittest.main()
