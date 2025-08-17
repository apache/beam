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
import unittest

from apache_beam.ml.anomaly import thresholds
from apache_beam.ml.anomaly.specifiable import Spec
from apache_beam.ml.anomaly.univariate.quantile import BufferedSlidingQuantileTracker  # pylint: disable=line-too-long
from apache_beam.ml.anomaly.univariate.quantile import SimpleSlidingQuantileTracker  # pylint: disable=line-too-long


class TestFixedThreshold(unittest.TestCase):
  def test_apply_only(self):
    threshold_fn = thresholds.FixedThreshold(2)
    self.assertEqual(threshold_fn.apply(1.0), 0)
    self.assertEqual(threshold_fn.apply(2.0), 1)
    self.assertEqual(threshold_fn.apply(None), None)
    self.assertEqual(threshold_fn.apply(float('NaN')), -2)


class TestQuantileThreshold(unittest.TestCase):
  def test_apply_only(self):
    threshold_fn = thresholds.QuantileThreshold(0.9)
    self.assertEqual(threshold_fn.apply(1.0), 1)
    self.assertEqual(threshold_fn.apply(2.0), 1)
    self.assertEqual(threshold_fn.apply(1.2), 0)
    self.assertEqual(threshold_fn.apply(None), None)
    self.assertEqual(threshold_fn.apply(float('NaN')), -2)

  def test_quantile_tracker(self):
    t1 = thresholds.QuantileThreshold()
    self.assertTrue(isinstance(t1._tracker, BufferedSlidingQuantileTracker))
    self.assertEqual(t1._tracker._q, 0.95)
    self.assertEqual(t1.to_spec(), Spec("QuantileThreshold", config={}))

    t2 = thresholds.QuantileThreshold(quantile=0.99)
    self.assertTrue(isinstance(t2._tracker, BufferedSlidingQuantileTracker))
    self.assertEqual(t2._tracker._q, 0.99)
    self.assertEqual(
        t2.to_spec(), Spec("QuantileThreshold", config={"quantile": 0.99}))

    # argument quantile=0.9 is not used because quantile_tracker is set
    t3 = thresholds.QuantileThreshold(
        quantile=0.9, quantile_tracker=SimpleSlidingQuantileTracker(50, 0.975))
    self.assertTrue(isinstance(t3._tracker, SimpleSlidingQuantileTracker))
    self.assertEqual(t3._tracker._q, 0.975)
    self.assertEqual(
        t3.to_spec(),
        Spec(
            "QuantileThreshold",
            config={
                'quantile': 0.9,
                'quantile_tracker': Spec(
                    type='SimpleSlidingQuantileTracker',
                    config={
                        'window_size': 50, 'q': 0.975
                    })
            }))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
