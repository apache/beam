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
import math
import unittest

import apache_beam as beam
from apache_beam.ml.anomaly.detectors.zscore import ZScore
from apache_beam.ml.anomaly.univariate.mean import IncSlidingMeanTracker
from apache_beam.ml.anomaly.univariate.stdev import IncSlidingStdevTracker


class ZScoreTest(unittest.TestCase):
  input = [
      beam.Row(x=1),
      beam.Row(x=1),
      beam.Row(x=5),
      beam.Row(x=9),
      beam.Row(x=20),
      beam.Row(x=10),
      beam.Row(x=1)
  ]

  def test_with_default_trackers(self):
    zscore = ZScore()

    scores = []
    for row in ZScoreTest.input:
      scores.append(zscore.score_one(row))
      zscore.learn_one(row)

    self.assertTrue(math.isnan(scores[0]))
    self.assertTrue(math.isnan(scores[1]))
    self.assertEqual(
        scores[2:],
        [
            0.0,
            2.8867513459481287,
            4.177863742936748,
            0.35502819053868157,
            0.932910509720565
        ])

  def test_with_custom_mean_tracker(self):
    zscore = ZScore(
        sub_stat_tracker=IncSlidingMeanTracker(3),
        stdev_tracker=IncSlidingStdevTracker(3))

    scores = []
    for row in ZScoreTest.input:
      scores.append(zscore.score_one(row))
      zscore.learn_one(row)

    self.assertTrue(math.isnan(scores[0]))
    self.assertTrue(math.isnan(scores[1]))
    self.assertEqual(
        scores[2:], [
            0.0,
            2.8867513459481287,
            3.75,
            0.17165643016914964,
            1.9727878476642864
        ])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
