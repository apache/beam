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
from apache_beam.ml.anomaly.detectors.robust_zscore import RobustZScore


class RobustZScoreTest(unittest.TestCase):
  input = [
      beam.Row(x=1),
      beam.Row(x=1),
      beam.Row(x=5),
      beam.Row(x=7),
      beam.Row(x=20),
      beam.Row(x=6),
      beam.Row(x=1)
  ]

  def test_with_default_trackers(self):
    zscore = RobustZScore()

    scores = []
    for row in RobustZScoreTest.input:
      scores.append(zscore.score_one(row))
      zscore.learn_one(row)

    self.assertTrue(math.isnan(scores[0]))
    self.assertEqual(scores[1:], [0.0, 0.0, 0.0, 5.73325, 0.168625, 1.349])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
