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
from apache_beam.ml.anomaly.detectors.iqr import IQR


class IQRTest(unittest.TestCase):
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
    iqr = IQR()

    scores = []
    for row in IQRTest.input:
      scores.append(iqr.score_one(row))
      iqr.learn_one(row)

    self.assertTrue(math.isnan(scores[0]))
    self.assertEqual(
        scores[1:], [0.0, 0.0, 3.0, 2.8, 0.125, 0.12903225806451613])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
