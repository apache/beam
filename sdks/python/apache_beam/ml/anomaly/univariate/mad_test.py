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

from apache_beam.ml.anomaly.univariate.mad import MadTracker


class MadTest(unittest.TestCase):
  def test_default_tracker(self):
    t = MadTracker()
    self.assertTrue(math.isnan(t.get()))

    t.push(4.0)  # median=4, abs_dev=[0]
    self.assertEqual(t.get(), 0)

    t.push(2.0)  # median=3, abs_dev=[0, 1]
    self.assertEqual(t.get(), 0.5)

    t.push(5.0)  # median=4, abs_dev=[0, 1, 1]
    self.assertEqual(t.get(), 1.0)

    t.push(3.0)  # median=3.5 abs_dev=[0, 0.5, 1, 1]
    self.assertEqual(t.get(), 0.75)

    t.push(0.0)  # median=3  abs_dev=[0, 0.5, 1, 1, 3]
    self.assertEqual(t.get(), 1.0)

    t.push(4.0)  # median=3.5 abs_dev[0, 0.5, 0.5, 1, 1, 3]
    self.assertEqual(t.get(), 0.75)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
