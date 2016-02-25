# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for Aggregator class."""

import unittest

from google.cloud.dataflow.transforms import combiners
from google.cloud.dataflow.transforms.aggregator import Aggregator


class AggregatorTest(unittest.TestCase):

  def test_str(self):
    basic = Aggregator('a-name')
    self.assertEqual('<Aggregator a-name>', str(basic))

    for_max = Aggregator('max-name', max)
    self.assertEqual('<Aggregator max-name max>', str(for_max))

    for_float = Aggregator('f-name', sum, float)
    self.assertEqual('<Aggregator f-name sum(float)>', str(for_float))

    for_mean = Aggregator('m-name', combiners.Mean(), float)
    self.assertEqual('<Aggregator m-name Mean(float)>', str(for_mean))


if __name__ == '__main__':
  unittest.main()
