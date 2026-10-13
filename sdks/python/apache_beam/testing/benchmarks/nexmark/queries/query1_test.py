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

"""Unit tests for Nexmark Query 1."""

# pytype: skip-file

import unittest

import apache_beam as beam
from apache_beam.testing.benchmarks.nexmark.models import nexmark_model
from apache_beam.testing.benchmarks.nexmark.queries import query1
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import Timestamp


class Query1Test(unittest.TestCase):
  def test_converted_price_is_encodable_integer(self):
    bids = [
        nexmark_model.Bid(1, 2, 100, Timestamp.of(0), ''),
        nexmark_model.Bid(1, 3, 1234, Timestamp.of(1), ''),
    ]

    def round_trip_price(bid):
      coder = nexmark_model.Bid.CODER
      return coder.decode(coder.encode(bid)).price

    with TestPipeline() as p:
      prices = (query1.load(p | beam.Create(bids)) | beam.Map(round_trip_price))
      assert_that(prices, equal_to([89, 1098]))


if __name__ == '__main__':
  unittest.main()
