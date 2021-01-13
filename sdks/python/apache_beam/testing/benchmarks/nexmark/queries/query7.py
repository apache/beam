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

"""
Query 7, 'Highest Bid'. Select the bids with the highest bid price in the
last minute. In CQL syntax::

  SELECT Rstream(B.auction, B.price, B.bidder)
  FROM Bid [RANGE 1 MINUTE SLIDE 1 MINUTE] B
  WHERE B.price = (SELECT MAX(B1.price)
                   FROM BID [RANGE 1 MINUTE SLIDE 1 MINUTE] B1);

We will use a shorter window to help make testing easier. We'll also
implement this using a side-input in order to exercise that functionality.
(A combiner, as used in Query 5, is a more efficient approach.).
"""

from __future__ import absolute_import

import apache_beam as beam
from apache_beam.testing.benchmarks.nexmark.queries import nexmark_query_util
from apache_beam.transforms import window


def load(events, metadata=None):
  # window bids into fixed window
  sliding_bids = (
      events
      | nexmark_query_util.JustBids()
      | beam.WindowInto(window.FixedWindows(metadata.get('window_size_sec'))))
  # find the largest price in all bids per window
  max_prices = (
      sliding_bids
      | beam.Map(lambda bid: bid.price)
      | beam.CombineGlobally(max).without_defaults())
  return (
      sliding_bids
      | 'select_bids' >> beam.ParDo(
          SelectMaxBidFn(), beam.pvalue.AsSingleton(max_prices)))


class SelectMaxBidFn(beam.DoFn):
  def process(self, element, max_bid_price):
    if element.price == max_bid_price:
      yield element
