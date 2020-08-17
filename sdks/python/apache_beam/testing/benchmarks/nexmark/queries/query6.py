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
Query 6, 'Average Selling Price by Seller'. Select the average selling price
over the last 10 closed auctions by the same seller. In CQL syntax::

  SELECT Istream(AVG(Q.final), Q.seller)
  FROM (SELECT Rstream(MAX(B.price) AS final, A.seller)
    FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
    WHERE A.id=B.auction
      AND B.datetime < A.expires AND A.expires < CURRENT_TIME
    GROUP BY A.id, A.seller) [PARTITION BY A.seller ROWS 10] Q
  GROUP BY Q.seller;
"""

from __future__ import absolute_import
from __future__ import division

import apache_beam as beam
from apache_beam.testing.benchmarks.nexmark.models.result_name import ResultNames
from apache_beam.testing.benchmarks.nexmark.queries import nexmark_query_util
from apache_beam.testing.benchmarks.nexmark.queries import winning_bids
from apache_beam.transforms import trigger
from apache_beam.transforms import window


def load(events, metadata=None):
  # find winning bids for each closed auction
  return (
      events
      # find winning bids
      | beam.Filter(nexmark_query_util.auction_or_bid)
      | winning_bids.WinningBids()
      # (auction_bids -> (aution.seller, bid)
      | beam.Map(lambda auc_bid: (auc_bid.auction.seller, auc_bid.bid))
      # calculate and output mean as data arrives
      | beam.WindowInto(
          window.GlobalWindows(),
          trigger=trigger.Repeatedly(trigger.AfterCount(1)),
          accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
          allowed_lateness=0)
      | beam.CombinePerKey(MovingMeanSellingPriceFn(10))
      | beam.Map(lambda t: {
          ResultNames.SELLER: t[0], ResultNames.PRICE: t[1]
      }))


class MovingMeanSellingPriceFn(beam.CombineFn):
  """
  Combiner to keep track of up to max_num_bids of the most recent wining
  bids and calculate their average selling price.
  """
  def __init__(self, max_num_bids):
    self.max_num_bids = max_num_bids

  def create_accumulator(self):
    return []

  def add_input(self, accumulator, element):
    accumulator.append(element)
    new_accu = sorted(accumulator, key=lambda bid: (bid.date_time, bid.price))
    if len(new_accu) > self.max_num_bids:
      del new_accu[0]
    return new_accu

  def merge_accumulators(self, accumulators):
    new_accu = []
    for accumulator in accumulators:
      new_accu += accumulator
    new_accu.sort(key=lambda bid: (bid.date_time, bid.price))
    return new_accu[-10:]

  def extract_output(self, accumulator):
    if len(accumulator) == 0:
      return 0
    sum_price = sum(bid.price for bid in accumulator)
    return int(sum_price / len(accumulator))
