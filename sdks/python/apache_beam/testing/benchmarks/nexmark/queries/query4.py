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
Query 4, 'Average Price for a Category'. Select the average of the wining bid
prices for all closed auctions in each category. In CQL syntax::

  SELECT Istream(AVG(Q.final))
  FROM Category C, (SELECT Rstream(MAX(B.price) AS final, A.category)
    FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
    WHERE A.id=B.auction
      AND B.datetime < A.expires AND A.expires < CURRENT_TIME
    GROUP BY A.id, A.category) Q
  WHERE Q.category = C.id
  GROUP BY C.id;

For extra spiciness our implementation differs slightly from the above:

* We select both the average winning price and the category.
* We don't bother joining with a static category table, since it's
  contents are never used.
* We only consider bids which are above the auction's reserve price.
* We accept the highest-price, earliest valid bid as the winner.
* We calculate the averages oven a sliding window of size
  window_size_sec and period window_period_sec.
"""

from __future__ import absolute_import

import apache_beam as beam
from apache_beam.testing.benchmarks.nexmark.queries import nexmark_query_util
from apache_beam.testing.benchmarks.nexmark.queries import winning_bids
from apache_beam.testing.benchmarks.nexmark.queries.nexmark_query_util import ResultNames
from apache_beam.transforms import window


def load(events, metadata=None):
  # find winning bids for each closed auction
  all_winning_bids = (
      events
      | beam.Filter(nexmark_query_util.auction_or_bid)
      | winning_bids.WinningBids())
  return (
      all_winning_bids
      # key winning bids by auction category
      | beam.Map(lambda auc_bid: (auc_bid.auction.category, auc_bid.bid.price))
      # re-window for sliding average
      | beam.WindowInto(
          window.SlidingWindows(
              metadata.get('window_size_sec'),
              metadata.get('window_period_sec')))
      # average for each category
      | beam.CombinePerKey(beam.combiners.MeanCombineFn())
      # TODO(leiyiz): fanout with sliding window produces duplicated results,
      #   uncomment after it is fixed [BEAM-10617]
      # .with_hot_key_fanout(metadata.get('fanout'))
      # produce output
      | beam.ParDo(ProjectToCategoryPriceFn()))


class ProjectToCategoryPriceFn(beam.DoFn):
  def process(self, element, pane_info=beam.DoFn.PaneInfoParam):
    yield {
        ResultNames.CATEGORY: element[0],
        ResultNames.PRICE: element[1],
        ResultNames.IS_LAST: pane_info.is_last
    }
