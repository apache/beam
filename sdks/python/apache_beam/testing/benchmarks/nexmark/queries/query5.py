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
Query 5, 'Hot Items'. Which auctions have seen the most bids in the last hour
(updated every minute). In CQL syntax::

  SELECT Rstream(auction)
  FROM (SELECT B1.auction, count(*) AS num
        FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B1
        GROUP BY B1.auction)
  WHERE num >= ALL (SELECT count(*)
                    FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B2
                    GROUP BY B2.auction);

To make things a bit more dynamic and easier to test we use much shorter
windows, and we'll also preserve the bid counts.
"""

import apache_beam as beam
from apache_beam.testing.benchmarks.nexmark.queries import nexmark_query_util
from apache_beam.testing.benchmarks.nexmark.queries.nexmark_query_util import ResultNames
from apache_beam.transforms import window


def load(events, metadata=None, pipeline_options=None):
  return (
      events
      | nexmark_query_util.JustBids()
      | 'query5_sliding_window' >> beam.WindowInto(
          window.SlidingWindows(
              metadata.get('window_size_sec'),
              metadata.get('window_period_sec')))
      # project out only the auction id for each bid
      | 'extract_bid_auction' >> beam.Map(lambda bid: bid.auction)
      | 'bid_count_per_auction' >> beam.combiners.Count.PerElement()
      | 'bid_max_count' >> beam.CombineGlobally(
          MostBidCombineFn()).without_defaults()
      # TODO(leiyiz): fanout with sliding window produces duplicated results,
      #   uncomment after it is fixed [BEAM-10617]
      # .with_fanout(metadata.get('fanout'))
      | beam.FlatMap(
          lambda auc_count: [{
              ResultNames.AUCTION_ID: auction, ResultNames.NUM: auc_count[1]
          } for auction in auc_count[0]]))


class MostBidCombineFn(beam.CombineFn):
  """
  combiner function to find auctions with most bid counts
  """
  def create_accumulator(self):
    return [], 0

  def add_input(self, accumulator, element):
    accu_list, accu_count = accumulator
    auction, count = element
    if accu_count < count:
      return [auction], count
    elif accu_count > count:
      return accu_list, accu_count
    else:
      accu_list_new = accu_list.copy()
      accu_list_new.append(auction)
      return accu_list_new, accu_count

  def merge_accumulators(self, accumulators):
    max_list = []
    max_count = 0
    for (accu_list, count) in accumulators:
      if count == max_count:
        max_list = max_list + accu_list
      elif count < max_count:
        continue
      else:
        max_list = accu_list
        max_count = count
    return max_list, max_count

  def extract_output(self, accumulator):
    return accumulator
