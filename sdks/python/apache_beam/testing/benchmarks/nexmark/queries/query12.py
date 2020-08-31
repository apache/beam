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
Query 12, How many bids does a user make within a fixed processing time limit
(Not in original suite.)

Group bids by the same user into processing time windows of window_size_sec.
Emit the count of bids per window.
"""

from __future__ import absolute_import

import apache_beam as beam
from apache_beam.testing.benchmarks.nexmark.queries import nexmark_query_util
from apache_beam.testing.benchmarks.nexmark.queries.nexmark_query_util import ResultNames
from apache_beam.transforms import trigger
from apache_beam.transforms import window


def load(events, metadata=None):
  return (
      events
      | nexmark_query_util.JustBids()
      | 'query12_extract_bidder' >> beam.Map(lambda bid: bid.bidder)
      # windowing with processing time trigger, currently not supported in batch
      | beam.WindowInto(
          window.GlobalWindows(),
          trigger=trigger.Repeatedly(
              trigger.AfterProcessingTime(metadata.get('window_size_sec'))),
          accumulation_mode=trigger.AccumulationMode.DISCARDING,
          allowed_lateness=0)
      | 'query12_bid_count' >> beam.combiners.Count.PerElement()
      | 'query12_output' >> beam.Map(
          lambda t: {
              ResultNames.BIDDER_ID: t[0], ResultNames.BID_COUNT: t[1]
          }))
