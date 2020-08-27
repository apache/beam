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
Query 11, How many bids did a user make in each session he was active?
(Not in original suite.)

Group bids by the same user into sessions with window_size_sec max gap.
However limit the session to at most max_log_events. Emit the number of
bids per session.
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
      # filter to get only bids and then extract bidder id
      | nexmark_query_util.JustBids()
      | 'query11_extract_bidder' >> beam.Map(lambda bid: bid.bidder)
      # window auction and key by auctions' seller
      | 'query11_session_window' >> beam.WindowInto(
          window.Sessions(metadata.get('window_size_sec')),
          trigger=trigger.AfterWatermark(
              early=trigger.AfterCount(metadata.get('max_log_events'))),
          accumulation_mode=trigger.AccumulationMode.DISCARDING,
          allowed_lateness=metadata.get('occasional_delay_sec') // 2)
      # count per bidder
      | beam.combiners.Count.PerElement()
      | beam.Map(
          lambda bidder_count: {
              ResultNames.BIDDER_ID: bidder_count[0],
              ResultNames.BID_COUNT: bidder_count[1]
          }))
