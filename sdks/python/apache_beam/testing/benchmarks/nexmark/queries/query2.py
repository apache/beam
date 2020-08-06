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
Query 2: Find bids with specific auction ids and show their bid price

This query selects Bids that have a particular auctiuon id, and output their
auction id with bid price.
It illustrates a simple filter.
"""

# pytype: skip-file

from __future__ import absolute_import

import apache_beam as beam
from apache_beam.testing.benchmarks.nexmark.models import auction_price
from apache_beam.testing.benchmarks.nexmark.queries import nexmark_query_util


def load(events, metadata=None):
  return (
      events
      | nexmark_query_util.JustBids()
      | 'filter_by_skip' >>
      beam.Filter(lambda bid: bid.auction % metadata.get('auction_skip') == 0)
      | 'project' >>
      beam.Map(lambda bid: auction_price.AuctionPrice(bid.auction, bid.price)))
