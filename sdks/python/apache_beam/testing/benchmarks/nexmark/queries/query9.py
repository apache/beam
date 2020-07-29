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

from __future__ import absolute_import

import apache_beam as beam
from apache_beam.testing.benchmarks.nexmark.queries import nexmark_query_util
from apache_beam.testing.benchmarks.nexmark.queries import winning_bids


def load(events, metadata=None):
  return (
      events
      | beam.Filter(nexmark_query_util.auction_or_bid)
      | winning_bids.WinningBids())
