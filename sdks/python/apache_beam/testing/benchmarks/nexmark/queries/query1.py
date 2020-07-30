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

"""Nexmark Query 1: Convert bid prices from dollars to euros.

The Nexmark suite is a series of queries (streaming pipelines) performed
on a simulation of auction events.

This query converts bid prices from dollars to euros.
It illustrates a simple map.
"""
# pytype: skip-file

from __future__ import absolute_import

import apache_beam as beam
from apache_beam.testing.benchmarks.nexmark.models import nexmark_model
from apache_beam.testing.benchmarks.nexmark.queries import nexmark_query_util


def load(raw_events, query_args=None):
  return (
      raw_events
      | nexmark_query_util.JustBids()
      | 'ConvertToEuro' >> beam.Map(
          lambda bid: nexmark_model.Bid(
              bid.auction,
              bid.bidder, (bid.price * 89) // 100,
              bid.date_time,
              bid.extra)))
