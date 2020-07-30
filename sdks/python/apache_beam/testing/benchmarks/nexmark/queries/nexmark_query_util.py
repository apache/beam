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

"""Utilities for working with NEXmark data stream."""

import apache_beam as beam
from apache_beam.testing.benchmarks.nexmark.models import nexmark_model

AUCTION_TAG = 'auctions'
BID_TAG = 'bids'
PERSON_TAG = 'person'


def is_bid(event):
  return isinstance(event, nexmark_model.Bid)


def is_auction(event):
  return isinstance(event, nexmark_model.Auction)


class JustBids(beam.PTransform):
  def expand(self, pcoll):
    return pcoll | "IsBid" >> beam.Filter(is_bid)


class JustAuctions(beam.PTransform):
  def expand(self, pcoll):
    return pcoll | "IsAuction" >> beam.Filter(is_auction)


class AuctionByIdFn(beam.DoFn):
  def process(self, element):
    yield element.id, element


class BidByAuctionIdFn(beam.DoFn):
  def process(self, element):
    yield element.auction, element


def auction_or_bid(event):
  return isinstance(event, (nexmark_model.Auction, nexmark_model.Bid))
