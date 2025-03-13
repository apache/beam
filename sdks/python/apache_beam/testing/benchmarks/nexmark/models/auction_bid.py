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

"""Result of WinningBid transform."""
from apache_beam.coders import coder_impl
from apache_beam.coders.coders import FastCoder
from apache_beam.testing.benchmarks.nexmark import nexmark_util
from apache_beam.testing.benchmarks.nexmark.models import nexmark_model


class AuctionBidCoder(FastCoder):
  def to_type_hint(self):
    return AuctionBid

  def _create_impl(self):
    return AuctionBidCoderImpl()

  def is_deterministic(self):
    return True


class AuctionBid(object):
  CODER = AuctionBidCoder()

  def __init__(self, auction, bid):
    self.auction = auction
    self.bid = bid

  def __repr__(self):
    return nexmark_util.model_to_json(self)


class AuctionBidCoderImpl(coder_impl.StreamCoderImpl):
  _auction_coder_impl = nexmark_model.AuctionCoderImpl()
  _bid_coder_Impl = nexmark_model.BidCoderImpl()

  def encode_to_stream(self, value, stream, nested):
    self._auction_coder_impl.encode_to_stream(value.auction, stream, True)
    self._bid_coder_Impl.encode_to_stream(value.bid, stream, True)

  def decode_from_stream(self, stream, nested):
    auction = self._auction_coder_impl.decode_from_stream(stream, True)
    bid = self._bid_coder_Impl.decode_from_stream(stream, True)
    return AuctionBid(auction, bid)
