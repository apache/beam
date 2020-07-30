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

"""Nexmark model.

The nexmark suite is a series of queries (streaming pipelines) performed
on a simulation of auction events. The model includes the three roles that
generate events:

  - The person who starts and auction or makes a bid (Person).
  - The auction item (Auction).
  - The bid on an item for auction (Bid).

"""
from apache_beam.coders import coder_impl
from apache_beam.coders.coders import FastCoder
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.testing.benchmarks.nexmark import nexmark_util


class PersonCoder(FastCoder):
  def _create_impl(self):
    return PersonCoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True


class Person(object):
  "Author of an auction or a bid."
  CODER = PersonCoder()

  def __init__(
      self, id, name, email, credit_card, city, state, date_time, extra=None):
    self.id = id
    self.name = name
    self.email_address = email  # key
    self.credit_card = credit_card
    self.city = city
    self.state = state
    self.date_time = date_time
    self.extra = extra

  def __repr__(self):
    return nexmark_util.model_to_json(self)


class AuctionCoder(FastCoder):
  def to_type_hint(self):
    pass

  def _create_impl(self):
    return AuctionCoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True


class Auction(object):
  "Item for auction."
  CODER = AuctionCoder()

  def __init__(
      self,
      id,
      item_name,
      description,
      initial_bid,
      reserve_price,
      date_time,
      expires,
      seller,
      category,
      extra=None):
    self.id = id
    self.item_name = item_name  # key
    self.description = description
    self.initial_bid = initial_bid
    self.reserve = reserve_price
    self.date_time = date_time
    self.expires = expires
    self.seller = seller
    self.category = category
    self.extra = extra

  def __repr__(self):
    return nexmark_util.model_to_json(self)


class BidCoder(FastCoder):
  def _create_impl(self):
    return BidCoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True


class Bid(object):
  "A bid for an item for auction."
  CODER = BidCoder()

  def __init__(self, auction, bidder, price, date_time, extra=None):
    self.auction = auction  # key
    self.bidder = bidder
    self.price = price
    self.date_time = date_time
    self.extra = extra

  def __repr__(self):
    return nexmark_util.model_to_json(self)


class AuctionCoderImpl(coder_impl.StreamCoderImpl):
  _int_coder_impl = coder_impl.VarIntCoderImpl()
  _str_coder_impl = StrUtf8Coder().get_impl()
  _time_coder_impl = coder_impl.TimestampCoderImpl()

  def encode_to_stream(self, value: Auction, stream, nested):
    self._int_coder_impl.encode_to_stream(value.id, stream, True)
    self._str_coder_impl.encode_to_stream(value.item_name, stream, True)
    self._str_coder_impl.encode_to_stream(value.description, stream, True)
    self._int_coder_impl.encode_to_stream(value.initial_bid, stream, True)
    self._int_coder_impl.encode_to_stream(value.reserve, stream, True)
    self._time_coder_impl.encode_to_stream(value.date_time, stream, True)
    self._time_coder_impl.encode_to_stream(value.expires, stream, True)
    self._int_coder_impl.encode_to_stream(value.seller, stream, True)
    self._int_coder_impl.encode_to_stream(value.category, stream, True)
    self._str_coder_impl.encode_to_stream(value.extra, stream, True)

  def decode_from_stream(self, stream, nested):
    id = self._int_coder_impl.decode_from_stream(stream, True)
    item_name = self._str_coder_impl.decode_from_stream(stream, True)
    description = self._str_coder_impl.decode_from_stream(stream, True)
    initial_bid = self._int_coder_impl.decode_from_stream(stream, True)
    reserve = self._int_coder_impl.decode_from_stream(stream, True)
    date_time = self._time_coder_impl.decode_from_stream(stream, True)
    expires = self._time_coder_impl.decode_from_stream(stream, True)
    seller = self._int_coder_impl.decode_from_stream(stream, True)
    category = self._int_coder_impl.decode_from_stream(stream, True)
    extra = self._str_coder_impl.decode_from_stream(stream, True)
    return Auction(
        id,
        item_name,
        description,
        initial_bid,
        reserve,
        date_time,
        expires,
        seller,
        category,
        extra)


class BidCoderImpl(coder_impl.StreamCoderImpl):
  _int_coder_impl = coder_impl.VarIntCoderImpl()
  _str_coder_impl = StrUtf8Coder().get_impl()
  _time_coder_impl = coder_impl.TimestampCoderImpl()

  def encode_to_stream(self, value: Bid, stream, nested):
    self._int_coder_impl.encode_to_stream(value.auction, stream, True)
    self._int_coder_impl.encode_to_stream(value.bidder, stream, True)
    self._int_coder_impl.encode_to_stream(value.price, stream, True)
    self._time_coder_impl.encode_to_stream(value.date_time, stream, True)
    self._str_coder_impl.encode_to_stream(value.extra, stream, True)

  def decode_from_stream(self, stream, nested):
    auction = self._int_coder_impl.decode_from_stream(stream, True)
    bidder = self._int_coder_impl.decode_from_stream(stream, True)
    price = self._int_coder_impl.decode_from_stream(stream, True)
    date_time = self._time_coder_impl.decode_from_stream(stream, True)
    extra = self._str_coder_impl.decode_from_stream(stream, True)
    return Bid(auction, bidder, price, date_time, extra)


class PersonCoderImpl(coder_impl.StreamCoderImpl):
  _int_coder_impl = coder_impl.VarIntCoderImpl()
  _str_coder_impl = StrUtf8Coder().get_impl()
  _time_coder_impl = coder_impl.TimestampCoderImpl()

  def encode_to_stream(self, value: Person, stream, nested):
    self._int_coder_impl.encode_to_stream(value.id, stream, True)
    self._str_coder_impl.encode_to_stream(value.name, stream, True)
    self._str_coder_impl.encode_to_stream(value.email_address, stream, True)
    self._str_coder_impl.encode_to_stream(value.credit_card, stream, True)
    self._str_coder_impl.encode_to_stream(value.city, stream, True)
    self._str_coder_impl.encode_to_stream(value.state, stream, True)
    self._time_coder_impl.encode_to_stream(value.date_time, stream, True)
    self._str_coder_impl.encode_to_stream(value.extra, stream, True)

  def decode_from_stream(self, stream, nested):
    id = self._int_coder_impl.decode_from_stream(stream, True)
    name = self._str_coder_impl.decode_from_stream(stream, True)
    email = self._str_coder_impl.decode_from_stream(stream, True)
    credit_card = self._str_coder_impl.decode_from_stream(stream, True)
    city = self._str_coder_impl.decode_from_stream(stream, True)
    state = self._str_coder_impl.decode_from_stream(stream, True)
    date_time = self._time_coder_impl.decode_from_stream(stream, True)
    extra = self._str_coder_impl.decode_from_stream(stream, True)
    return Person(id, name, email, credit_card, city, state, date_time, extra)
