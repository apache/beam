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
from apache_beam.testing.benchmarks.nexmark import nexmark_util


class Person(object):
  "Author of an auction or a bid."

  def __init__(
      self, id, name, email, credit_card, city, state, date_time, extra=None):
    self.id = id
    self.name = name
    self.emailAddress = email  # key
    self.creditCard = credit_card
    self.city = city
    self.state = state
    self.dateTime = date_time
    self.extra = extra

  def __repr__(self):
    return nexmark_util.model_to_json(self)


class Auction(object):
  "Item for auction."

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
    self.itemName = item_name  # key
    self.description = description
    self.initialBid = initial_bid
    self.reserve = reserve_price
    self.dateTime = date_time
    self.expires = expires
    self.seller = seller
    self.category = category
    self.extra = extra

  def __repr__(self):
    return nexmark_util.model_to_json(self)


class Bid(object):
  "A bid for an item for auction."

  def __init__(self, auction, bidder, price, date_time, extra=None):
    self.auction = auction  # key
    self.bidder = bidder
    self.price = price
    self.dateTime = date_time
    self.extra = extra

  def __repr__(self):
    return nexmark_util.model_to_json(self)
