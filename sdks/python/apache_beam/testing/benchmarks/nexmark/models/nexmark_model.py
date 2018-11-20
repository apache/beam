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


class Person(object):
  "Author of an auction or a bid."

  def __init__(self, id, name, email, credit_card,
               city, state, timestamp, extra=None):
    self.id = id
    self.name = name
    self.email = email    # key
    self.credit_card = credit_card
    self.city = city
    self.state = state
    self.timestamp = timestamp
    self.extra = extra

  def __repr__(self):
    return 'Person({id}, {email})'.format(**{'id': self.id,
                                             'email': self.email})


class Auction(object):
  "Item for auction."

  def __init__(self, id, item_name, description, initial_bid, reserve_price,
               timestamp, expires, seller, category, extra=None):
    self.id = id
    self.item_name = item_name      # key
    self.description = description
    self.initial_bid = initial_bid
    self.reserve_price = reserve_price
    self.timestamp = timestamp
    self.expires = expires
    self.seller = seller
    self.category = category
    self.extra = extra

  def __repr__(self):
    return 'Auction({id}, {item_name})'.format(**{'id': self.id,
                                                  'item_name': self.item_name})


class Bid(object):
  "A bid for an item for auction."

  def __init__(self, auction, bidder, price, timestamp, extra=None):
    self.auction = auction    # key
    self.bidder = bidder
    self.price = price
    self.timestamp = timestamp
    self.extra = extra

  def __repr__(self):
    return 'Bid({auction}, {bidder}, {price})'.format(
        **{'auction': self.auction,
           'bidder': self.bidder,
           'price': self.price})
