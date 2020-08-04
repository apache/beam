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

"""Utilities for the Nexmark suite.

The Nexmark suite is a series of queries (streaming pipelines) performed
on a simulation of auction events. This util includes:

  - A Command class used to terminate the streaming jobs
    launched in nexmark_launcher.py by the DirectRunner.
  - A ParseEventFn DoFn to parse events received from PubSub.

Usage:

To run a process for a certain duration, define in the code:
  command = Command(process_to_terminate, args)
  command.run(timeout=duration)

"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import print_function

import json
import logging
import threading

import apache_beam as beam
from apache_beam.testing.benchmarks.nexmark.models import auction_bid
from apache_beam.testing.benchmarks.nexmark.models import auction_price
from apache_beam.testing.benchmarks.nexmark.models import nexmark_model
from apache_beam.testing.benchmarks.nexmark.models.field_name import FieldNames
from apache_beam.transforms import window
from apache_beam.utils.timestamp import Timestamp

_LOGGER = logging.getLogger(__name__)


class Command(object):
  def __init__(self, cmd, args):
    self.cmd = cmd
    self.args = args

  def run(self, timeout):
    def thread_target():
      logging.debug(
          'Starting thread for %d seconds: %s', timeout, self.cmd.__name__)

      self.cmd(*self.args)
      _LOGGER.info(
          '%d seconds elapsed. Thread (%s) finished.',
          timeout,
          self.cmd.__name__)

    thread = threading.Thread(target=thread_target, name='Thread-timeout')
    thread.daemon = True
    thread.start()
    thread.join(timeout)


def setup_coder():
  beam.coders.registry.register_coder(
      nexmark_model.Auction, nexmark_model.AuctionCoder)
  beam.coders.registry.register_coder(
      nexmark_model.Person, nexmark_model.PersonCoder)
  beam.coders.registry.register_coder(nexmark_model.Bid, nexmark_model.BidCoder)
  beam.coders.registry.register_coder(
      auction_bid.AuctionBid, auction_bid.AuctionBidCoder)
  beam.coders.registry.register_coder(
      auction_price.AuctionPrice, auction_price.AuctionPriceCoder)


class ParseEventFn(beam.DoFn):
  """Parses the raw event info into a Python objects.

  Each event line has the following format:

    person: <id starting with 'p'>,name,email,credit_card,city, \
                          state,timestamp,extra
    auction: <id starting with 'a'>,item_name, description,initial_bid, \
                          reserve_price,timestamp,expires,seller,category,extra
    bid: <auction starting with 'b'>,bidder,price,timestamp,extra

  For example:

    'p12345,maria,maria@maria.com,1234-5678-9012-3456, \
                                        sunnyvale,CA,1528098831536'
    'a12345,car67,2012 hyundai elantra,15000,20000, \
                                        1528098831536,20180630,maria,vehicle'
    'b12345,maria,20000,1528098831536'
  """
  def process(self, elem):
    model_dict = {
        'p': nexmark_model.Person,
        'a': nexmark_model.Auction,
        'b': nexmark_model.Bid,
    }
    row = elem.split(',')
    model = model_dict.get(elem[0])
    if not model:
      raise ValueError('Invalid event: %s.' % row)

    event = model(*row)
    logging.debug('Parsed event: %s', event)
    yield event


class ParseJsonEvnetFn(beam.DoFn):
  """Parses the raw event info into a Python objects.

  Each event line has the following format:

    person: {id,name,email,credit_card,city, \
                          state,timestamp,extra}
    auction: {id,item_name, description,initial_bid, \
                          reserve_price,timestamp,expires,seller,category,extra}
    bid: {auction,bidder,price,timestamp,extra}

  For example:

    {"id":1000,"name":"Peter Jones","emailAddress":"nhd@xcat.com",\
        "creditCard":"7241 7320 9143 4888","city":"Portland","state":"WY",\
        "dateTime":1528098831026,\"extra":"WN_HS_bnpVQ\\[["}

    {"id":1000,"itemName":"wkx mgee","description":"eszpqxtdxrvwmmywkmogoahf",\
        "initialBid":28873,"reserve":29448,"dateTime":1528098831036,\
        "expires":1528098840451,"seller":1000,"category":13,"extra":"zcuupiz"}

    {"auction":1000,"bidder":1001,"price":32530001,"dateTime":1528098831066,\
        "extra":"fdiysaV^]NLVsbolvyqwgticfdrwdyiyofWPYTOuwogvszlxjrcNOORM"}
  """
  def process(self, elem):
    json_dict = json.loads(elem)
    if type(json_dict[FieldNames.DATE_TIME]) is dict:
      json_dict[FieldNames.DATE_TIME] = json_dict[
          FieldNames.DATE_TIME]['millis']
    if FieldNames.NAME in json_dict:
      yield nexmark_model.Person(
          json_dict[FieldNames.ID],
          json_dict[FieldNames.NAME],
          json_dict[FieldNames.EMAIL_ADDRESS],
          json_dict[FieldNames.CREDIT_CARD],
          json_dict[FieldNames.CITY],
          json_dict[FieldNames.STATE],
          millis_to_timestamp(json_dict[FieldNames.DATE_TIME]),
          json_dict[FieldNames.EXTRA])
    elif FieldNames.ITEM_NAME in json_dict:
      if type(json_dict[FieldNames.EXPIRES]) is dict:
        json_dict[FieldNames.EXPIRES] = json_dict[FieldNames.EXPIRES]['millis']
      yield nexmark_model.Auction(
          json_dict[FieldNames.ID],
          json_dict[FieldNames.ITEM_NAME],
          json_dict[FieldNames.DESCRIPTION],
          json_dict[FieldNames.INITIAL_BID],
          json_dict[FieldNames.RESERVE],
          millis_to_timestamp(json_dict[FieldNames.DATE_TIME]),
          millis_to_timestamp(json_dict[FieldNames.EXPIRES]),
          json_dict[FieldNames.SELLER],
          json_dict[FieldNames.CATEGORY],
          json_dict[FieldNames.EXTRA])
    elif FieldNames.AUCTION in json_dict:
      yield nexmark_model.Bid(
          json_dict[FieldNames.AUCTION],
          json_dict[FieldNames.BIDDER],
          json_dict[FieldNames.PRICE],
          millis_to_timestamp(json_dict[FieldNames.DATE_TIME]),
          json_dict[FieldNames.EXTRA])
    else:
      raise ValueError('Invalid event: %s.' % str(json_dict))


class CountAndLog(beam.PTransform):
  def expand(self, pcoll):
    return (
        pcoll
        | 'window' >> beam.WindowInto(window.GlobalWindows())
        | "Count" >> beam.combiners.Count.Globally()
        | "Log" >> beam.Map(log_count_info))


def log_count_info(count):
  logging.info('Query resulted in %d results', count)
  return count


def display(elm):
  logging.debug(elm)
  return elm


def model_to_json(model):
  return json.dumps(construct_json_dict(model), separators=(',', ':'))


def construct_json_dict(model):
  return {k: unnest_to_json(v) for k, v in model.__dict__.items()}


def unnest_to_json(cand):
  if isinstance(cand, Timestamp):
    return cand.micros // 1000
  elif isinstance(
      cand, (nexmark_model.Auction, nexmark_model.Bid, nexmark_model.Person)):
    return construct_json_dict(cand)
  else:
    return cand


def millis_to_timestamp(millis):
  micro_second = millis * 1000
  return Timestamp(micros=micro_second)
