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

import logging
import threading
import json

import apache_beam as beam
from apache_beam.testing.benchmarks.nexmark.models import nexmark_model
from apache_beam.testing.benchmarks.nexmark.models.field_name import FieldName

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
    if type(json_dict[FieldName.DATE_TIME]) is dict:
      json_dict[FieldName.DATE_TIME] = json_dict[FieldName.DATE_TIME]['millis']
    if FieldName.NAME in json_dict:
      yield nexmark_model.Person(
          json_dict[FieldName.ID],
          json_dict[FieldName.NAME],
          json_dict[FieldName.EMAIL_ADDRESS],
          json_dict[FieldName.CREDIT_CARD],
          json_dict[FieldName.CITY],
          json_dict[FieldName.STATE],
          json_dict[FieldName.DATE_TIME],
          json_dict[FieldName.EXTRA])
    elif FieldName.ITEM_NAME in json_dict:
      yield nexmark_model.Auction(
          json_dict[FieldName.ID],
          json_dict[FieldName.ITEM_NAME],
          json_dict[FieldName.DESCRIPTION],
          json_dict[FieldName.INITIAL_BID],
          json_dict[FieldName.RESERVE],
          json_dict[FieldName.DATE_TIME],
          json_dict[FieldName.EXPIRES],
          json_dict[FieldName.SELLER],
          json_dict[FieldName.CATEGORY],
          json_dict[FieldName.EXTRA])
    elif FieldName.AUCTION in json_dict:
      yield nexmark_model.Bid(
          json_dict[FieldName.AUCTION],
          json_dict[FieldName.BIDDER],
          json_dict[FieldName.PRICE],
          json_dict[FieldName.DATE_TIME],
          json_dict[FieldName.EXTRA])
    else:
      raise ValueError('Invalid event: %s.' % str(json_dict))


class CountAndLog(beam.PTransform):
  def expand(self, pcoll):
    return (
        pcoll | "Count" >> beam.combiners.Count.Globally()
        | "Log" >> beam.Map(log_count_info))


def log_count_info(count):
  logging.info('Query resulted in %d results', count)
  return count


def display(elm):
  logging.debug(elm)
  return elm
