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
Query 3, 'Local Item Suggestion'. Who is selling in OR, ID or CA in category
10, and for what auction ids? In CQL syntax::

  SELECT Istream(P.name, P.city, P.state, A.id)
  FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
  WHERE A.seller = P.id
    AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA')
    AND A.category = 10;

We'll implement this query to allow 'new auction' events to come before the
'new person' events for the auction seller. Those auctions will be stored until
the matching person is seen. Then all subsequent auctions for a person will use
the stored person record.
"""

import logging
import typing

import apache_beam as beam
from apache_beam.testing.benchmarks.nexmark.models import nexmark_model
from apache_beam.testing.benchmarks.nexmark.queries import nexmark_query_util
from apache_beam.testing.benchmarks.nexmark.queries.nexmark_query_util import ResultNames
from apache_beam.transforms import trigger
from apache_beam.transforms import userstate
from apache_beam.transforms import window
from apache_beam.transforms.userstate import on_timer


def load(events, metadata=None, pipeline_options=None):
  num_events_in_pane = 30
  windowed_events = (
      events
      | beam.WindowInto(
          window.GlobalWindows(),
          trigger=trigger.Repeatedly(trigger.AfterCount(num_events_in_pane)),
          accumulation_mode=trigger.AccumulationMode.DISCARDING))
  auction_by_seller_id = (
      windowed_events
      | nexmark_query_util.JustAuctions()
      | 'query3_filter_category' >> beam.Filter(lambda auc: auc.category == 10)
      | 'query3_key_by_seller' >> beam.ParDo(
          nexmark_query_util.AuctionBySellerFn()))
  person_by_id = (
      windowed_events
      | nexmark_query_util.JustPerson()
      | 'query3_filter_region' >>
      beam.Filter(lambda person: person.state in ['OR', 'ID', 'CA'])
      | 'query3_key_by_person_id' >> beam.ParDo(
          nexmark_query_util.PersonByIdFn()))
  return ({
      nexmark_query_util.AUCTION_TAG: auction_by_seller_id,
      nexmark_query_util.PERSON_TAG: person_by_id,
  }
          | beam.CoGroupByKey()
          | 'query3_join' >> beam.ParDo(
              JoinFn(metadata.get('max_auction_waiting_time')))
          | 'query3_output' >> beam.Map(
              lambda t: {
                  ResultNames.NAME: t[1].name, ResultNames.CITY: t[1].city,
                  ResultNames.STATE: t[1].state, ResultNames.AUCTION_ID: t[0].id
              }))


class JoinFn(beam.DoFn):
  """
  Join auctions and person by person id and emit their product one pair at
  a time.

  We know a person may submit any number of auctions. Thus new person event
  must have the person record stored in persistent state in order to match
  future auctions by that person.

  However we know that each auction is associated with at most one person, so
  only need to store auction records in persistent state until we have seen the
  corresponding person record. And of course may have already seen that record.
  """

  AUCTIONS = 'auctions_state'
  PERSON = 'person_state'
  PERSON_EXPIRING = 'person_state_expiring'

  auction_spec = userstate.BagStateSpec(AUCTIONS, nexmark_model.Auction.CODER)
  person_spec = userstate.ReadModifyWriteStateSpec(
      PERSON, nexmark_model.Person.CODER)
  person_timer_spec = userstate.TimerSpec(
      PERSON_EXPIRING, userstate.TimeDomain.WATERMARK)

  def __init__(self, max_auction_wait_time):
    self.max_auction_wait_time = max_auction_wait_time

  def process(  # type: ignore
      self,
      element: typing.Tuple[
          str,
          typing.Dict[str,
                      typing.Union[typing.List[nexmark_model.Auction],
                                   typing.List[nexmark_model.Person]]]],
      auction_state=beam.DoFn.StateParam(auction_spec),
      person_state=beam.DoFn.StateParam(person_spec),
      person_timer=beam.DoFn.TimerParam(person_timer_spec)):
    # extract group with tags from element tuple
    _, group = element

    existing_person = person_state.read()
    if existing_person:
      # the person exists in person_state for this person id
      for auction in group[nexmark_query_util.AUCTION_TAG]:
        yield auction, existing_person
      return

    new_person = None
    for person in group[nexmark_query_util.PERSON_TAG]:
      if not new_person:
        new_person = person
      else:
        logging.error(
            'two new person wtih same key: %s and %s' % (person, new_person))
        continue
      # read all pending auctions for this person id, output and flush it
      pending_auctions = auction_state.read()
      if pending_auctions:
        for pending_auction in pending_auctions:
          yield pending_auction, new_person
        auction_state.clear()
      # output new auction for this person id
      for auction in group[nexmark_query_util.AUCTION_TAG]:
        yield auction, new_person
      # remember person for max_auction_wait_time seconds for future auctions
      person_state.write(new_person)
      person_timer.set(new_person.date_time + self.max_auction_wait_time)
    # we are done if we have seen a new person
    if new_person:
      return

    # remember auction until we see person
    for auction in group[nexmark_query_util.AUCTION_TAG]:
      auction_state.add(auction)

  @on_timer(person_timer_spec)
  def expiry(self, person_state=beam.DoFn.StateParam(person_spec)):
    person_state.clear()
