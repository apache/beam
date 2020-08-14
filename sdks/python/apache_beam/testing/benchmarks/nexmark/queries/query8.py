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
Query 8, 'Monitor New Users'. Select people who have entered the system and
created auctions in the last 12 hours, updated every 12 hours. In CQL syntax::

  SELECT Rstream(P.id, P.name, A.reserve)
  FROM Person [RANGE 12 HOUR] P, Auction [RANGE 12 HOUR] A
  WHERE P.id = A.seller;

To make things a bit more dynamic and easier to test we'll use a much
shorter window.
"""

from __future__ import absolute_import

import apache_beam as beam
from apache_beam.testing.benchmarks.nexmark.models import id_name_reserve
from apache_beam.testing.benchmarks.nexmark.queries import nexmark_query_util
from apache_beam.transforms import window


def load(events, metadata=None):
  # window person and key by persons' id
  persons_by_id = (
      events
      | nexmark_query_util.JustPerson()
      | 'query8_window_person' >> beam.WindowInto(
          window.FixedWindows(metadata.get('window_size_sec')))
      | 'query8_person_by_id' >> beam.ParDo(nexmark_query_util.PersonByIdFn()))
  # window auction and key by auctions' seller
  auctions_by_seller = (
      events
      | nexmark_query_util.JustAuctions()
      | 'query8_window_auction' >> beam.WindowInto(
          window.FixedWindows(metadata.get('window_size_sec')))
      | 'query8_auction_by_seller' >> beam.ParDo(
          nexmark_query_util.AuctionBySellerFn()))
  return ({
      nexmark_query_util.PERSON_TAG: persons_by_id,
      nexmark_query_util.AUCTION_TAG: auctions_by_seller
  }
          | beam.CoGroupByKey()
          | 'query8_join' >> beam.ParDo(JoinPersonAuctionFn()))


class JoinPersonAuctionFn(beam.DoFn):
  def process(self, element):
    _, group = element
    persons = group[nexmark_query_util.PERSON_TAG]
    person = persons[0] if persons else None
    if person is None:
      # do nothing if this seller id is not a new person in this window
      return
    for auction in group[nexmark_query_util.AUCTION_TAG]:
      yield id_name_reserve.IdNameReserve(
          person.id, person.name, auction.reserve)
