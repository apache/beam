/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.nexmark.queries.sql;

import java.util.List;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.NameCityStateId;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SqlQuery3}. */
@RunWith(Enclosed.class)
public class SqlQuery3Test {

  private static final List<Person> PEOPLE =
      ImmutableList.of(
          newPerson(0L, "WA"),
          newPerson(1L, "CA"), // matches query
          newPerson(2L, "OR"), // matches query
          newPerson(3L, "ID"), // matches query
          newPerson(4L, "NY"));

  private static final List<Auction> AUCTIONS =
      ImmutableList.of(
          newAuction(0L, 0L, 5L),
          newAuction(1L, 1L, 10L), // matches query
          newAuction(2L, 2L, 5L),
          newAuction(3L, 3L, 10L), // matches query
          newAuction(4L, 4L, 5L),
          newAuction(5L, 0L, 5L),
          newAuction(6L, 1L, 10L), // matches query
          newAuction(7L, 2L, 5L),
          newAuction(8L, 3L, 10L), // matches query
          newAuction(9L, 4L, 5L));

  private static final List<Event> PEOPLE_AND_AUCTIONS_EVENTS =
      ImmutableList.of(
          new Event(PEOPLE.get(0)),
          new Event(AUCTIONS.get(0)),
          new Event(PEOPLE.get(1)),
          new Event(AUCTIONS.get(1)),
          new Event(PEOPLE.get(2)),
          new Event(AUCTIONS.get(2)),
          new Event(PEOPLE.get(3)),
          new Event(AUCTIONS.get(3)),
          new Event(AUCTIONS.get(4)),
          new Event(AUCTIONS.get(5)),
          new Event(AUCTIONS.get(6)),
          new Event(PEOPLE.get(4)),
          new Event(AUCTIONS.get(2)),
          new Event(AUCTIONS.get(7)),
          new Event(AUCTIONS.get(8)),
          new Event(AUCTIONS.get(9)));

  public static final List<NameCityStateId> RESULTS =
      ImmutableList.of(
          new NameCityStateId("name_1", "city_1", "CA", 1L),
          new NameCityStateId("name_3", "city_3", "ID", 3L),
          new NameCityStateId("name_1", "city_1", "CA", 6L),
          new NameCityStateId("name_3", "city_3", "ID", 8L));

  private abstract static class SqlQuery3TestCases {
    protected abstract SqlQuery3 getQuery(NexmarkConfiguration configuration);

    @Rule public TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void testJoinsPeopleWithAuctions() throws Exception {
      PCollection<Event> events = testPipeline.apply(Create.of(PEOPLE_AND_AUCTIONS_EVENTS));
      PAssert.that(events.apply(getQuery(new NexmarkConfiguration()))).containsInAnyOrder(RESULTS);
      testPipeline.run();
    }
  }

  @RunWith(JUnit4.class)
  public static class SqlQuery3TestCalcite extends SqlQuery3TestCases {
    @Override
    protected SqlQuery3 getQuery(NexmarkConfiguration configuration) {
      return SqlQuery3.calciteSqlQuery3(configuration);
    }
  }

  @RunWith(JUnit4.class)
  public static class SqlQuery3TestZetaSql extends SqlQuery3TestCases {
    @Override
    protected SqlQuery3 getQuery(NexmarkConfiguration configuration) {
      return SqlQuery3.calciteSqlQuery3(configuration);
    }
  }

  private static Person newPerson(long id, String state) {
    return new Person(
        id,
        "name_" + id,
        "email_" + id,
        "cc_" + id,
        "city_" + id,
        state,
        new Instant(123123L + id),
        "extra_" + id);
  }

  private static Auction newAuction(long id, long seller, long category) {
    return new Auction(
        id,
        "item_" + id,
        "desc_" + id,
        123 + id,
        200 + id,
        new Instant(123123L + id),
        new Instant(223123 + id),
        seller,
        category,
        "extra_" + id);
  }
}
