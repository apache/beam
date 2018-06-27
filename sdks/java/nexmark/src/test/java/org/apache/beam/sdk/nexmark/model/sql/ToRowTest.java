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

package org.apache.beam.sdk.nexmark.model.sql;

import static org.apache.beam.sdk.nexmark.model.sql.adapter.ModelAdaptersMapping.ADAPTERS;

import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.model.sql.adapter.ModelFieldsAdapter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link ToRow}. */
public class ToRowTest {
  private static final Person PERSON =
      new Person(3L, "name", "email", "cc", "city", "state", 329823L, "extra");

  private static final Bid BID = new Bid(5L, 3L, 123123L, 43234234L, "extra2");

  private static final Auction AUCTION =
      new Auction(5L, "item", "desc", 342L, 321L, 3423342L, 2349234L, 3L, 1L, "extra3");

  @Rule public TestPipeline testPipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testConvertsBids() throws Exception {
    PCollection<Event> bids =
        testPipeline.apply(
            TestStream.create(Event.CODER)
                .addElements(new Event(BID))
                .advanceWatermarkToInfinity());

    Row expectedBidRow = toRow(BID);

    PAssert.that(bids.apply(ToRow.parDo())).containsInAnyOrder(expectedBidRow);

    testPipeline.run();
  }

  @Test
  public void testConvertsPeople() throws Exception {
    PCollection<Event> people =
        testPipeline.apply(
            TestStream.create(Event.CODER)
                .addElements(new Event(PERSON))
                .advanceWatermarkToInfinity());

    Row expectedPersonRow = toRow(PERSON);

    PAssert.that(people.apply(ToRow.parDo())).containsInAnyOrder(expectedPersonRow);

    testPipeline.run();
  }

  @Test
  public void testConvertsAuctions() throws Exception {
    PCollection<Event> auctions =
        testPipeline.apply(
            TestStream.create(Event.CODER)
                .addElements(new Event(AUCTION))
                .advanceWatermarkToInfinity());

    Row expectedAuctionRow = toRow(AUCTION);

    PAssert.that(auctions.apply(ToRow.parDo())).containsInAnyOrder(expectedAuctionRow);

    testPipeline.run();
  }

  private static Row toRow(Object obj) {
    ModelFieldsAdapter adapter = ADAPTERS.get(obj.getClass());
    return Row.withSchema(adapter.getSchema()).addValues(adapter.getFieldsValues(obj)).build();
  }
}
