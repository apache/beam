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
import org.apache.beam.sdk.nexmark.model.AuctionPrice;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
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

/** Unit tests for {@link SqlQuery2}. */
@RunWith(Enclosed.class)
public class SqlQuery2Test {

  private static final List<Bid> BIDS =
      ImmutableList.of(
          newBid(1L),
          newBid(2L),
          newBid(3L),
          newBid(4L),
          newBid(5L),
          newBid(6L),
          newBid(7L),
          newBid(8L));

  private static final List<Event> BIDS_EVENTS =
      ImmutableList.of(
          new Event(BIDS.get(0)),
          new Event(BIDS.get(1)),
          new Event(BIDS.get(2)),
          new Event(BIDS.get(3)),
          new Event(BIDS.get(4)),
          new Event(BIDS.get(5)),
          new Event(BIDS.get(6)),
          new Event(BIDS.get(7)));

  private static final List<AuctionPrice> BIDS_EVEN =
      ImmutableList.of(
          newAuctionPrice(BIDS.get(1)),
          newAuctionPrice(BIDS.get(3)),
          newAuctionPrice(BIDS.get(5)),
          newAuctionPrice(BIDS.get(7)));

  private static final List<AuctionPrice> BIDS_EVERY_THIRD =
      ImmutableList.of(newAuctionPrice(BIDS.get(2)), newAuctionPrice(BIDS.get(5)));

  private abstract static class SqlQuery2TestCases {
    protected abstract SqlQuery2 getQuery(long skipFactor);

    @Rule public TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void testSkipsEverySecondElement() {
      PCollection<Event> bids = testPipeline.apply(Create.of(BIDS_EVENTS));
      PAssert.that(bids.apply(getQuery(2))).containsInAnyOrder(BIDS_EVEN);
      testPipeline.run();
    }

    @Test
    public void testSkipsEveryThirdElement() {
      PCollection<Event> bids = testPipeline.apply(Create.of(BIDS_EVENTS));
      PAssert.that(bids.apply(getQuery(3))).containsInAnyOrder(BIDS_EVERY_THIRD);
      testPipeline.run();
    }
  }

  @RunWith(JUnit4.class)
  public static class SqlQuery2TestCalcite extends SqlQuery2TestCases {
    @Override
    protected SqlQuery2 getQuery(long skipFactor) {
      return SqlQuery2.calciteSqlQuery2(skipFactor);
    }
  }

  @RunWith(JUnit4.class)
  public static class SqlQuery2TestZetaSql extends SqlQuery2TestCases {
    @Override
    protected SqlQuery2 getQuery(long skipFactor) {
      return SqlQuery2.zetaSqlQuery2(skipFactor);
    }
  }

  private static Bid newBid(long id) {
    return new Bid(id, 3L, 100L, new Instant(432342L + id), "extra_" + id);
  }

  private static AuctionPrice newAuctionPrice(Bid bid) {
    return new AuctionPrice(bid.auction, bid.price);
  }
}
