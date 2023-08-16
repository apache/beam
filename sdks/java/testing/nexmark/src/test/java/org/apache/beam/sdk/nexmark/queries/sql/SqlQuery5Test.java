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

import static org.junit.Assert.assertEquals;

import java.util.List;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.AuctionCount;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

/** Unit tests for {@link SqlQuery5}. */
public class SqlQuery5Test {

  private static final NexmarkConfiguration config = new NexmarkConfiguration();

  private static final List<Bid> BIDS =
      ImmutableList.of(newBid(1L, 1L), newBid(1L, 3L), newBid(1L, 4L), newBid(2L, 4L));

  private static final List<Event> BIDS_EVENTS =
      ImmutableList.of(
          new Event(BIDS.get(0)),
          new Event(BIDS.get(1)),
          new Event(BIDS.get(2)),
          new Event(BIDS.get(3)));

  public static final List<AuctionCount> RESULTS =
      ImmutableList.of(
          new AuctionCount(1L, 1L),
          new AuctionCount(1L, 1L),
          new AuctionCount(1L, 1L),
          new AuctionCount(1L, 2L),
          new AuctionCount(1L, 1L),
          new AuctionCount(2L, 1L));

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Test
  @Ignore("https://github.com/apache/beam/issues/19541")
  public void testBids() throws Exception {
    assertEquals(Long.valueOf(config.windowSizeSec), Long.valueOf(config.windowPeriodSec * 2));

    PCollection<Event> bids = testPipeline.apply(Create.of(BIDS_EVENTS));

    PAssert.that(bids.apply(new SqlQuery5(config))).containsInAnyOrder(RESULTS);

    testPipeline.run();
  }

  private static Bid newBid(long auction, long index) {
    return new Bid(
        auction,
        3L,
        100L,
        new Instant(432342L + index * config.windowPeriodSec * 1000),
        "extra_" + auction);
  }
}
