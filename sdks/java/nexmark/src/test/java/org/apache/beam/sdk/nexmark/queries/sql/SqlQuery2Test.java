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

import static org.apache.beam.sdk.nexmark.model.sql.adapter.ModelAdaptersMapping.ADAPTERS;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.sql.adapter.ModelFieldsAdapter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for {@link SqlQuery2}.
 */
public class SqlQuery2Test {

  private static final ModelFieldsAdapter<Bid> BID_ADAPTER = ADAPTERS.get(Bid.class);

  private static final List<Bid> BIDS = ImmutableList.of(
      newBid(1L),
      newBid(2L),
      newBid(3L),
      newBid(4L),
      newBid(5L),
      newBid(6L),
      newBid(7L),
      newBid(8L));

  private static final List<Event> BIDS_EVENTS = ImmutableList.of(
      new Event(BIDS.get(0)),
      new Event(BIDS.get(1)),
      new Event(BIDS.get(2)),
      new Event(BIDS.get(3)),
      new Event(BIDS.get(4)),
      new Event(BIDS.get(5)),
      new Event(BIDS.get(6)),
      new Event(BIDS.get(7)));

  private static final List<Row> BIDS_EVEN_ROWS = ImmutableList.of(
      newBidRow(BIDS.get(1)),
      newBidRow(BIDS.get(3)),
      newBidRow(BIDS.get(5)),
      newBidRow(BIDS.get(7)));

  private static final List<Row> BIDS_EVERY_THIRD_ROW = ImmutableList.of(
      newBidRow(BIDS.get(2)),
      newBidRow(BIDS.get(5)));


  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testSkipsEverySecondElement() throws Exception {
    PCollection<Event> bids =
        PBegin
            .in(testPipeline)
            .apply(Create.of(BIDS_EVENTS).withCoder(Event.CODER));

    PAssert
        .that(bids.apply(new SqlQuery2(2)))
        .containsInAnyOrder(BIDS_EVEN_ROWS);

    testPipeline.run();
  }

  @Test
  public void testSkipsEveryThirdElement() throws Exception {
    PCollection<Event> bids =
        PBegin
            .in(testPipeline)
            .apply(Create.of(BIDS_EVENTS).withCoder(Event.CODER));

    PAssert
        .that(bids.apply(new SqlQuery2(3)))
        .containsInAnyOrder(BIDS_EVERY_THIRD_ROW);

    testPipeline.run();
  }

  private static Bid newBid(long id) {
    return new Bid(id, 3L, 100L, 432342L + id, "extra_" + id);
  }

  private static Row newBidRow(Bid bid) {
    return
        Row
            .withRowType(BID_ADAPTER.getRowType())
            .addValues(BID_ADAPTER.getFieldsValues(bid))
            .build();
  }
}
