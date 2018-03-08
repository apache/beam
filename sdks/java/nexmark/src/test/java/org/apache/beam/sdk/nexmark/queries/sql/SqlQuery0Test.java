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

import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.sql.adapter.ModelAdaptersMapping;
import org.apache.beam.sdk.nexmark.model.sql.adapter.ModelFieldsAdapter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for {@link SqlQuery0}.
 */
public class SqlQuery0Test {

  private static final Bid BID1 =
      new Bid(5L, 3L, 123123L, 43234234L, "extra1");

  private static final Bid BID2 =
      new Bid(6L, 4L, 134123L, 13234234L, "extra2");

  private static final ModelFieldsAdapter<Bid> BID_ADAPTER =
      ModelAdaptersMapping.ADAPTERS.get(Bid.class);

  @Rule
  public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testPassesBidsThrough() throws Exception {
    PCollection<Event> bids = testPipeline.apply(
        TestStream.create(Event.CODER)
            .addElements(new Event(BID1))
            .addElements(new Event(BID2))
            .advanceWatermarkToInfinity());

    PAssert
        .that(bids.apply(new SqlQuery0()))
        .containsInAnyOrder(BID1, BID2);

    testPipeline.run();
  }
}
