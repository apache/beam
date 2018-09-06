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

import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/** Unit tests for {@link SqlQuery1}. */
public class SqlQuery1Test {

  private static final Bid BID1_USD = new Bid(5L, 3L, 100L, new Instant(43234234L), "extra1");

  private static final Bid BID2_USD = new Bid(6L, 4L, 500L, new Instant(13234234L), "extra2");

  private static final Bid BID1_EUR = new Bid(5L, 3L, 89L, new Instant(43234234L), "extra1");

  private static final Bid BID2_EUR = new Bid(6L, 4L, 445L, new Instant(13234234L), "extra2");

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testDolToEurConversion() {
    SqlQuery1.DolToEur dolToEur = new SqlQuery1.DolToEur();
    assertEquals(Long.valueOf(445), dolToEur.apply(500L));
  }

  @Test
  public void testConvertsPriceToEur() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    PCollection<Event> bids =
        testPipeline.apply(
            TestStream.create(
                    registry.getSchema(Event.class),
                    registry.getToRowFunction(Event.class),
                    registry.getFromRowFunction(Event.class))
                .addElements(new Event(BID1_USD))
                .addElements(new Event(BID2_USD))
                .advanceWatermarkToInfinity());

    PAssert.that(bids.apply(new SqlQuery1())).containsInAnyOrder(BID1_EUR, BID2_EUR);

    testPipeline.run();
  }
}
