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
package org.apache.beam.sdk.extensions.timeseries.joins;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.timeseries.joins.BiTemporalTestUtils.QuoteData;
import org.apache.beam.sdk.extensions.timeseries.joins.BiTemporalTestUtils.TradeData;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/** */
public class BiTemporalFunctionalTests implements Serializable {

  static TupleTag<TradeData> tradeTag = new TupleTag() {};
  static TupleTag<QuoteData> quoteTag = new TupleTag() {};
  static final Duration WINDOW_DURATION = Duration.standardDays(30);

  @Rule public transient TestPipeline p = TestPipeline.create();

  // We will start our timer at 1 sec from the fixed upper boundary of our minute window
  Instant now = Instant.parse("2000-01-01T00:00:00Z");

  @Test
  public void leftStreamBeforeAnyRightStreamValues() {

    Instant now = Instant.parse("2000-01-01T00:00:00Z");

    TimestampedValue<TradeData> tradeStream_BeforeAnyValues =
        TimestampedValue.of(
            BiTemporalTestUtils.createTradeData(
                "FX_1", "Trade@23:59:00.000", now.minus(Duration.standardMinutes(1))),
            now.minus(Duration.standardMinutes(1)));

    List<TimestampedValue<TradeData>> tradeStream = ImmutableList.of(tradeStream_BeforeAnyValues);

    List<TimestampedValue<QuoteData>> rightStream = BiTemporalTestUtils.createQuotesList(now);

    PCollection<KV<String, QuoteData>> quotes =
        p.apply("Create Quotes", Create.timestamped(rightStream))
            .setCoder(AvroCoder.of(QuoteData.class))
            .apply("Quote Key", WithKeys.of("FX_1"));

    PCollection<KV<String, TradeData>> trades =
        p.apply("Create Trades", Create.timestamped(tradeStream))
            .setCoder(AvroCoder.of(TradeData.class))
            .apply("Trade Key", WithKeys.of("FX_1"));

    KeyedPCollectionTuple<String> kct =
        KeyedPCollectionTuple.of(tradeTag, trades).and(quoteTag, quotes);

    PCollection<BiTemporalJoinResult<String, TradeData, QuoteData>> stream =
        kct.apply(
            "BiTemporalStreams",
            BiTemporalStreams.<String, TradeData, QuoteData>join(
                tradeTag, quoteTag, WINDOW_DURATION));

    PAssert.that(stream.apply(new BiTemporalTestUtils.BiTemporalTest())).empty();

    p.run();
  }

  @Test
  public void leftValuesEqualToAndCloseToRightStreamValues() {

    TimestampedValue<TradeData> tradeStream_MiddleOfValues_Exact =
        TimestampedValue.of(
            BiTemporalTestUtils.createTradeData(
                "FX_1", "Trade@00:15:00.000", now.plus(Duration.standardMinutes(15))),
            now.plus(Duration.standardMinutes(15)));

    TimestampedValue<TradeData> tradeStream_MiddleOfValues_MidPoint =
        TimestampedValue.of(
            BiTemporalTestUtils.createTradeData(
                "FX_1",
                "Trade@00:15:00.500",
                now.plus(Duration.standardMinutes(15)).plus(Duration.millis(500))),
            now.plus(Duration.standardMinutes(15)).plus(Duration.millis(500)));

    List<TimestampedValue<TradeData>> tradeStream =
        ImmutableList.of(tradeStream_MiddleOfValues_Exact, tradeStream_MiddleOfValues_MidPoint);

    List<TimestampedValue<QuoteData>> rightStream = BiTemporalTestUtils.createQuotesList(now);

    PCollection<KV<String, QuoteData>> quotes =
        p.apply("Create Quotes", Create.timestamped(rightStream))
            .setCoder(AvroCoder.of(QuoteData.class))
            .apply("Quote Key", WithKeys.of("FX_1"));

    PCollection<KV<String, TradeData>> trades =
        p.apply("Create Trades", Create.timestamped(tradeStream))
            .setCoder(AvroCoder.of(TradeData.class))
            .apply("Trade Key", WithKeys.of("FX_1"));

    KeyedPCollectionTuple<String> kct =
        KeyedPCollectionTuple.of(tradeTag, trades).and(quoteTag, quotes);

    PCollection<BiTemporalJoinResult<String, TradeData, QuoteData>> stream =
        kct.apply(
            "BiTemporalStreams",
            BiTemporalStreams.<String, TradeData, QuoteData>join(
                tradeTag, quoteTag, WINDOW_DURATION));

    PAssert.that(stream.apply(new BiTemporalTestUtils.BiTemporalTest())).containsInAnyOrder(2);

    p.run();
  }

  @Test
  public void leftStreamValuesAfterTheLastRightStreamValues() {

    TimestampedValue<TradeData> tradeStream_AfterAllValues =
        TimestampedValue.of(
            BiTemporalTestUtils.createTradeData(
                "FX_1", "Trade@00:35:00.000", now.plus(Duration.standardMinutes(35))),
            now.plus(Duration.standardMinutes(35)));

    List<TimestampedValue<TradeData>> tradeStream = ImmutableList.of(tradeStream_AfterAllValues);

    List<TimestampedValue<QuoteData>> rightStream = BiTemporalTestUtils.createQuotesList(now);

    PCollection<KV<String, QuoteData>> quotes =
        p.apply("Create Quotes", Create.timestamped(rightStream))
            .setCoder(AvroCoder.of(QuoteData.class))
            .apply("Quote Key", WithKeys.of("FX_1"));

    PCollection<KV<String, TradeData>> trades =
        p.apply("Create Trades", Create.timestamped(tradeStream))
            .setCoder(AvroCoder.of(TradeData.class))
            .apply("Trade Key", WithKeys.of("FX_1"));

    KeyedPCollectionTuple<String> kct =
        KeyedPCollectionTuple.of(tradeTag, trades).and(quoteTag, quotes);

    PCollection<BiTemporalJoinResult<String, TradeData, QuoteData>> stream =
        kct.apply(
            "BiTemporalStreams",
            BiTemporalStreams.<String, TradeData, QuoteData>join(
                tradeTag, quoteTag, WINDOW_DURATION));

    PAssert.that(stream.apply(new BiTemporalTestUtils.BiTemporalTest())).containsInAnyOrder(1);

    p.run();
  }
}
