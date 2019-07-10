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
package org.apache.beam.sdk.extensions.timeseries.example;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.timeseries.joins.BiTemporalJoinResult;
import org.apache.beam.sdk.extensions.timeseries.joins.BiTemporalStreams;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This sample will take two streams a Trade stream and a Quote stream. It will match the Trade to
 * the nearest Quote based on the timestamp. Nearest trade is either <= to the quote timestamp.
 *
 * <p>If two values in the right steam have the same timestamp then the results are
 * non-deterministic.
 */
public class BiTemporalJoinExample {

  private static final Logger LOG = LoggerFactory.getLogger(BiTemporalJoinExample.class);

  static Instant now = Instant.parse("2000-01-01T00:00:00Z");

  public static void main(String[] args) {

    // Mock trade data for left stream
    List<TimestampedValue<TradeData>> tradeStream = generateTradeData();

    // Mock Quote data for right stream
    List<TimestampedValue<QuoteData>> quoteStream = generateQuoteData();

    // Setup Pipeline

    Pipeline p = Pipeline.create();

    // Create Tuple Tags for the Object types

    TupleTag<TradeData> tradeTag = new TupleTag() {};

    TupleTag<QuoteData> quoteTag = new TupleTag() {};

    PCollection<KV<String, QuoteData>> quotes =
        p.apply("Create Quotes", Create.timestamped(quoteStream))
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
            "BiTemporalJoin",
            BiTemporalStreams.<String, TradeData, QuoteData>join(
                tradeTag, quoteTag, Duration.standardHours(1)));

    stream.apply(
        ParDo.of(
            new DoFn<BiTemporalJoinResult<String, TradeData, QuoteData>, String>() {

              @ProcessElement
              public void process(
                  @Element BiTemporalJoinResult<String, TradeData, QuoteData> input) {
                if (input.getMatched()) {
                  System.out.println(
                      String.format(
                          "Trade %s matches Quote %s",
                          input.getLeftData().getValue().id, input.getRightData().getValue().id));
                } else {
                  System.out.println(
                      String.format("No Match for Trade %s", input.getLeftData().getValue().id));
                }
              }
            }));

    p.run();
  }

  public static List<TimestampedValue<TradeData>> generateTradeData() {
    // Mock trade data for left stream
    TimestampedValue<TradeData> tradeBeforeQuote =
        TimestampedValue.of(
            createTradeData(
                "FX_1", "Trade@1999-12-31T23:59:00.000", now.minus(Duration.standardMinutes(1))),
            now.minus(Duration.standardMinutes(1)));

    TimestampedValue<TradeData> tradeAfterQuoteButBeforeEndOfQuotes =
        TimestampedValue.of(
            createTradeData(
                "FX_1", "Trade@2000-01-01T00:15:00.000", now.plus(Duration.standardMinutes(15))),
            now.plus(Duration.standardMinutes(15)));

    TimestampedValue<TradeData> tradeAfterAllQuotesButInWindow =
        TimestampedValue.of(
            createTradeData(
                "FX_1", "Trade@2000-01-01T00:35:00.000", now.plus(Duration.standardMinutes(35))),
            now.plus(Duration.standardMinutes(35)));

    TimestampedValue<TradeData> tradeAfterWindowClosed =
        TimestampedValue.of(
            createTradeData(
                "FX_1", "Trade@2000-01-01T01:15:00.000", now.plus(Duration.standardMinutes(75))),
            now.plus(Duration.standardMinutes(75)));

    return ImmutableList.of(
        tradeBeforeQuote,
        tradeAfterAllQuotesButInWindow,
        tradeAfterQuoteButBeforeEndOfQuotes,
        tradeAfterWindowClosed);
  }

  public static List<TimestampedValue<QuoteData>> generateQuoteData() {

    TimestampedValue<QuoteData> quoteTenMinutes =
        TimestampedValue.of(
            createQuoteData(
                "FX_1", "Quote@2000-01-01T00:10:00.000", now.plus(Duration.standardMinutes(10))),
            now.plus(Duration.standardMinutes(10)));

    // Mock trade data for left stream
    TimestampedValue<QuoteData> quoteTwentyMinutes =
        TimestampedValue.of(
            createQuoteData(
                "FX_1", "Quote@2000-01-01T00:20:00.000", now.plus(Duration.standardMinutes(20))),
            now.plus(Duration.standardMinutes(20)));

    return ImmutableList.of(quoteTenMinutes, quoteTwentyMinutes);
  }

  @DefaultCoder(AvroCoder.class)
  public static class TradeData extends SomeData {

    public TradeData() {}

    public TradeData(String type, String key, String id, Long timestamp) {
      super(type, key, id, timestamp);
    }
  }

  @DefaultCoder(AvroCoder.class)
  public static class QuoteData extends SomeData {

    public QuoteData() {}

    public QuoteData(String type, String key, String id, Long timestamp) {
      super(type, key, id, timestamp);
    }
  };

  @DefaultCoder(AvroCoder.class)
  public static class SomeData {

    public SomeData() {}

    public SomeData(String type, String key, String id, Long timestamp) {
      this.type = type;
      this.id = id;
      this.timestamp = timestamp;
      this.key = key;
    }

    String type;
    String id;
    Long timestamp;
    String key;

    @Override
    public String toString() {
      return String.format("id: %s Time: %s Type:%s", id, new Instant(timestamp), type);
    }
  }

  public static TradeData createTradeData(String key, String id, Instant time) {
    return new TradeData("Trade", key, id, time.getMillis());
  }

  public static QuoteData createQuoteData(String key, String id, Instant time) {
    return new QuoteData("Quote", key, id, time.getMillis());
  }
}
