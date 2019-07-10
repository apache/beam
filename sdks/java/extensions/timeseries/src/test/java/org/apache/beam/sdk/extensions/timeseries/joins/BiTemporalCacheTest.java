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

import java.io.Serializable;
import org.apache.beam.sdk.extensions.timeseries.joins.BiTemporalTestUtils.QuoteData;
import org.apache.beam.sdk.extensions.timeseries.joins.BiTemporalTestUtils.TradeData;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BiTemporalCacheTest implements Serializable {

  static final int TRADE_QUOTE_SIZE = 2000;
  static final Duration WINDOW_DURATION = Duration.standardDays(30);
  private static final Logger LOG = LoggerFactory.getLogger(BiTemporalCacheTest.class);

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void cacheTest() {

    // We will start our timer at 1 sec from the fixed upper boundary of our minute window
    Instant now = Instant.parse("2000-01-01T00:00:00Z");

    LOG.info("Duration is set to days {}", WINDOW_DURATION);

    PCollection<KV<String, BiTemporalTestUtils.QuoteData>> quotes =
        // ----- Stream A
        p.apply("Create Quotes", Create.of(1))
            .apply(
                "Generate Quotes",
                ParDo.of(
                    new DoFn<Integer, KV<String, BiTemporalTestUtils.QuoteData>>() {

                      @ProcessElement
                      public void process(
                          @Element Integer m,
                          OutputReceiver<KV<String, BiTemporalTestUtils.QuoteData>> o) {

                        for (int i = 0; i < TRADE_QUOTE_SIZE; ++i) {

                          Instant time = now.plus(Duration.millis(i * 100));

                          QuoteData dataKey1 =
                              BiTemporalTestUtils.createQuoteData("FX_1", "Quote#" + time, time);
                          o.outputWithTimestamp(
                              KV.of("FX_1", dataKey1), new Instant(dataKey1.timestamp));

                          QuoteData dataKey2 =
                              BiTemporalTestUtils.createQuoteData("FX_2", "Quote#" + time, time);
                          o.outputWithTimestamp(
                              KV.of("FX_2", dataKey2), new Instant(dataKey2.timestamp));
                        }
                      }
                    }));

    PCollection<KV<String, TradeData>> trades =
        p.apply("Create Trades", Create.of(1))
            .apply(
                "Generate Trades",
                ParDo.of(
                    new DoFn<Integer, KV<String, TradeData>>() {

                      // ----- Stream B
                      @ProcessElement
                      public void process(
                          @Element Integer m, OutputReceiver<KV<String, TradeData>> o) {

                        for (int i = 0; i < TRADE_QUOTE_SIZE; ++i) {

                          Instant time = now.plus(Duration.millis(i * 100));

                          TradeData dataKey1 =
                              BiTemporalTestUtils.createTradeData("FX_1", "Trade#" + time, time);
                          o.outputWithTimestamp(
                              KV.of("FX_1", dataKey1), new Instant(dataKey1.timestamp));

                          TradeData dataKey2 =
                              BiTemporalTestUtils.createTradeData("FX_2", "Trade#" + time, time);
                          o.outputWithTimestamp(
                              KV.of("FX_2", dataKey2), new Instant(dataKey2.timestamp));
                        }
                      }
                    }));

    TupleTag<TradeData> tradeTag = new TupleTag() {};
    TupleTag<QuoteData> quoteTag = new TupleTag() {};

    KeyedPCollectionTuple<String> kct =
        KeyedPCollectionTuple.of(tradeTag, trades).and(quoteTag, quotes);

    PCollection<BiTemporalJoinResult<String, TradeData, QuoteData>> stream =
        kct.apply(
            "",
            BiTemporalStreams.<String, TradeData, QuoteData>join(
                tradeTag, quoteTag, WINDOW_DURATION));

    PCollection<Integer> matches =
        stream
            .apply(
                ParDo.of(
                    new DoFn<BiTemporalJoinResult<String, TradeData, QuoteData>, Integer>() {

                      @ProcessElement
                      public void process(
                          @Element BiTemporalJoinResult<String, TradeData, QuoteData> m,
                          OutputReceiver<Integer> o) {
                        if (m.getMatched()) {
                          o.output(1);
                        }
                      }
                    }))
            .apply(Sum.integersGlobally().withoutDefaults());

    PAssert.that(matches).containsInAnyOrder(TRADE_QUOTE_SIZE * 2);

    p.run();
  }
}
