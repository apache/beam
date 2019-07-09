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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class BiTemporalTestUtils {

  public static class BiTemporalTest
      extends PTransform<
          PCollection<BiTemporalJoinResult<String, TradeData, QuoteData>>, PCollection<Integer>> {

    @Override
    public PCollection<Integer> expand(
        PCollection<BiTemporalJoinResult<String, TradeData, QuoteData>> input) {

      return input
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
    }
  }

  public static List<TimestampedValue<QuoteData>> createQuotesList(Instant now) {

    List<TimestampedValue<QuoteData>> rightStream = new ArrayList<>();
    // Create a id every sec for 30 mins
    for (int i = 0; i < 1800; ++i) {

      Instant time = now.plus(Duration.standardSeconds(i));
      rightStream.add(
          TimestampedValue.of(BiTemporalTestUtils.createQuoteData("Quote", "FX_1", time), time));
    }

    return rightStream;
  }

  @DefaultCoder(AvroCoder.class)
  public static class TradeData extends SomeData {

    public TradeData() {}

    public TradeData(String type, String key, String id, Long timestamp) {
      super(type, key, id, timestamp);
    }
  };

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
