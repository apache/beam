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
package org.apache.beam.sdk.testing.watermarks;

import java.util.ArrayList;
import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class defines the Apache Beam pipeline that was used to benchmark the watermark subsystems
 * of Google Cloud Dataflow and Apache Flink for the paper "Watermarks in Stream Processing Systems:
 * Semantics and Comparative Analysis of Apache Flink and Google Cloud Dataflow", submitted for the
 * Industrial Track of VLDB 2021 by Tyler Akidau, Edmon Begoli, Slava Chernyak, Fabian Hueske,
 * Kathryn Knight, Kenneth Knowles, Daniel Mills, and Dan Sotolongo.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WatermarkLatency {
  private static final TupleTag<KV<Long, Instant>> output = new TupleTag<KV<Long, Instant>>() {};
  private static final TupleTag<KV<String, Duration>> latencyResult =
      new TupleTag<KV<String, Duration>>() {};
  private static final Logger LOG = LoggerFactory.getLogger(WatermarkLatency.class);

  public interface WatermarkLatencyOptions extends PipelineOptions {
    @Default.Integer(3)
    Integer getNumShuffles();

    void setNumShuffles(Integer value);

    @Default.Integer(1000)
    Integer getInputRatePerSec();

    void setInputRatePerSec(Integer value);

    @Default.Integer(1000)
    Integer getNumKeys();

    void setNumKeys(Integer value);

    @Default.String("Default")
    String getConfigName();

    void setConfigName(String value);

    @Default.String("")
    String getOutputPath();

    void setOutputPath(String value);
  }

  static void run(WatermarkLatencyOptions options) {
    Pipeline p = Pipeline.create(options);

    Duration period = Duration.standardSeconds(1);
    final int numKeys = options.getNumKeys();
    final String configName = options.getConfigName();

    PCollection<KV<Long, Instant>> input =
        p.apply("Generate", GenerateSequence.from(0).withRate(options.getInputRatePerSec(), period))
            .apply(
                ParDo.of(
                    new DoFn<Long, KV<Long, Instant>>() {
                      @ProcessElement
                      public void process(ProcessContext c) {
                        Instant now = Instant.now();
                        c.output(KV.of(c.element() % numKeys, now));
                      }
                    }))
            .apply(
                Window.<KV<Long, Instant>>into(FixedWindows.of(Duration.standardSeconds(1)))
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO));

    PCollectionList<KV<String, Duration>> latencyList =
        PCollectionList.<KV<String, Duration>>empty(p);
    for (int i = 0; i < options.getNumShuffles(); i++) {
      final int idx = i;
      PCollectionTuple tup =
          input
              .apply(GroupByKey.create())
              .apply(
                  ParDo.of(
                          new DoFn<KV<Long, Iterable<Instant>>, KV<Long, Instant>>() {
                            @ProcessElement
                            public void process(ProcessContext c, @Timestamp Instant ts) {
                              Instant now = Instant.now();
                              Instant lastGBKTs = Instant.ofEpochMilli(0L);
                              // forward records to next window
                              for (Instant v : c.element().getValue()) {
                                lastGBKTs = v;
                                // enforce re-shuffling by changing keys
                                c.output(KV.of(c.element().getKey() + 1, now));
                              }
                              if (idx > 0) {
                                // compute delay since last shuffle and emit result to side output
                                Duration sessionDelay = new Duration(lastGBKTs, now);
                                c.output(
                                    latencyResult,
                                    KV.of(
                                        String.format("GBK%d-GBK%d", idx - 1, idx), sessionDelay));
                              }
                            }
                          })
                      .withOutputTags(output, TupleTagList.of(latencyResult)));

      latencyList = latencyList.and(tup.get(latencyResult));
      input = tup.get(output);
    }

    PCollectionList<String> collectionList = PCollectionList.<String>empty(p);
    for (PCollection<KV<String, Duration>> latency : latencyList.getAll()) {
      collectionList =
          collectionList.and(
              latency
                  .apply(
                      Window.<KV<String, Duration>>into(
                              FixedWindows.of(Duration.standardMinutes(1)))
                          .triggering(AfterWatermark.pastEndOfWindow())
                          .discardingFiredPanes()
                          .withAllowedLateness(Duration.ZERO))
                  .apply(GroupByKey.create())
                  .apply(
                      ParDo.of(
                          new DoFn<KV<String, Iterable<Duration>>, String>() {

                            Duration median = null;
                            Duration p75 = null;
                            Duration p95 = null;
                            Duration p99 = null;
                            int numElements = -1;

                            @ProcessElement
                            public void process(ProcessContext c) {

                              computePercentiles(c.element().getValue());

                              if (numElements < 0) {
                                return;
                              }

                              String out =
                                  String.format(
                                      "%s, %s, %d, %d, %d, %d, %d",
                                      configName,
                                      c.element().getKey(),
                                      median.getMillis(),
                                      p75.getMillis(),
                                      p95.getMillis(),
                                      p99.getMillis(),
                                      numElements);
                              LOG.info(out);
                            }

                            private void computePercentiles(Iterable<Duration> vals) {
                              numElements = -1;
                              ArrayList<Duration> accumulator = new ArrayList<>(6000);

                              for (Duration v : vals) {
                                accumulator.add(v);
                              }
                              if (accumulator.isEmpty()) {
                                return;
                              }

                              // Compute the median of the available points.
                              int medianIndex = (int) Math.floor(accumulator.size() * 0.5);
                              int p75Index = (int) Math.floor(accumulator.size() * 0.75);
                              int p95Index = (int) Math.floor(accumulator.size() * 0.95);
                              int p99Index = (int) Math.floor(accumulator.size() * 0.99);

                              if (medianIndex < 0
                                  || medianIndex >= accumulator.size()
                                  || p75Index < 0
                                  || p75Index >= accumulator.size()
                                  || p95Index < 0
                                  || p95Index >= accumulator.size()
                                  || p99Index < 0
                                  || p99Index >= accumulator.size()) {
                                LOG.info("Computed bogus index");
                                return;
                              }

                              Collections.sort(accumulator);

                              median = accumulator.get(medianIndex);
                              p75 = accumulator.get(p75Index);
                              p95 = accumulator.get(p95Index);
                              p99 = accumulator.get(p99Index);
                              numElements = accumulator.size();
                            }
                          })));
    }

    // Run pipeline
    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    WatermarkLatencyOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(WatermarkLatencyOptions.class);

    run(options);
  }
}
