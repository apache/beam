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
package org.apache.beam.runners.flink.adapter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Map;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

public class BeamFlinkDataStreamAdapterTest {

  private static PTransform<PCollection<? extends String>, PCollection<String>> withPrefix(
      String prefix) {
    return ParDo.of(
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(@Element String word, OutputReceiver<String> out) {
            out.output(prefix + word);
          }
        });
  }

  @Test
  public void testApplySimpleTransform() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

    DataStream<String> input = env.fromCollection(ImmutableList.of("a", "b", "c"));
    DataStream<String> result =
        new BeamFlinkDataStreamAdapter().applyBeamPTransform(input, withPrefix("x"));

    assertThat(
        ImmutableList.copyOf(result.executeAndCollect()), containsInAnyOrder("xa", "xb", "xc"));
  }

  @Test
  public void testApplyCompositeTransform() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

    DataStream<String> input = env.fromCollection(ImmutableList.of("a", "b", "c"));
    DataStream<String> result =
        new BeamFlinkDataStreamAdapter()
            .applyBeamPTransform(
                input,
                new PTransform<PCollection<String>, PCollection<String>>() {
                  @Override
                  public PCollection<String> expand(PCollection<String> input) {
                    return input.apply(withPrefix("x")).apply(withPrefix("y"));
                  }
                });

    assertThat(
        ImmutableList.copyOf(result.executeAndCollect()), containsInAnyOrder("yxa", "yxb", "yxc"));
  }

  @Test
  public void testApplyMultiInputTransform() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

    DataStream<String> input1 = env.fromCollection(ImmutableList.of("a", "b", "c"));
    DataStream<String> input2 = env.fromCollection(ImmutableList.of("d", "e", "f"));
    DataStream<String> result =
        new BeamFlinkDataStreamAdapter()
            .applyBeamPTransform(
                ImmutableMap.of("x", input1, "y", input2),
                new PTransform<PCollectionTuple, PCollection<String>>() {
                  @Override
                  public PCollection<String> expand(PCollectionTuple input) {
                    return PCollectionList.of(input.<String>get("x").apply(withPrefix("x")))
                        .and(input.<String>get("y").apply(withPrefix("y")))
                        .apply(Flatten.pCollections());
                  }
                });

    assertThat(
        ImmutableList.copyOf(result.executeAndCollect()),
        containsInAnyOrder("xa", "xb", "xc", "yd", "ye", "yf"));
  }

  @Test
  public void testApplyMultiOutputTransform() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

    DataStream<String> input = env.fromCollection(ImmutableList.of("a", "b", "c"));
    Map<String, DataStream<?>> result =
        new BeamFlinkDataStreamAdapter()
            .applyMultiOutputBeamPTransform(
                input,
                new PTransform<PCollection<String>, PCollectionTuple>() {
                  @Override
                  public PCollectionTuple expand(PCollection<String> input) {
                    return PCollectionTuple.of("x", input.apply(withPrefix("x")))
                        .and("y", input.apply(withPrefix("y")));
                  }
                });

    assertThat(
        ImmutableList.copyOf(result.get("x").executeAndCollect()),
        containsInAnyOrder("xa", "xb", "xc"));
    assertThat(
        ImmutableList.copyOf(result.get("y").executeAndCollect()),
        containsInAnyOrder("ya", "yb", "yc"));
  }

  @Test
  public void testApplyGroupingTransform() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

    DataStream<String> input = env.fromCollection(ImmutableList.of("a", "a", "b"));
    DataStream<KV<String, Long>> result =
        new BeamFlinkDataStreamAdapter()
            .applyBeamPTransform(
                input,
                new PTransform<PCollection<String>, PCollection<KV<String, Long>>>() {
                  @Override
                  public PCollection<KV<String, Long>> expand(PCollection<String> input) {
                    return input
                        .apply(Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(Count.perElement());
                  }
                });

    assertThat(
        ImmutableList.copyOf(result.executeAndCollect()),
        containsInAnyOrder(KV.of("a", 2L), KV.of("b", 1L)));
  }

  @Test
  public void testApplyPreservesInputTimestamps() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    DataStream<Long> input =
        env.fromCollection(ImmutableList.of(1L, 2L, 12L))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Long>forBoundedOutOfOrderness(java.time.Duration.ofMillis(100))
                    .withTimestampAssigner(
                        (SerializableTimestampAssigner<Long>)
                            (element, recordTimestamp) -> element));
    DataStream<Long> result =
        new BeamFlinkDataStreamAdapter()
            .applyBeamPTransform(
                input,
                new PTransform<PCollection<Long>, PCollection<Long>>() {
                  @Override
                  public PCollection<Long> expand(PCollection<Long> input) {
                    return input
                        .apply(Window.into(FixedWindows.of(Duration.millis(10))))
                        .apply(Sum.longsGlobally().withoutDefaults());
                  }
                });

    assertThat(ImmutableList.copyOf(result.executeAndCollect()), containsInAnyOrder(3L, 12L));
  }

  @Test
  public void testApplyPreservesOutputTimestamps() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    DataStream<Long> input = env.fromCollection(ImmutableList.of(1L, 2L, 12L));
    DataStream<Long> withTimestamps =
        new BeamFlinkDataStreamAdapter()
            .applyBeamPTransform(
                input,
                new PTransform<PCollection<Long>, PCollection<Long>>() {
                  @Override
                  public PCollection<Long> expand(PCollection<Long> input) {
                    return input.apply(WithTimestamps.of(x -> Instant.ofEpochMilli(x)));
                  }
                });

    assertThat(
        ImmutableList.copyOf(
            withTimestamps
                .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(10)))
                .reduce((ReduceFunction<Long>) (a, b) -> a + b)
                .executeAndCollect()),
        containsInAnyOrder(3L, 12L));
  }
}
