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

package org.apache.beam.runners.spark;

import static org.junit.Assert.assertThat;

import java.util.Collections;
import org.apache.beam.runners.spark.examples.WordCount;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;


/**
 * Test {@link SparkRunnerDebugger} with different pipelines.
 */
public class SparkRunnerDebuggerTest {

  @Rule
  public final PipelineRule batchPipelineRule = PipelineRule.batch();

  @Rule
  public final PipelineRule streamingPipelineRule = PipelineRule.streaming();

  @Test
  public void debugBatchPipeline() {
    TestSparkPipelineOptions options = batchPipelineRule.getOptions();
    options.setRunner(SparkRunnerDebugger.class);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> lines = pipeline
        .apply(Create.of(Collections.<String>emptyList()).withCoder(StringUtf8Coder.of()));

    PCollection<KV<String, Long>> wordCounts = lines
        .apply(new WordCount.CountWords());

    wordCounts
        .apply(GroupByKey.<String, Long>create())
        .apply(Combine.<String, Long, Long>groupedValues(Sum.ofLongs()));

    PCollection<KV<String, Long>> wordCountsPlusOne = wordCounts
        .apply(MapElements.via(new PlusOne()));

    PCollectionList.of(wordCounts).and(wordCountsPlusOne)
        .apply(Flatten.<KV<String, Long>>pCollections());

    wordCounts
        .apply(MapElements.via(new WordCount.FormatAsTextFn()))
        .apply(TextIO.write().to("!!PLACEHOLDER-OUTPUT-DIR!!").withNumShards(3).withSuffix(".txt"));

    final String expectedPipeline = "sparkContext.parallelize(Arrays.asList(...))\n"
        + "_.mapPartitions(new org.apache.beam.runners.spark.examples.WordCount$ExtractWordsFn())\n"
        + "_.mapPartitions(new org.apache.beam.sdk.transforms.Count$PerElement$1())\n"
        + "_.combineByKey(..., new org.apache.beam.sdk.transforms.Count$CountFn(), ...)\n"
        + "_.groupByKey()\n"
        + "_.map(new org.apache.beam.sdk.transforms.Sum$SumLongFn())\n"
        + "_.mapPartitions(new org.apache.beam.runners.spark"
        + ".SparkRunnerDebuggerTest$PlusOne())\n"
        + "sparkContext.union(...)\n"
        + "_.mapPartitions(new org.apache.beam.runners.spark.examples.WordCount$FormatAsTextFn())\n"
        + "_.<org.apache.beam.sdk.io.AutoValue_TextIO_Write>";

    SparkRunnerDebugger.DebugSparkPipelineResult result =
        (SparkRunnerDebugger.DebugSparkPipelineResult) pipeline.run();

    assertThat("Debug pipeline did not equal expected", result.getDebugString(),
        Matchers.equalTo(expectedPipeline));
  }

  @Test
  public void debugStreamingPipeline() {
    TestSparkPipelineOptions options = streamingPipelineRule.getOptions();
    options.setRunner(SparkRunnerDebugger.class);

    Pipeline pipeline = Pipeline.create(options);

    KafkaIO.Read<String, String> read = KafkaIO.<String, String>read()
        .withBootstrapServers("mykafka:9092")
        .withTopics(Collections.singletonList("my_input_topic"))
        .withKeyDeserializer(StringDeserializer.class)
        .withValueDeserializer(StringDeserializer.class);

    KafkaIO.Write<String, String> write = KafkaIO.<String, String>write()
        .withBootstrapServers("myotherkafka:9092")
        .withTopic("my_output_topic")
        .withKeySerializer(StringSerializer.class)
        .withValueSerializer(StringSerializer.class);

    KvCoder<String, String> stringKvCoder = KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    pipeline
        .apply(read.withoutMetadata()).setCoder(stringKvCoder)
        .apply(Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(5))))
        .apply(ParDo.of(new SparkRunnerDebuggerTest.FormatKVFn()))
        .apply(Distinct.<String>create())
        .apply(WithKeys.of(new SparkRunnerDebuggerTest.ArbitraryKeyFunction()))
        .apply(write);

    final String expectedPipeline = "KafkaUtils.createDirectStream(...)\n"
        + "_.map(new org.apache.beam.sdk.transforms.windowing.FixedWindows())\n"
        + "_.mapPartitions(new org.apache.beam.runners.spark."
        + "SparkRunnerDebuggerTest$FormatKVFn())\n"
        + "_.mapPartitions(new org.apache.beam.sdk.transforms.Distinct$2())\n"
        + "_.groupByKey()\n"
        + "_.map(new org.apache.beam.sdk.transforms.Combine$IterableCombineFn())\n"
        + "_.mapPartitions(new org.apache.beam.sdk.transforms.Keys$1())\n"
        + "_.mapPartitions(new org.apache.beam.sdk.transforms.WithKeys$2())\n"
        + "_.<org.apache.beam.sdk.io.kafka.AutoValue_KafkaIO_Write>";

    SparkRunnerDebugger.DebugSparkPipelineResult result =
        (SparkRunnerDebugger.DebugSparkPipelineResult) pipeline.run();

    assertThat("Debug pipeline did not equal expected",
        result.getDebugString(),
        Matchers.equalTo(expectedPipeline));
  }

  private static class FormatKVFn extends DoFn<KV<String, String>, String> {
    @SuppressWarnings("unused")
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().getKey() + "," + c.element().getValue());
    }
  }

  private static class ArbitraryKeyFunction implements SerializableFunction<String, String> {
    @Override
    public String apply(String input) {
      return "someKey";
    }
  }

  private static class PlusOne extends SimpleFunction<KV<String, Long>, KV<String, Long>> {
    @Override
    public KV<String, Long> apply(KV<String, Long> input) {
      return KV.of(input.getKey(), input.getValue() + 1);
    }
  }
}
