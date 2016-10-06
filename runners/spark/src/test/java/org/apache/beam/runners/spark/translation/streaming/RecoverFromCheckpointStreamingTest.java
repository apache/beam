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
package org.apache.beam.runners.spark.translation.streaming;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import kafka.serializer.StringDecoder;
import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.aggregators.AccumulatorSingleton;
import org.apache.beam.runners.spark.io.KafkaIO;
import org.apache.beam.runners.spark.translation.streaming.utils.EmbeddedKafkaCluster;
import org.apache.beam.runners.spark.translation.streaming.utils.PAssertStreaming;
import org.apache.beam.runners.spark.translation.streaming.utils.TestOptionsForStreaming;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Tests DStream recovery from checkpoint - recreate the job and continue (from checkpoint).
 *
 * <p>Tests Aggregators, which rely on Accumulators - Aggregators should be available, though
 * state is not preserved (Spark issue), so they start from initial value.
 * //TODO: after the runner supports recovering the state of Aggregators, update this test's
 * expected values for the recovered (second) run.
 */
public class RecoverFromCheckpointStreamingTest {
  private static final EmbeddedKafkaCluster.EmbeddedZookeeper EMBEDDED_ZOOKEEPER =
      new EmbeddedKafkaCluster.EmbeddedZookeeper();
  private static final EmbeddedKafkaCluster EMBEDDED_KAFKA_CLUSTER =
      new EmbeddedKafkaCluster(EMBEDDED_ZOOKEEPER.getConnection(), new Properties());
  private static final String TOPIC = "kafka_beam_test_topic";
  private static final Map<String, String> KAFKA_MESSAGES = ImmutableMap.of(
      "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"
  );
  private static final String[] EXPECTED = {"k1,v1", "k2,v2", "k3,v3", "k4,v4"};
  private static final long EXPECTED_AGG_FIRST = 4L;

  @Rule
  public TemporaryFolder checkpointParentDir = new TemporaryFolder();

  @Rule
  public TestOptionsForStreaming commonOptions = new TestOptionsForStreaming();

  @BeforeClass
  public static void init() throws IOException {
    EMBEDDED_ZOOKEEPER.startup();
    EMBEDDED_KAFKA_CLUSTER.startup();
    /// this test actually requires to NOT reuse the context but rather to stop it and start again
    // from the checkpoint with a brand new context.
    System.setProperty("beam.spark.test.reuseSparkContext", "false");
    // write to Kafka
    Properties producerProps = new Properties();
    producerProps.putAll(EMBEDDED_KAFKA_CLUSTER.getProps());
    producerProps.put("request.required.acks", 1);
    producerProps.put("bootstrap.servers", EMBEDDED_KAFKA_CLUSTER.getBrokerList());
    Serializer<String> stringSerializer = new StringSerializer();
    try (@SuppressWarnings("unchecked") KafkaProducer<String, String> kafkaProducer =
        new KafkaProducer(producerProps, stringSerializer, stringSerializer)) {
      for (Map.Entry<String, String> en : KAFKA_MESSAGES.entrySet()) {
        kafkaProducer.send(new ProducerRecord<>(TOPIC, en.getKey(), en.getValue()));
      }
      kafkaProducer.close();
    }
  }

  @Test
  public void testRun() throws Exception {
    SparkPipelineOptions options = commonOptions.withTmpCheckpointDir(
        checkpointParentDir.newFolder(getClass().getSimpleName()));

    // checkpoint after first (and only) interval.
    options.setCheckpointDurationMillis(options.getBatchIntervalMillis());

    // first run will read from Kafka backlog - "auto.offset.reset=smallest"
    EvaluationResult res = run(options);
    res.close();
    long processedMessages1 = res.getAggregatorValue("processedMessages", Long.class);
    assertThat(String.format("Expected %d processed messages count but "
        + "found %d", EXPECTED_AGG_FIRST, processedMessages1), processedMessages1,
            equalTo(EXPECTED_AGG_FIRST));

    // recovery should resume from last read offset, so nothing is read here.
    res = runAgain(options);
    res.close();
    long processedMessages2 = res.getAggregatorValue("processedMessages", Long.class);
    assertThat(String.format("Expected %d processed messages count but "
        + "found %d", 0, processedMessages2), processedMessages2, equalTo(0L));
  }

  private static EvaluationResult runAgain(SparkPipelineOptions options) {
    AccumulatorSingleton.clear();
    // sleep before next run.
    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    return run(options);
  }

  private static EvaluationResult run(SparkPipelineOptions options) {
    Map<String, String> kafkaParams = ImmutableMap.of(
            "metadata.broker.list", EMBEDDED_KAFKA_CLUSTER.getBrokerList(),
            "auto.offset.reset", "smallest"
    );
    Pipeline p = Pipeline.create(options);
    PCollection<KV<String, String>> kafkaInput = p.apply(KafkaIO.Read.from(
        StringDecoder.class, StringDecoder.class, String.class, String.class,
            Collections.singleton(TOPIC), kafkaParams)).setCoder(KvCoder.of(StringUtf8Coder.of(),
                StringUtf8Coder.of()));
    PCollection<KV<String, String>> windowedWords = kafkaInput
        .apply(Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(1))));
    PCollection<String> formattedKV = windowedWords.apply(ParDo.of(
        new FormatAsText()));

    PAssertStreaming.assertContents(formattedKV, EXPECTED);

    return  (EvaluationResult) p.run();
  }

  @AfterClass
  public static void tearDown() {
    EMBEDDED_KAFKA_CLUSTER.shutdown();
    EMBEDDED_ZOOKEEPER.shutdown();
  }

  private static class FormatAsText extends DoFn<KV<String, String>, String> {

    private final Aggregator<Long, Long> aggregator =
        createAggregator("processedMessages", new Sum.SumLongFn());

    @ProcessElement
    public void process(ProcessContext c) {
      aggregator.addValue(1L);
      String formatted = c.element().getKey() + "," + c.element().getValue();
      c.output(formatted);
    }
  }

}
