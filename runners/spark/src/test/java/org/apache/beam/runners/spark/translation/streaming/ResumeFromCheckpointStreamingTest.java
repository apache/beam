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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkPipelineResult;
import org.apache.beam.runners.spark.aggregators.ClearAggregatorsRule;
import org.apache.beam.runners.spark.translation.streaming.utils.EmbeddedKafkaCluster;
import org.apache.beam.runners.spark.translation.streaming.utils.PAssertStreaming;
import org.apache.beam.runners.spark.translation.streaming.utils.SparkTestPipelineOptionsForStreaming;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Test pipelines which are resumed from checkpoint.
 */
public class ResumeFromCheckpointStreamingTest {
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
  private static final long EXPECTED_AGG_SECOND = 8L;

  @Rule
  public TemporaryFolder checkpointParentDir = new TemporaryFolder();

  @Rule
  public SparkTestPipelineOptionsForStreaming commonOptions =
      new SparkTestPipelineOptionsForStreaming();

  @Rule
  public ClearAggregatorsRule clearAggregatorsRule = new ClearAggregatorsRule();

  @BeforeClass
  public static void init() throws IOException {
    EMBEDDED_ZOOKEEPER.startup();
    EMBEDDED_KAFKA_CLUSTER.startup();
    /// this test actually requires to NOT reuse the context but rather to stop it and start again
    // from the checkpoint with a brand new context.
    System.setProperty("beam.spark.test.reuseSparkContext", "false");
  }

  private static void produce() {
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

  /**
   * Tests DStream recovery from checkpoint - recreate the job and continue (from checkpoint).
   * <p>Also tests Aggregator values, which should be restored upon recovery from checkpoint.</p>
   */
  @Test
  public void testRun() throws Exception {
    Duration batchIntervalDuration = Duration.standardSeconds(5);
    SparkPipelineOptions options = commonOptions.withTmpCheckpointDir(checkpointParentDir);
    // provide a generous enough batch-interval to have everything fit in one micro-batch.
    options.setBatchIntervalMillis(batchIntervalDuration.getMillis());
    // provide a very generous read time bound, we rely on num records bound here.
    options.setMinReadTimeMillis(batchIntervalDuration.minus(1).getMillis());
    // bound the read on the number of messages - 1 topic of 4 messages.
    options.setMaxRecordsPerBatch(4L);

    // checkpoint after first (and only) interval.
    options.setCheckpointDurationMillis(options.getBatchIntervalMillis());

    // first run will read from Kafka backlog - "auto.offset.reset=smallest"
    SparkPipelineResult res = run(options);
    long processedMessages1 = res.getAggregatorValue("processedMessages", Long.class);
    assertThat(String.format("Expected %d processed messages count but "
        + "found %d", EXPECTED_AGG_FIRST, processedMessages1), processedMessages1,
            equalTo(EXPECTED_AGG_FIRST));

    // recovery should resume from last read offset, and read the second batch of input.
    res = runAgain(options);
    long processedMessages2 = res.getAggregatorValue("processedMessages", Long.class);
    assertThat(String.format("Expected %d processed messages count but "
        + "found %d", EXPECTED_AGG_SECOND, processedMessages2), processedMessages2,
            equalTo(EXPECTED_AGG_SECOND));
  }

  private SparkPipelineResult runAgain(SparkPipelineOptions options) {
    clearAggregatorsRule.clearNamedAggregators();
    // sleep before next run.
    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    return run(options);
  }

  private static SparkPipelineResult run(SparkPipelineOptions options) {
    // write to Kafka
    produce();
    Map<String, Object> consumerProps = ImmutableMap.<String, Object>of(
        "auto.offset.reset", "earliest"
    );

    KafkaIO.Read<String, String> read = KafkaIO.<String, String>read()
        .withBootstrapServers(EMBEDDED_KAFKA_CLUSTER.getBrokerList())
        .withTopics(Collections.singletonList(TOPIC))
        .withKeyCoder(StringUtf8Coder.of())
        .withValueCoder(StringUtf8Coder.of())
        .updateConsumerProperties(consumerProps);

    Duration windowDuration = new Duration(options.getBatchIntervalMillis());

    Pipeline p = Pipeline.create(options);

    PCollection<String> expectedCol =
        p.apply(Create.of(ImmutableList.copyOf(EXPECTED)).withCoder(StringUtf8Coder.of()));
    final PCollectionView<List<String>> expectedView = expectedCol.apply(View.<String>asList());

    PCollection<String> formattedKV =
        p.apply(read.withoutMetadata())
          .apply(ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
               @ProcessElement
               public void process(ProcessContext c) {
                  // Check side input is passed correctly also after resuming from checkpoint
                  Assert.assertEquals(c.sideInput(expectedView), Arrays.asList(EXPECTED));
                  c.output(c.element());
                }
          }).withSideInputs(expectedView))
        .apply(Window.<KV<String, String>>into(FixedWindows.of(windowDuration)))
        .apply(ParDo.of(new FormatAsText()));

    // graceful shutdown will make sure first batch (at least) will finish.
    Duration timeout = Duration.standardSeconds(1L);
    return PAssertStreaming.runAndAssertContents(p, formattedKV, EXPECTED, timeout);
  }

  @AfterClass
  public static void tearDown() {
    EMBEDDED_KAFKA_CLUSTER.shutdown();
    EMBEDDED_ZOOKEEPER.shutdown();
  }

  private static class FormatAsText extends DoFn<KV<String, String>, String> {

    private final Aggregator<Long, Long> aggregator =
        createAggregator("processedMessages", Sum.ofLongs());

    @ProcessElement
    public void process(ProcessContext c) {
      aggregator.addValue(1L);
      String formatted = c.element().getKey() + "," + c.element().getValue();
      c.output(formatted);
    }
  }

}
