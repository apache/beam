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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.translation.streaming.utils.EmbeddedKafkaCluster;
import org.apache.beam.runners.spark.translation.streaming.utils.KafkaWriteOnBatchCompleted;
import org.apache.beam.runners.spark.translation.streaming.utils.PAssertStreaming;
import org.apache.beam.runners.spark.translation.streaming.utils.SparkTestPipelineOptionsForStreaming;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.streaming.api.java.JavaStreamingListener;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test Kafka as input.
 */
public class KafkaStreamingTest {
  private static final EmbeddedKafkaCluster.EmbeddedZookeeper EMBEDDED_ZOOKEEPER =
      new EmbeddedKafkaCluster.EmbeddedZookeeper();
  private static final EmbeddedKafkaCluster EMBEDDED_KAFKA_CLUSTER =
      new EmbeddedKafkaCluster(EMBEDDED_ZOOKEEPER.getConnection());

  @BeforeClass
  public static void init() throws IOException {
    EMBEDDED_ZOOKEEPER.startup();
    EMBEDDED_KAFKA_CLUSTER.startup();
  }

  @Rule
  public TemporaryFolder checkpointParentDir = new TemporaryFolder();

  @Rule
  public SparkTestPipelineOptionsForStreaming commonOptions =
      new SparkTestPipelineOptionsForStreaming();

  @Test
  public void testEarliest2Topics() throws Exception {
    SparkPipelineOptions options = commonOptions.withTmpCheckpointDir(checkpointParentDir);
    // It seems that the consumer's first "position" lookup (in unit test) takes +200 msec,
    // so to be on the safe side we'll set to 750 msec.
    options.setMinReadTimeMillis(750L);
    //--- setup
    // two topics.
    final String topic1 = "topic1";
    final String topic2 = "topic2";
    // messages.
    final Map<String, String> messages = ImmutableMap.of(
        "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"
    );
    // expected.
    final String[] expected = {"k1,v1", "k2,v2", "k3,v3", "k4,v4"};
    // batch and window duration.
    final Duration batchAndWindowDuration = Duration.standardSeconds(1);

    // write to both topics ahead.
    produce(topic1, messages);
    produce(topic2, messages);

    //------- test: read and dedup.
    Pipeline p = Pipeline.create(options);

    Map<String, Object> consumerProps = ImmutableMap.<String, Object>of(
        "auto.offset.reset", "earliest"
    );

    KafkaIO.Read<String, String> read = KafkaIO.read()
        .withBootstrapServers(EMBEDDED_KAFKA_CLUSTER.getBrokerList())
        .withTopics(Arrays.asList(topic1, topic2))
        .withKeyCoder(StringUtf8Coder.of())
        .withValueCoder(StringUtf8Coder.of())
        .updateConsumerProperties(consumerProps);

    PCollection<String> deduped =
        p.apply(read.withoutMetadata()).setCoder(
            KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply(Window.<KV<String, String>>into(FixedWindows.of(batchAndWindowDuration)))
        .apply(ParDo.of(new FormatKVFn()))
        .apply(Distinct.<String>create());

    PAssertStreaming.runAndAssertContents(p, deduped, expected, Duration.standardSeconds(1L));
  }

  @Test
  public void testLatest() throws Exception {
    SparkContextOptions options =
        commonOptions.withTmpCheckpointDir(checkpointParentDir).as(SparkContextOptions.class);
    //--- setup
    final String topic = "topic";
    // messages.
    final Map<String, String> messages = ImmutableMap.of(
        "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"
    );
    // expected.
    final String[] expected = {"k1,v1", "k2,v2", "k3,v3", "k4,v4"};
    // batch and window duration.
    final Duration batchAndWindowDuration = Duration.standardSeconds(1);

    // write once first batch completes, this will guarantee latest-like behaviour.
    options.setListeners(Collections.<JavaStreamingListener>singletonList(
        KafkaWriteOnBatchCompleted.once(messages, Collections.singletonList(topic),
            EMBEDDED_KAFKA_CLUSTER.getProps(), EMBEDDED_KAFKA_CLUSTER.getBrokerList())));
    // It seems that the consumer's first "position" lookup (in unit test) takes +200 msec,
    // so to be on the safe side we'll set to 750 msec.
    options.setMinReadTimeMillis(750L);

    //------- test: read and format.
    Pipeline p = Pipeline.create(options);

    Map<String, Object> consumerProps = ImmutableMap.<String, Object>of(
        "auto.offset.reset", "latest"
    );

    KafkaIO.Read<String, String> read = KafkaIO.read()
        .withBootstrapServers(EMBEDDED_KAFKA_CLUSTER.getBrokerList())
        .withTopics(Collections.singletonList(topic))
        .withKeyCoder(StringUtf8Coder.of())
        .withValueCoder(StringUtf8Coder.of())
        .updateConsumerProperties(consumerProps);

    PCollection<String> formatted =
        p.apply(read.withoutMetadata()).setCoder(
            KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply(Window.<KV<String, String>>into(FixedWindows.of(batchAndWindowDuration)))
        .apply(ParDo.of(new FormatKVFn()));

    // run for more than 1 batch interval, so that reading of latest is attempted in the
    // first batch with no luck, while the OnBatchCompleted injected-input afterwards will be read
    // in the second interval.
    PAssertStreaming.runAndAssertContents(p, formatted, expected, Duration.standardSeconds(3));
  }

  private static void produce(String topic, Map<String, String> messages) {
    Serializer<String> stringSerializer = new StringSerializer();
    try (@SuppressWarnings("unchecked") KafkaProducer<String, String> kafkaProducer =
        new KafkaProducer(defaultProducerProps(), stringSerializer, stringSerializer)) {
          // feed topic.
          for (Map.Entry<String, String> en : messages.entrySet()) {
            kafkaProducer.send(new ProducerRecord<>(topic, en.getKey(), en.getValue()));
          }
          // await send completion.
          kafkaProducer.flush();
        }
  }

  private static Properties defaultProducerProps() {
    Properties producerProps = new Properties();
    producerProps.putAll(EMBEDDED_KAFKA_CLUSTER.getProps());
    producerProps.put("acks", "1");
    producerProps.put("bootstrap.servers", EMBEDDED_KAFKA_CLUSTER.getBrokerList());
    return producerProps;
  }

  @AfterClass
  public static void tearDown() {
    EMBEDDED_KAFKA_CLUSTER.shutdown();
    EMBEDDED_ZOOKEEPER.shutdown();
  }

  private static class FormatKVFn extends DoFn<KV<String, String>, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().getKey() + "," + c.element().getValue());
    }
  }

}
