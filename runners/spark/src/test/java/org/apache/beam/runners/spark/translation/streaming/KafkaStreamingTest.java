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

import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.translation.streaming.utils.EmbeddedKafkaCluster;
import org.apache.beam.runners.spark.translation.streaming.utils.PAssertStreaming;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.streaming.Durations;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Test Kafka as input.
 */
public class KafkaStreamingTest {
  private static final EmbeddedKafkaCluster.EmbeddedZookeeper EMBEDDED_ZOOKEEPER =
          new EmbeddedKafkaCluster.EmbeddedZookeeper();
  private static final EmbeddedKafkaCluster EMBEDDED_KAFKA_CLUSTER =
          new EmbeddedKafkaCluster(EMBEDDED_ZOOKEEPER.getConnection(), new Properties());
  private static final String TOPIC = "kafka_dataflow_test_topic";
  private static final Map<String, String> KAFKA_MESSAGES = ImmutableMap.of(
      "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"
  );
  private static final String[] EXPECTED = {"k1,v1", "k2,v2", "k3,v3", "k4,v4"};
  private static final long TEST_TIMEOUT_MSEC = 1000L;

  @BeforeClass
  public static void init() throws IOException {
    EMBEDDED_ZOOKEEPER.startup();
    EMBEDDED_KAFKA_CLUSTER.startup();

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
    }
  }

  @Test
  public void testRun() throws Exception {
    // test read from Kafka
    SparkPipelineOptions options =
        PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    options.setStreaming(true);
    options.setBatchIntervalMillis(Durations.seconds(1).milliseconds());
    options.setTimeout(TEST_TIMEOUT_MSEC); // run for one interval
    Pipeline p = Pipeline.create(options);

    Map<String, Object> props = ImmutableMap.<String, Object>of(
        "auto.offset.reset", "smallest"
    );

    KafkaIO.Read<String, String> kafkaIO = KafkaIO.read()
        .withTopics(Lists.newArrayList(TOPIC))
        .withBootstrapServers(EMBEDDED_KAFKA_CLUSTER.getBrokerList())
        .withKeyCoder(StringUtf8Coder.of())
        .withValueCoder(StringUtf8Coder.of())
        .updateConsumerProperties(props);

    PCollection<KV<String, String>> kafkaInput = p.apply(kafkaIO.withoutMetadata());
    PCollection<KV<String, String>> windowedWords = kafkaInput
        .apply(Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(1))));

    PCollection<String> formattedKV = windowedWords.apply(ParDo.of(new FormatKVFn()));

    PAssertStreaming.assertContents(formattedKV, EXPECTED);

    EvaluationResult res = (EvaluationResult) p.run();
    res.close();
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
