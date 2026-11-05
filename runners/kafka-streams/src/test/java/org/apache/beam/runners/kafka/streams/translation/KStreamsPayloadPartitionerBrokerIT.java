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
package org.apache.beam.runners.kafka.streams.translation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Checks on a real broker that a flush is written once to each partition it names. The {@code
 * TopologyTestDriver} cannot show this, since it reports every topic as having one partition.
 */
@RunWith(JUnit4.class)
public class KStreamsPayloadPartitionerBrokerIT {

  private static final int PARTITIONS = 4;

  private static KafkaContainer kafka;

  @BeforeClass
  public static void startBroker() {
    kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.0"));
    kafka.start();
  }

  @AfterClass
  public static void stopBroker() {
    if (kafka != null) {
      kafka.stop();
    }
  }

  @Test
  public void aFlushIsWrittenOnceToEachPartitionItNames() throws Exception {
    String id = UUID.randomUUID().toString();
    String input = "flush-in-" + id;
    String output = "flush-out-" + id;
    try (Admin admin =
        Admin.create(
            ImmutableMap.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin
          .createTopics(
              Arrays.asList(
                  new NewTopic(input, 1, (short) 1), new NewTopic(output, PARTITIONS, (short) 1)))
          .all()
          .get();
    }

    KStreamsPayloadSerde<Integer> serde =
        new KStreamsPayloadSerde<>(
            WindowedValues.getFullCoder(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE));
    Topology topology = new Topology();
    topology.addSource("in", new ByteArrayDeserializer(), serde.deserializer(), input);
    topology.addSink(
        "out",
        output,
        new ByteArraySerializer(),
        serde.serializer(),
        new KStreamsPayloadPartitioner<>(PARTITIONS),
        "in");

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "flush-it-" + id);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    config.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("flush-it").toString());
    KafkaStreams streams = new KafkaStreams(topology, config);
    streams.start();
    try {
      try (KafkaProducer<byte[], KStreamsPayload<Integer>> producer =
          new KafkaProducer<>(
              ImmutableMap.<String, Object>of(
                  ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
              new ByteArraySerializer(),
              serde.serializer())) {
        producer
            .send(
                new ProducerRecord<>(
                    input, new byte[0], KStreamsPayload.<Integer>flush(ImmutableSet.of(1, 2))))
            .get();
      }

      Map<Integer, Integer> expected = ImmutableMap.of(1, 1, 2, 1);
      assertThat(copiesPerPartition(output, serde, 2), is(expected));
    } finally {
      streams.close(Duration.ofSeconds(30));
      streams.cleanUp();
    }
  }

  /** Reads the topic until {@code expected} records arrive, then a little longer for extras. */
  private static Map<Integer, Integer> copiesPerPartition(
      String topic, KStreamsPayloadSerde<Integer> serde, int expected) {
    List<TopicPartition> partitions = new ArrayList<>();
    for (int partition = 0; partition < PARTITIONS; partition++) {
      partitions.add(new TopicPartition(topic, partition));
    }
    Map<Integer, Integer> copies = new TreeMap<>();
    try (KafkaConsumer<byte[], KStreamsPayload<Integer>> consumer =
        new KafkaConsumer<>(
            ImmutableMap.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
            new ByteArrayDeserializer(),
            serde.deserializer())) {
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      int seen = 0;
      boolean draining = false;
      long deadline = System.currentTimeMillis() + 60_000;
      while (System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<byte[], KStreamsPayload<Integer>> record :
            consumer.poll(Duration.ofMillis(500))) {
          assertThat(record.value().isFlush(), is(true));
          copies.merge(record.partition(), 1, Integer::sum);
          seen++;
        }
        if (!draining && seen >= expected) {
          draining = true;
          deadline = System.currentTimeMillis() + 3_000;
        }
      }
    }
    return copies;
  }
}
