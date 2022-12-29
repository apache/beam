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
package org.apache.beam.examples.io.examplekafkaread;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

/**
 * An example unbounded SDF IO, using Kafka as an underlying source. This IO deliberately has a
 * minimal set of Kafka features, and exists as an example of how to write an unbounded SDF source.
 *
 * <p>This IO should not be used in pipelines, and has no guarantees for quality, correctness, or
 * performance.
 *
 * <p>This IO was generated as a pared down version of KafkaIO, and should act to consume
 * KV<byte[],byte[]> pairs from Kafka In practice, this IO would be used thusly: pipeline
 * .apply(ExampleKafkaReadIO.read() .withBootstrapServers("broker_1:9092,broker_2:9092")
 * .withTopics(ImmutableList.of("my_topic"))) .apply(Some Other Transforms);
 */
public class ExampleKafkaReadIO {

  public static Read read() {
    return new AutoValue_ExampleKafkaReadIO_Read.Builder()
        .setTopics(new ArrayList<>())
        .setConsumerConfig(ExampleKafkaReadIOUtils.DEFAULT_CONSUMER_PROPERTIES)
        .build();
  }

  @AutoValue
  @AutoValue.CopyAnnotations
  public abstract static class Read extends PTransform<PBegin, PCollection<KV<byte[], byte[]>>> {

    @Pure
    abstract Map<String, Object> getConsumerConfig();

    @Pure
    abstract List<String> getTopics();

    abstract Read.Builder toBuilder();

    @Experimental(Kind.PORTABILITY)
    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConsumerConfig(Map<String, Object> config);

      abstract Builder setTopics(List<String> topics);

      abstract Read build();
    }

    /**
     * This expand method is characteristic of many top level expands for IOs. It validates required
     * parameters and configurations, and then it applies a series of DoFns to actually execute the
     * read or write. It is typical to have multiple DoFns here, as frequently an IO needs to
     * determine what to read from the configuration before it actually reads. In this case, we are
     * provided a topic name, but Kafka further divides its topics into partitions. As such, {@link
     * GenerateTopicPartitions} exists to query Kafka for this detail, yielding one or more {@link
     * org.apache.kafka.common.TopicPartition}s for every topic name.
     */
    @Override
    public PCollection<KV<byte[], byte[]>> expand(@NonNull PBegin input) {
      checkArgument(
          getConsumerConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) != null,
          "withBootstrapServers() is required");
      checkArgument(getTopics() != null && getTopics().size() > 0, "withTopics() is required");
      return input
          .apply(Impulse.create())
          .apply(ParDo.of(new GenerateTopicPartitions(getConsumerConfig(), getTopics())))
          .apply(ParDo.of(new ReadFromKafka(getConsumerConfig())));
    }

    /** Sets the bootstrap servers for the Kafka consumer. */
    public ExampleKafkaReadIO.Read withBootstrapServers(String bootstrapServers) {
      return withConsumerConfigUpdates(
          ImmutableMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    /** Sets a list of topics to read from. All the partitions from each of the topics are read. */
    public ExampleKafkaReadIO.Read withTopics(List<String> topics) {
      return toBuilder().setTopics(ImmutableList.copyOf(topics)).build();
    }

    /**
     * Update configuration for the backend main consumer. Note that the default consumer properties
     * will not be completely overridden. This method only updates the value which has the same key.
     *
     * <p>In {@link ExampleKafkaReadIO#read()}, there are two consumers running in the backend
     * actually:<br>
     * 1. the main consumer, which reads data from kafka;<br>
     * 2. the secondary offset consumer, which is used to estimate backlog, by fetching latest
     * offset;<br>
     *
     * <p>By default, main consumer uses the configuration from {@link
     * ExampleKafkaReadIOUtils#DEFAULT_CONSUMER_PROPERTIES}.
     */
    public ExampleKafkaReadIO.Read withConsumerConfigUpdates(Map<String, Object> configUpdates) {
      Map<String, Object> config =
          ExampleKafkaReadIOUtils.updateKafkaProperties(getConsumerConfig(), configUpdates);
      return toBuilder().setConsumerConfig(config).build();
    }
  }
}
