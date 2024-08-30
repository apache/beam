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
package org.apache.beam.sdk.io.kafka;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link PTransform} that commits offsets of {@link KafkaRecord}. */
public class KafkaCommitOffset<K, V>
    extends PTransform<
        PCollection<KV<KafkaSourceDescriptor, KafkaRecord<K, V>>>, PCollection<Void>> {
  private final KafkaIO.ReadSourceDescriptors<K, V> readSourceDescriptors;
  private final boolean use259implementation;

  KafkaCommitOffset(
      KafkaIO.ReadSourceDescriptors<K, V> readSourceDescriptors, boolean use259implementation) {
    this.readSourceDescriptors = readSourceDescriptors;
    this.use259implementation = use259implementation;
  }

  static class CommitOffsetDoFn extends DoFn<KV<KafkaSourceDescriptor, Long>, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(CommitOffsetDoFn.class);
    private final Map<String, Object> consumerConfig;
    private final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
        consumerFactoryFn;

    CommitOffsetDoFn(KafkaIO.ReadSourceDescriptors<?, ?> readSourceDescriptors) {
      consumerConfig = readSourceDescriptors.getConsumerConfig();
      consumerFactoryFn = readSourceDescriptors.getConsumerFactoryFn();
    }

    @RequiresStableInput
    @ProcessElement
    public void processElement(@Element KV<KafkaSourceDescriptor, Long> element) {
      Map<String, Object> updatedConsumerConfig =
          overrideBootstrapServersConfig(consumerConfig, element.getKey());
      try (Consumer<byte[], byte[]> consumer = consumerFactoryFn.apply(updatedConsumerConfig)) {
        try {
          consumer.commitSync(
              Collections.singletonMap(
                  element.getKey().getTopicPartition(),
                  new OffsetAndMetadata(element.getValue() + 1)));
        } catch (Exception e) {
          // TODO: consider retrying.
          LOG.warn("Getting exception when committing offset: {}", e.getMessage());
        }
      }
    }

    private Map<String, Object> overrideBootstrapServersConfig(
        Map<String, Object> currentConfig, KafkaSourceDescriptor description) {
      checkState(
          currentConfig.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)
              || description.getBootStrapServers() != null);
      Map<String, Object> config = new HashMap<>(currentConfig);
      if (description.getBootStrapServers() != null
          && !description.getBootStrapServers().isEmpty()) {
        config.put(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            String.join(",", description.getBootStrapServers()));
      }
      return config;
    }
  }

  private static final class MaxOffsetFn<K, V>
      extends DoFn<KV<KafkaSourceDescriptor, KafkaRecord<K, V>>, KV<KafkaSourceDescriptor, Long>> {
    private static class OffsetAndTimestamp {
      OffsetAndTimestamp(long offset, Instant timestamp) {
        this.offset = offset;
        this.timestamp = timestamp;
      }

      void merge(long offset, Instant timestamp) {
        if (this.offset < offset) {
          this.offset = offset;
          this.timestamp = timestamp;
        }
      }

      long offset;
      Instant timestamp;
    }

    private transient @MonotonicNonNull Map<KafkaSourceDescriptor, OffsetAndTimestamp> maxObserved;

    @StartBundle
    public void startBundle() {
      if (maxObserved == null) {
        maxObserved = new HashMap<>();
      } else {
        maxObserved.clear();
      }
    }

    @RequiresStableInput
    @ProcessElement
    @SuppressWarnings("nullness") // startBundle guaranteed to initialize
    public void processElement(
        @Element KV<KafkaSourceDescriptor, KafkaRecord<K, V>> element,
        @Timestamp Instant timestamp) {
      maxObserved.compute(
          element.getKey(),
          (k, v) -> {
            long offset = element.getValue().getOffset();
            if (v == null) {
              return new OffsetAndTimestamp(offset, timestamp);
            }
            v.merge(offset, timestamp);
            return v;
          });
    }

    @FinishBundle
    @SuppressWarnings("nullness") // startBundle guaranteed to initialize
    public void finishBundle(FinishBundleContext context) {
      maxObserved.forEach(
          (k, v) -> context.output(KV.of(k, v.offset), v.timestamp, GlobalWindow.INSTANCE));
    }
  }

  @Override
  public PCollection<Void> expand(PCollection<KV<KafkaSourceDescriptor, KafkaRecord<K, V>>> input) {
    try {
      PCollection<KV<KafkaSourceDescriptor, Long>> offsets;
      if (use259implementation) {
        offsets =
            input.apply(
                MapElements.into(new TypeDescriptor<KV<KafkaSourceDescriptor, Long>>() {})
                    .via(element -> KV.of(element.getKey(), element.getValue().getOffset())));
      } else {
        // Reduce the amount of data to combine by calculating a max within the generally dense
        // bundles of reading
        // from a Kafka partition.
        offsets = input.apply(ParDo.of(new MaxOffsetFn<>()));
      }
      return offsets
          .setCoder(
              KvCoder.of(
                  input
                      .getPipeline()
                      .getSchemaRegistry()
                      .getSchemaCoder(KafkaSourceDescriptor.class),
                  VarLongCoder.of()))
          .apply(Window.into(FixedWindows.of(Duration.standardMinutes(5))))
          .apply(Max.longsPerKey())
          .apply(ParDo.of(new CommitOffsetDoFn(readSourceDescriptors)))
          .setCoder(VoidCoder.of());
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e.getMessage());
    }
  }
}
