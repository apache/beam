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
package org.apache.beam.sdk.io.sparkreceiver;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import java.util.PriorityQueue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.spark.streaming.receiver.Receiver;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming sources and sinks from <a href="https://github.com/data-integrations">CDAP</a> plugins.
 */
@SuppressWarnings("rawtypes")
public class SparkReceiverIO {

  private static final Logger LOG = LoggerFactory.getLogger(SparkReceiverIO.class);

  public static <V> Read<V> read() {
    return new AutoValue_SparkReceiverIO_Read.Builder<V>().build();
  }

  /** A {@link PTransform} to read from CDAP streaming source. */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"UnnecessaryParentheses", "UnusedVariable", "rawtypes"})
  public abstract static class Read<V> extends PTransform<PBegin, PCollection<V>> {

    abstract @Nullable PluginConfig getPluginConfig();

    abstract @Nullable Class<? extends Receiver> getSparkReceiverClass();

    abstract @Nullable Class<V> getValueClass();

    abstract @Nullable Coder<V> getValueCoder();

    abstract Builder<V> toBuilder();

    @Experimental(Experimental.Kind.PORTABILITY)
    @AutoValue.Builder
    abstract static class Builder<V> {

      abstract Builder<V> setValueClass(Class<V> valueClass);

      abstract Builder<V> setPluginConfig(PluginConfig config);

      abstract Builder<V> setValueCoder(Coder<V> valueCoder);

      abstract Builder<V> setSparkReceiverClass(Class<? extends Receiver> sparkReceiverClass);

      abstract Read<V> build();
    }

    public Read<V> withPluginConfig(PluginConfig pluginConfig) {
      return toBuilder().setPluginConfig(pluginConfig).build();
    }

    public Read<V> withValueClass(Class<V> valueClass) {
      return toBuilder().setValueClass(valueClass).build();
    }

    public Read<V> withValueCoder(Coder<V> valueCoder) {
      return toBuilder().setValueCoder(valueCoder).build();
    }

    public Read<V> withSparkReceiverClass(Class<? extends Receiver> sparkReceiverClass) {
      return toBuilder().setSparkReceiverClass(sparkReceiverClass).build();
    }

    @Override
    public PCollection<V> expand(PBegin input) {
      checkArgument(getValueClass() != null, "withValueClass() is required");

      LOG.info("SparkReceiverIO");
      return input.apply(new ReadFromSparkReceiverViaUnbounded<>(this, getValueCoder()));
    }

    /**
     * Creates an {@link UnboundedSource UnboundedSource&lt;KafkaRecord&lt;K, V&gt;, ?&gt;} with the
     * configuration in {@link Read}. Primary use case is unit tests, should not be used in an
     * application.
     */
    @VisibleForTesting
    UnboundedSource<V, SparkReceiverCheckpointMark> makeSource() {
      // FIXME
      return new SparkReceiverUnboundedSource<>(this, -1, null, null, null, new PriorityQueue<>());
    }
  }

  private static class ReadFromSparkReceiverViaUnbounded<V>
      extends PTransform<PBegin, PCollection<V>> {

    Read<V> sparkReceiverRead;
    Coder<V> valueCoder;

    ReadFromSparkReceiverViaUnbounded(Read<V> sparkReceiverRead, Coder<V> valueCoder) {
      this.sparkReceiverRead = sparkReceiverRead;
      this.valueCoder = valueCoder;
    }

    @Override
    public PCollection<V> expand(PBegin input) {
      // Handles unbounded source to bounded conversion if maxNumRecords or maxReadTime is set.
      org.apache.beam.sdk.io.Read.Unbounded<V> unbounded =
          org.apache.beam.sdk.io.Read.from(
              sparkReceiverRead
                  .toBuilder()
                  .setValueCoder(valueCoder)
                  .setPluginConfig(sparkReceiverRead.getPluginConfig())
                  .setSparkReceiverClass(sparkReceiverRead.getSparkReceiverClass())
                  .build()
                  .makeSource());

      //      PTransform<PBegin, PCollection<KV<K, V>>> transform = unbounded;

      //      if (sparkReceiverRead.getMaxNumRecords() < Long.MAX_VALUE ||
      // sparkReceiverRead.getMaxReadTime() != null) {
      //        transform =
      //                unbounded
      //                        .withMaxReadTime(kafkaRead.getMaxReadTime())
      //                        .withMaxNumRecords(kafkaRead.getMaxNumRecords());
      //      }

      return input.getPipeline().apply(unbounded);
    }
  }
}
