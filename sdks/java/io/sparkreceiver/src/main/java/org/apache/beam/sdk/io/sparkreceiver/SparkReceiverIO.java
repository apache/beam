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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.spark.streaming.receiver.Receiver;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Streaming sources for Spark {@link Receiver}. */
@SuppressWarnings("rawtypes")
public class SparkReceiverIO {

  private static final Logger LOG = LoggerFactory.getLogger(SparkReceiverIO.class);

  public static <V> Read<V> read() {
    return new AutoValue_SparkReceiverIO_Read.Builder<V>().build();
  }

  /** A {@link PTransform} to read from Spark {@link Receiver}. */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"UnnecessaryParentheses", "UnusedVariable"})
  public abstract static class Read<V> extends PTransform<PBegin, PCollection<V>> {

    abstract @Nullable Receiver<V> getSparkReceiver();

    abstract @Nullable ProxyReceiverBuilder<V, ? extends Receiver<V>> getSparkReceiverBuilder();

    abstract @Nullable Class<V> getValueClass();

    abstract @Nullable Coder<V> getValueCoder();

    abstract @Nullable SerializableFunction<V, Long> getGetOffsetFn();

    abstract Builder<V> toBuilder();

    @Experimental(Experimental.Kind.PORTABILITY)
    @AutoValue.Builder
    abstract static class Builder<V> {

      abstract Builder<V> setValueClass(Class<V> valueClass);

      abstract Builder<V> setValueCoder(Coder<V> valueCoder);

      abstract Builder<V> setSparkReceiver(Receiver<V> sparkReceiver);

      abstract Builder<V> setSparkReceiverBuilder(
          ProxyReceiverBuilder<V, ? extends Receiver<V>> sparkReceiverBuilder);

      abstract Builder<V> setGetOffsetFn(SerializableFunction<V, Long> getOffsetFn);

      abstract Read<V> build();
    }

    public Read<V> withValueClass(Class<V> valueClass) {
      return toBuilder().setValueClass(valueClass).build();
    }

    public Read<V> withValueCoder(Coder<V> valueCoder) {
      return toBuilder().setValueCoder(valueCoder).build();
    }

    public Read<V> withSparkReceiver(Receiver<V> sparkReceiver) {
      return toBuilder().setSparkReceiver(sparkReceiver).build();
    }

    public Read<V> withSparkReceiverBuilder(
        ProxyReceiverBuilder<V, ? extends Receiver<V>> sparkReceiverBuilder) {
      return toBuilder().setSparkReceiverBuilder(sparkReceiverBuilder).build();
    }

    public Read<V> withGetOffsetFn(SerializableFunction<V, Long> getOffsetFn) {
      return toBuilder().setGetOffsetFn(getOffsetFn).build();
    }

    @Override
    public PCollection<V> expand(PBegin input) {
      checkArgument(getValueClass() != null, "withValueClass() is required");

      LOG.info("SparkReceiverIO");
      if (getSparkReceiverBuilder() != null) {
        return input.apply(new ReadFromSparkReceiverViaSdf<>(this, getValueCoder()));
      } else {
        return input.apply(new ReadFromSparkReceiverViaUnbounded<>(this, getValueCoder()));
      }
    }

    /**
     * Creates an {@link UnboundedSource UnboundedSource with the
     * configuration in {@link Read}.
     */
    @VisibleForTesting
    UnboundedSource<V, SparkReceiverCheckpointMark> makeSource() {
      return new SparkReceiverUnboundedSource<>(this, -1, null, null, getSparkReceiver());
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
      org.apache.beam.sdk.io.Read.Unbounded<V> unbounded =
          org.apache.beam.sdk.io.Read.from(
              sparkReceiverRead
                  .toBuilder()
                  .setValueCoder(valueCoder)
                  .setSparkReceiver(sparkReceiverRead.getSparkReceiver())
                  .build()
                  .makeSource());

      return input.getPipeline().apply(unbounded);
    }
  }

  public static class ReadFromSparkReceiverViaSdf<V> extends PTransform<PBegin, PCollection<V>> {

    Read<V> sparkReceiverRead;
    Coder<V> valueCoder;

    ReadFromSparkReceiverViaSdf(Read<V> sparkReceiverRead, Coder<V> valueCoder) {
      this.sparkReceiverRead = sparkReceiverRead;
      this.valueCoder = valueCoder;
    }

    @Override
    public PCollection<V> expand(PBegin input) {
      return input
          .apply(Impulse.create())
          .apply(ParDo.of(new GenerateSparkReceiverSourceDescriptor(sparkReceiverRead)))
          .apply(ParDo.of(new ReadFromSparkReceiverDoFn<>(this)))
          .setCoder(valueCoder);
    }
  }

  @SuppressWarnings("UnusedVariable")
  static class GenerateSparkReceiverSourceDescriptor
      extends DoFn<byte[], SparkReceiverSourceDescriptor> {

    private final SparkReceiverIO.Read read;

    GenerateSparkReceiverSourceDescriptor(SparkReceiverIO.Read read) {
      this.read = read;
    }

    @ProcessElement
    public void processElement(OutputReceiver<SparkReceiverSourceDescriptor> receiver) {
      // TODO:
      receiver.output(new SparkReceiverSourceDescriptor(null));
    }
  }
}
