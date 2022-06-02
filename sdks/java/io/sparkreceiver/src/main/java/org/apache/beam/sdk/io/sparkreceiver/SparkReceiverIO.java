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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.streaming.receiver.Receiver;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Streaming sources for Spark {@link Receiver}. */
public class SparkReceiverIO {

  public static <V> Read<V> read() {
    return new AutoValue_SparkReceiverIO_Read.Builder<V>().build();
  }

  /** A {@link PTransform} to read from Spark {@link Receiver}. */
  @AutoValue
  @AutoValue.CopyAnnotations
  public abstract static class Read<V> extends PTransform<PBegin, PCollection<V>> {

    abstract @Nullable ReceiverBuilder<V, ? extends Receiver<V>> getSparkReceiverBuilder();

    abstract @Nullable Class<V> getValueClass();

    abstract @Nullable Coder<V> getValueCoder();

    abstract @Nullable SerializableFunction<V, Long> getGetOffsetFn();

    abstract @Nullable SparkConsumer<V> getSparkConsumer();

    abstract Builder<V> toBuilder();

    @Experimental(Experimental.Kind.PORTABILITY)
    @AutoValue.Builder
    abstract static class Builder<V> {

      abstract Builder<V> setValueClass(Class<V> valueClass);

      abstract Builder<V> setValueCoder(Coder<V> valueCoder);

      abstract Builder<V> setSparkReceiverBuilder(
          ReceiverBuilder<V, ? extends Receiver<V>> sparkReceiverBuilder);

      abstract Builder<V> setGetOffsetFn(SerializableFunction<V, Long> getOffsetFn);

      abstract Builder<V> setSparkConsumer(SparkConsumer<V> sparkConsumer);

      abstract Read<V> build();
    }

    public Read<V> withValueClass(Class<V> valueClass) {
      return toBuilder().setValueClass(valueClass).build();
    }

    public Read<V> withValueCoder(Coder<V> valueCoder) {
      return toBuilder().setValueCoder(valueCoder).build();
    }

    public Read<V> withSparkReceiverBuilder(
        ReceiverBuilder<V, ? extends Receiver<V>> sparkReceiverBuilder) {
      return toBuilder().setSparkReceiverBuilder(sparkReceiverBuilder).build();
    }

    public Read<V> withGetOffsetFn(SerializableFunction<V, Long> getOffsetFn) {
      return toBuilder().setGetOffsetFn(getOffsetFn).build();
    }

    public Read<V> withSparkConsumer(SparkConsumer<V> sparkConsumer) {
      return toBuilder().setSparkConsumer(sparkConsumer).build();
    }

    @Override
    public PCollection<V> expand(PBegin input) {
      checkArgument(getValueCoder() != null, "withValueCoder() is required");
      checkArgument(getValueClass() != null, "withValueClass() is required");
      checkArgument(getSparkReceiverBuilder() != null, "withSparkReceiverBuilder() is required");
      checkArgument(getGetOffsetFn() != null, "withGetOffsetFn() is required");
      if (!HasOffset.class.isAssignableFrom(getSparkReceiverBuilder().getSparkReceiverClass())) {
        checkArgument(getSparkConsumer() != null, "withSparkConsumer() is required");
      }

      return input.apply(new ReadFromSparkReceiverViaSdf<>(this, getValueCoder()));
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
          .apply(ParDo.of(new GenerateSparkReceiverSourceDescriptor()))
          .apply(ParDo.of(new ReadFromSparkReceiverDoFn<>(this)))
          .setCoder(valueCoder);
    }
  }

  static class GenerateSparkReceiverSourceDescriptor
      extends DoFn<byte[], SparkReceiverSourceDescriptor> {

    GenerateSparkReceiverSourceDescriptor() {}

    @ProcessElement
    public void processElement(OutputReceiver<SparkReceiverSourceDescriptor> receiver) {
      // TODO:
      receiver.output(new SparkReceiverSourceDescriptor(null));
    }
  }
}
