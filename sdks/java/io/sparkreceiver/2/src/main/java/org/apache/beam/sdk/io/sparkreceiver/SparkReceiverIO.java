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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.streaming.receiver.Receiver;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming sources for Spark {@link Receiver}.
 *
 * <h3>Reading using {@link SparkReceiverIO}</h3>
 *
 * <p>You will need to pass a {@link ReceiverBuilder} which is responsible for instantiating new
 * {@link Receiver} objects.
 *
 * <p>{@link Receiver} that will be used should implement {@link HasOffset} interface. You will need
 * to pass {@code getOffsetFn} which is a {@link SerializableFunction} that defines how to get
 * {@code Long offset} from {@code V record}.
 *
 * <p>Optionally you can pass {@code timestampFn} which is a {@link SerializableFunction} that
 * defines how to get {@code Instant timestamp} from {@code V record}, you can pass {@code
 * startOffset} which is inclusive start offset from which the reading should be started.
 *
 * <p>Optionally you can pass {@code pullFrequencySec} which is a delay in seconds between polling
 * for new records updates. Also, you can pass {@code startPollTimeoutSec} which is delay in seconds
 * before start polling.
 *
 * <p>Example of {@link SparkReceiverIO#read()} usage:
 *
 * <pre>{@code
 * Pipeline p = ...; // Create pipeline.
 *
 * // Create ReceiverBuilder for CustomReceiver
 * ReceiverBuilder<String, CustomReceiverWithOffset> receiverBuilder =
 *         new ReceiverBuilder<>(CustomReceiver.class).withConstructorArgs();
 *
 * //Read from CustomReceiver
 * p.apply("Spark Receiver Read",
 *  SparkReceiverIO.Read<String> reader =
 *    SparkReceiverIO.<String>read()
 *      .withGetOffsetFn(Long::valueOf)
 *      .withTimestampFn(Instant::parse)
 *      .withPullFrequencySec(1L)
 *      .withStartPollTimeoutSec(2L)
 *      .withStartOffset(10L)
 *      .withSparkReceiverBuilder(receiverBuilder);
 * }</pre>
 */
public class SparkReceiverIO {

  private static final Logger LOG = LoggerFactory.getLogger(SparkReceiverIO.class);

  public static <V> Read<V> read() {
    return new AutoValue_SparkReceiverIO_Read.Builder<V>().build();
  }

  /** A {@link PTransform} to read from Spark {@link Receiver}. */
  @AutoValue
  @AutoValue.CopyAnnotations
  public abstract static class Read<V> extends PTransform<PBegin, PCollection<V>> {

    abstract @Nullable ReceiverBuilder<V, ? extends Receiver<V>> getSparkReceiverBuilder();

    abstract @Nullable SerializableFunction<V, Long> getGetOffsetFn();

    abstract @Nullable SerializableFunction<V, Instant> getTimestampFn();

    abstract @Nullable Long getPullFrequencySec();

    abstract @Nullable Long getStartPollTimeoutSec();

    abstract @Nullable Long getStartOffset();

    abstract Builder<V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<V> {

      abstract Builder<V> setSparkReceiverBuilder(
          ReceiverBuilder<V, ? extends Receiver<V>> sparkReceiverBuilder);

      abstract Builder<V> setGetOffsetFn(SerializableFunction<V, Long> getOffsetFn);

      abstract Builder<V> setTimestampFn(SerializableFunction<V, Instant> timestampFn);

      abstract Builder<V> setPullFrequencySec(Long pullFrequencySec);

      abstract Builder<V> setStartPollTimeoutSec(Long startPollTimeoutSec);

      abstract Builder<V> setStartOffset(Long startOffset);

      abstract Read<V> build();
    }

    /** Sets {@link ReceiverBuilder} with value and custom Spark {@link Receiver} class. */
    public Read<V> withSparkReceiverBuilder(
        ReceiverBuilder<V, ? extends Receiver<V>> sparkReceiverBuilder) {
      checkArgument(sparkReceiverBuilder != null, "Spark receiver builder can not be null");
      return toBuilder().setSparkReceiverBuilder(sparkReceiverBuilder).build();
    }

    /** A function to get offset in order to start {@link Receiver} from it. */
    public Read<V> withGetOffsetFn(SerializableFunction<V, Long> getOffsetFn) {
      checkArgument(getOffsetFn != null, "Get offset function can not be null");
      return toBuilder().setGetOffsetFn(getOffsetFn).build();
    }

    /** A function to calculate timestamp for a record. */
    public Read<V> withTimestampFn(SerializableFunction<V, Instant> timestampFn) {
      checkArgument(timestampFn != null, "Timestamp function can not be null");
      return toBuilder().setTimestampFn(timestampFn).build();
    }

    /** Delay in seconds between polling for new records updates. */
    public Read<V> withPullFrequencySec(Long pullFrequencySec) {
      checkArgument(pullFrequencySec != null, "Pull frequency can not be null");
      return toBuilder().setPullFrequencySec(pullFrequencySec).build();
    }

    /** Waiting time after the {@link Receiver} starts. Required to prepare for polling. */
    public Read<V> withStartPollTimeoutSec(Long startPollTimeoutSec) {
      checkArgument(startPollTimeoutSec != null, "Start poll timeout can not be null");
      return toBuilder().setStartPollTimeoutSec(startPollTimeoutSec).build();
    }

    /** Inclusive start offset from which the reading should be started. */
    public Read<V> withStartOffset(Long startOffset) {
      checkArgument(startOffset != null, "Start offset can not be null");
      return toBuilder().setStartOffset(startOffset).build();
    }

    @Override
    public PCollection<V> expand(PBegin input) {
      validateTransform();
      return input.apply(new ReadFromSparkReceiverViaSdf<>(this));
    }

    public void validateTransform() {
      ReceiverBuilder<V, ? extends Receiver<V>> sparkReceiverBuilder = getSparkReceiverBuilder();
      checkStateNotNull(sparkReceiverBuilder, "withSparkReceiverBuilder() is required");
      checkStateNotNull(getGetOffsetFn(), "withGetOffsetFn() is required");
    }
  }

  static class ReadFromSparkReceiverViaSdf<V> extends PTransform<PBegin, PCollection<V>> {

    private final Read<V> sparkReceiverRead;

    ReadFromSparkReceiverViaSdf(Read<V> sparkReceiverRead) {
      this.sparkReceiverRead = sparkReceiverRead;
    }

    @Override
    public PCollection<V> expand(PBegin input) {
      final ReceiverBuilder<V, ? extends Receiver<V>> sparkReceiverBuilder =
          sparkReceiverRead.getSparkReceiverBuilder();
      checkStateNotNull(sparkReceiverBuilder, "withSparkReceiverBuilder() is required");
      if (!HasOffset.class.isAssignableFrom(sparkReceiverBuilder.getSparkReceiverClass())) {
        throw new UnsupportedOperationException(
            String.format(
                "Given Spark Receiver class %s doesn't implement HasOffset interface,"
                    + " therefore it is not supported!",
                sparkReceiverBuilder.getSparkReceiverClass().getName()));
      } else {
        LOG.info("{} started reading", ReadFromSparkReceiverWithOffsetDoFn.class.getSimpleName());
        return input
            .apply(Impulse.create())
            .apply(ParDo.of(new ReadFromSparkReceiverWithOffsetDoFn<>(sparkReceiverRead)));
        // TODO: Split data from SparkReceiver into multiple workers
      }
    }
  }
}
