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
package org.apache.beam.io.requestresponse;

import static org.apache.beam.io.requestresponse.Monitoring.incIfPresent;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Optional;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.IncompatibleWindowException;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PartitioningWindowFn;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Throttle a {@link PCollection} of {@link RequestT} elements.
 *
 * <pre>
 * {@link Throttle} simply processes a {@link RequestT}
 * {@link PCollection}, resulting in the same {@link PCollection} of elements but emitted at a
 * slower rate. Throttling is a best effort to decrease a {@link PCollection} throughput to a
 * maximum of a configured {@link Rate} parameter via reassignment of elements to {@link
 * IntervalWindow}s spaced by {@link Rate#getInterval()}. Prior to this window reassignment, the
 * original {@link PCollection} is transformed into a {@code PCollection<KV<Integer, RequestT>>} by
 * random allocation of integer keys using {@link RandomDataGenerator} within a [0, {@link
 * Rate#getNumElements}) range. Therefore, the {@link IntervalWindow}s are assigned along the new
 * key space. The final {@link PCollection} is reassigned its original {@link WindowingStrategy}.
 * </pre>
 *
 * <h2>Basic Usage</h2>
 *
 * {@link Throttle} minimally requires specifying a maximum {@link Rate} parameter.
 *
 * <pre>{@code
 * PCollection<RequestT> original = ...
 *
 * PCollection<RequestT> throttled = original.apply(Throttle.of(Rate.of(10, Duration.standardSeconds(1L))));
 *
 * }</pre>
 *
 * <h2>Applying metrics</h2>
 *
 * Additionally, usage can enable optional metrics to the measure counts of input and outputs.
 *
 * <pre>{@code
 * PCollection<RequestT> original = ...
 *
 * PCollection<RequestT> throttled = original.apply(
 *   Throttle.of(Rate.of(10, Duration.standardSeconds(1L)))
 *           .withMetricsCollected()
 * );
 *
 * }</pre>
 */
public class Throttle<RequestT> extends PTransform<PCollection<RequestT>, PCollection<RequestT>> {

  static final String INPUT_ELEMENTS_COUNTER_NAME = "input_elements_count";
  static final String OUTPUT_ELEMENTS_COUNTER_NAME = "output_elements_count";

  public static <RequestT> Throttle<RequestT> of(Rate maximumRate) {
    return new Throttle<>(builder().setMaximumRate(maximumRate).build());
  }

  public Throttle<RequestT> withMetricsCollected() {
    return new Throttle<>(configuration.toBuilder().setCollectMetrics(true).build());
  }

  private final Configuration configuration;

  private Throttle(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public PCollection<RequestT> expand(PCollection<RequestT> input) {

    PCollection<KV<Integer, RequestT>> throttled =
        input
            .apply(AssignChannelFn.class.getSimpleName(), assignChannels())
            .apply(
                DurationWindows.class.getSimpleName(),
                Window.into(new DurationWindows(configuration.getMaximumRate().getInterval())));

    if (configuration.getCollectMetrics()) {
      return throttled
          .apply(
              EmitAndMeasureOutputFn.class.getSimpleName(), ParDo.of(new EmitAndMeasureOutputFn()))
          .setWindowingStrategyInternal(updateWindowingStrategy(input.getWindowingStrategy()));
    }

    return throttled
        .apply(Values.class.getSimpleName(), Values.create())
        .setWindowingStrategyInternal(input.getWindowingStrategy())
        .setWindowingStrategyInternal(updateWindowingStrategy(input.getWindowingStrategy()));
  }

  private WindowingStrategy<?, ?> updateWindowingStrategy(WindowingStrategy<?, ?> inputStrategy) {
    return inputStrategy
        .withAlreadyMerged(!inputStrategy.getWindowFn().isNonMerging())
        .withTrigger(inputStrategy.getTrigger().getContinuationTrigger());
  }

  /**
   * Returns a {@link ParDo.SingleOutput} that assigns each {@link RequestT} a key using {@link
   * AssignChannelFn}.
   */
  ParDo.SingleOutput<RequestT, KV<Integer, RequestT>> assignChannels() {
    return ParDo.of(new AssignChannelFn());
  }

  /**
   * Assigns each {@link RequestT} an {@link KV} key using {@link RandomDataGenerator#nextInt} from
   * [0, {@link Rate#getNumElements}). The design goals of this {@link DoFn} are to distribute
   * elements among fixed parallel channels that are each throttled such that the sum total maximum
   * rate of emission is up to {@link Rate}.
   */
  private class AssignChannelFn extends DoFn<RequestT, KV<Integer, RequestT>> {
    private transient @MonotonicNonNull RandomDataGenerator random;
    private @MonotonicNonNull Counter inputElementsCounter = null;

    @Setup
    public void setup() {
      random = new RandomDataGenerator();
      if (configuration.getCollectMetrics()) {
        inputElementsCounter = Metrics.counter(Throttle.class, INPUT_ELEMENTS_COUNTER_NAME);
      }
    }

    @ProcessElement
    public void process(@Element RequestT request, OutputReceiver<KV<Integer, RequestT>> receiver) {
      incIfPresent(inputElementsCounter);
      Integer key =
          checkStateNotNull(random).nextInt(0, configuration.getMaximumRate().getNumElements() - 1);
      receiver.output(KV.of(key, request));
    }
  }

  private static class DurationWindows extends PartitioningWindowFn<Object, IntervalWindow> {

    private Instant lastTimestamp = Instant.EPOCH;
    private final Duration interval;

    private DurationWindows(Duration interval) {
      this.interval = interval;
    }

    @Override
    public IntervalWindow assignWindow(Instant timestamp) {
      if (!lastTimestamp.isAfter(Instant.EPOCH)) {
        this.lastTimestamp = Instant.now();
      }
      IntervalWindow result = new IntervalWindow(lastTimestamp, lastTimestamp.plus(interval));
      this.lastTimestamp = result.end();
      return result;
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
      return IntervalWindow.getCoder();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DurationWindows that = (DurationWindows) o;
      return Objects.equal(lastTimestamp, that.lastTimestamp)
          && Objects.equal(interval, that.interval);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(lastTimestamp, interval);
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return equals(other);
    }

    @Override
    public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
      if (!this.equals(other)) {
        throw new IncompatibleWindowException(
            other,
            String.format(
                "Only %s objects with the same number of days, start date "
                    + "and time zone are compatible.",
                DurationWindows.class.getSimpleName()));
      }
    }
  }

  private class EmitAndMeasureOutputFn extends DoFn<KV<Integer, RequestT>, RequestT> {

    private final Counter outputElementsCounter =
        Metrics.counter(Throttle.class, OUTPUT_ELEMENTS_COUNTER_NAME);;

    @ProcessElement
    public void process(@Element KV<Integer, RequestT> element, OutputReceiver<RequestT> receiver) {
      outputElementsCounter.inc();
      receiver.output(element.getValue());
    }
  }

  static Configuration.Builder builder() {
    return new AutoValue_Throttle_Configuration.Builder();
  }

  @AutoValue
  abstract static class Configuration implements Serializable {
    abstract Rate getMaximumRate();

    abstract Boolean getCollectMetrics();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setMaximumRate(Rate maximumRate);

      abstract Builder setCollectMetrics(Boolean value);

      abstract Optional<Boolean> getCollectMetrics();

      abstract Configuration autoBuild();

      final Configuration build() {
        if (!getCollectMetrics().isPresent()) {
          setCollectMetrics(false);
        }
        return autoBuild();
      }
    }
  }
}
