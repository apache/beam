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
import static org.apache.beam.sdk.values.TypeDescriptors.integers;
import static org.apache.beam.sdk.values.TypeDescriptors.kvs;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metric;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.GetInitialRestriction;
import org.apache.beam.sdk.transforms.DoFn.GetInitialWatermarkEstimatorState;
import org.apache.beam.sdk.transforms.DoFn.NewTracker;
import org.apache.beam.sdk.transforms.DoFn.NewWatermarkEstimator;
import org.apache.beam.sdk.transforms.DoFn.Restriction;
import org.apache.beam.sdk.transforms.DoFn.TruncateRestriction;
import org.apache.beam.sdk.transforms.DoFn.WatermarkEstimatorState;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Throttle a {@link PCollection} of {@link RequestT} elements.
 *
 * <pre>
 * {@link Throttle} returns the same {@link RequestT}
 * {@link PCollection}, but with elements emitted at a
 * slower rate. Throttling is a best effort to decrease a {@link PCollection} throughput to a
 * maximum of a configured {@link Rate} parameter.
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

  /**
   * Instantiates a {@link Throttle} with the maximumRate of {@link Rate} and without collecting
   * metrics.
   */
  static <RequestT> Throttle<RequestT> of(Rate maximumRate) {
    return new Throttle<>(Configuration.builder().setMaximumRate(maximumRate).build());
  }

  /** Returns {@link Throttle} with metrics collection turned on. */
  Throttle<RequestT> withMetricsCollected() {
    return new Throttle<>(configuration.toBuilder().setCollectMetrics(true).build());
  }

  private final Configuration configuration;

  private Throttle(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public PCollection<RequestT> expand(PCollection<RequestT> input) {
    ListCoder<RequestT> listCoder = ListCoder.of(input.getCoder());
    Coder<KV<Integer, List<RequestT>>> kvCoder = KvCoder.of(VarIntCoder.of(), listCoder);
    WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();

    return input
        // Break up the PCollection into fixed channels assigned to an int key [0,
        // Rate::numElements).
        .apply(AssignChannelFn.class.getSimpleName(), assignChannels())
        // Apply GroupByKey to convert PCollection of KV<Integer, RequestT> to KV<Integer,
        // Iterable<RequestT>>.
        .apply(GroupByKey.class.getSimpleName(), GroupByKey.create())
        // Convert KV<Integer, Iterable<RequestT>> to KV<Integer, List<RequestT>> for cleaner
        // processing by ThrottleFn; IterableCoder uses a List for IterableLikeCoder's
        // structuralValue.
        .apply("ConvertToList", toList())
        .setCoder(kvCoder)
        .apply(
            GlobalWindows.class.getSimpleName(),
            Window.<KV<Integer, List<RequestT>>>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes())
        .apply(ThrottleFn.class.getSimpleName(), throttle())
        .setWindowingStrategyInternal(windowingStrategy);
  }

  private ParDo.SingleOutput<KV<Integer, List<RequestT>>, RequestT> throttle() {
    return ParDo.of(new ThrottleFn());
  }

  private class ThrottleFn extends DoFn<KV<Integer, List<RequestT>>, RequestT> {
    private @MonotonicNonNull Counter inputElementsCounter = null;
    private @MonotonicNonNull Counter outputElementsCounter = null;

    @Setup
    public void setup() {
      if (configuration.getCollectMetrics()) {
        inputElementsCounter = Metrics.counter(Throttle.class, INPUT_ELEMENTS_COUNTER_NAME);
        outputElementsCounter = Metrics.counter(Throttle.class, OUTPUT_ELEMENTS_COUNTER_NAME);
      }
    }

    /**
     * Instantiates an initial {@link RestrictionTracker.IsBounded#BOUNDED} {@link OffsetRange}
     * restriction from [-1, {@link List#size()}). Defaults to [-1, 0) for null {@link
     * KV#getValue()} elements.
     */
    @GetInitialRestriction
    public OffsetRange getInitialRange(@Element KV<Integer, List<RequestT>> element) {
      int size = 0;
      if (element.getValue() != null) {
        size = element.getValue().size();
      }
      incIfPresent(inputElementsCounter, size);
      return new OffsetRange(-1, size);
    }

    /**
     * The {@link GetInitialWatermarkEstimatorState} initializes to this DoFn's output watermark to
     * a negative infinity timestamp via {@link BoundedWindow#TIMESTAMP_MIN_VALUE}. The {@link
     * Instant} returned by this method provides the runner the value is passes as an argument to
     * this DoFn's {@link #newWatermarkEstimator}.
     */
    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkState() {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    /**
     * This DoFn uses a {@link WatermarkEstimators.Manual} as its {@link NewWatermarkEstimator},
     * instantiated from an {@link Instant}. The state argument in this method comes from the return
     * of the {@link #getInitialWatermarkState}.
     */
    @NewWatermarkEstimator
    public WatermarkEstimator<Instant> newWatermarkEstimator(
        @WatermarkEstimatorState Instant state) {
      return new WatermarkEstimators.Manual(state);
    }

    /** Instantiates an {@link OffsetRangeTracker} from an {@link OffsetRange} instance. */
    @NewTracker
    public RestrictionTracker<OffsetRange, Long> newTracker(@Restriction OffsetRange restriction) {
      return new OffsetRangeTracker(restriction);
    }

    /** Simply returns the {@link OffsetRange} restriction. */
    @TruncateRestriction
    public RestrictionTracker.TruncateResult<OffsetRange> truncate(
        @Restriction OffsetRange restriction) {
      return new RestrictionTracker.TruncateResult<OffsetRange>() {
        @Override
        public OffsetRange getTruncatedRestriction() {
          return restriction;
        }
      };
    }

    @ProcessElement
    public ProcessContinuation process(
        @Element KV<Integer, List<RequestT>> element,
        ManualWatermarkEstimator<Instant> estimator,
        RestrictionTracker<OffsetRange, Long> tracker,
        OutputReceiver<RequestT> receiver) {

      if (element.getValue() == null || element.getValue().isEmpty()) {
        return ProcessContinuation.stop();
      }

      long position = tracker.currentRestriction().getFrom();
      if (position < 0) {
        position = 0;
      }

      if (!tracker.tryClaim(position)) {
        return ProcessContinuation.stop();
      }

      Instant timestamp = estimator.getState();
      estimator.setWatermark(timestamp);
      receiver.outputWithTimestamp(element.getValue().get((int) position), timestamp);
      incIfPresent(outputElementsCounter);

      // If we know that the next position is at the end, then we don't bother resuming.
      if (position + 1 >= tracker.currentRestriction().getTo()) {
        return ProcessContinuation.stop();
      }

      return ProcessContinuation.resume()
          .withResumeDelay(configuration.getMaximumRate().getInterval());
    }
  }

  /**
   * Returns a {@link ParDo.SingleOutput} that assigns each {@link RequestT} a key using {@link
   * AssignChannelFn}.
   */
  private ParDo.SingleOutput<RequestT, KV<Integer, RequestT>> assignChannels() {
    return ParDo.of(new AssignChannelFn());
  }

  /**
   * Assigns each {@link RequestT} an {@link KV} key using {@link RandomDataGenerator#nextInt} from
   * [0, {@link Rate#getNumElements}). The design goals of this {@link DoFn} are to distribute
   * elements among fixed parallel channels that are each throttled such that the sum total maximum
   * rate of emission is up to {@link Configuration#getMaximumRate()}.
   */
  private class AssignChannelFn extends DoFn<RequestT, KV<Integer, RequestT>> {
    private transient @MonotonicNonNull RandomDataGenerator random;

    @Setup
    public void setup() {
      random = new RandomDataGenerator();
    }

    @ProcessElement
    public void process(@Element RequestT request, OutputReceiver<KV<Integer, RequestT>> receiver) {
      Integer key =
          checkStateNotNull(random).nextInt(0, configuration.getMaximumRate().getNumElements() - 1);
      receiver.output(KV.of(key, request));
    }
  }

  /**
   * The result of {@link GroupByKey} is a {@link PCollection} of {@link KV} with an {@link
   * Iterable} of {@link RequestT}s value. This method converts to a {@link List} for cleaner
   * processing via {@link ThrottleFn} within a Splittable DoFn context.
   */
  private MapElements<KV<Integer, Iterable<RequestT>>, KV<Integer, List<RequestT>>> toList() {
    return MapElements.into(kvs(integers(), new TypeDescriptor<List<RequestT>>() {}))
        .via(
            kv -> {
              if (kv.getValue() == null) {
                return KV.of(kv.getKey(), ImmutableList.of());
              }
              try {
                List<RequestT> list =
                    StreamSupport.stream(kv.getValue().spliterator(), true)
                        .collect(Collectors.toList());
                return KV.of(kv.getKey(), list);
              } catch (OutOfMemoryError e) {
                Spliterator<RequestT> spliterator = kv.getValue().spliterator();
                long count = spliterator.estimateSize();
                if (count == -1) {
                  count = StreamSupport.stream(kv.getValue().spliterator(), true).count();
                }
                String message =
                    String.format(
                        "an %s exception thrown when attempting to process a %s result with a count of: %d; "
                            + "consider modifying the %s parameters of %s to process smaller bundle sizes",
                        OutOfMemoryError.class,
                        GroupByKey.class,
                        count,
                        Rate.class,
                        RequestResponseIO.class);
                throw new IllegalStateException(message);
              }
            });
  }

  @AutoValue
  abstract static class Configuration implements Serializable {

    static Builder builder() {
      return new AutoValue_Throttle_Configuration.Builder();
    }

    /** The maximum {@link Rate} of throughput. */
    abstract Rate getMaximumRate();

    /** Configures whether to collect {@link Metric}s. Defaults to false. */
    abstract Boolean getCollectMetrics();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      /** See {@link #getMaximumRate()}. */
      abstract Builder setMaximumRate(Rate value);

      /** See {@link Configuration#getCollectMetrics()}. */
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
