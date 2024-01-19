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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.values.TypeDescriptors.integers;
import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.SplittableRandom;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metric;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * {@link ThrottleWithoutExternalResource} throttles a {@link RequestT} {@link PCollection} emitting
 * a {@link RequestT} {@link PCollection} at a maximally configured rate, without using an external
 * resource.
 */
class ThrottleWithoutExternalResource<RequestT>
    extends PTransform<PCollection<RequestT>, PCollection<RequestT>> {

  static final String DISTRIBUTION_METRIC_NAME = "milliseconds_between_element_emissions";
  static final String INPUT_ELEMENTS_COUNTER_NAME = "input_elements_count";
  static final String OUTPUT_ELEMENTS_COUNTER_NAME = "output_elements_count";

  static <RequestT> ThrottleWithoutExternalResource<RequestT> of(Rate maximumRate) {
    return new ThrottleWithoutExternalResource<>(
        Configuration.builder().setMaximumRate(maximumRate).build());
  }

  ThrottleWithoutExternalResource<RequestT> withMetricsCollected() {
    return new ThrottleWithoutExternalResource<>(
        configuration.toBuilder().setCollectMetrics(true).build());
  }

  private final Configuration configuration;

  private ThrottleWithoutExternalResource(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public PCollection<RequestT> expand(PCollection<RequestT> input) {
    ListCoder<RequestT> listCoder = ListCoder.of(input.getCoder());
    Coder<KV<Integer, List<RequestT>>> kvCoder = KvCoder.of(VarIntCoder.of(), listCoder);

    PCollection<RequestT> result =
        input
            // Apply GlobalWindows to prevent multiple window assignment.
            .apply(GlobalWindows.class.getSimpleName(), Window.into(new GlobalWindows()))
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
            // Finally apply a splittable DoFn by splitting the Iterable<RequestT>, controlling the
            // output via the watermark estimator.
            .apply(ThrottleFn.class.getSimpleName(), throttle())
            .setCoder(input.getCoder());

    // If configured to collect metrics, assign a single key to the global window timestamp and
    // apply
    // ComputeMetricsFn.
    if (configuration.getCollectMetrics()) {
      result
          .apply(
              BoundedWindow.class.getSimpleName(),
              WithKeys.of(ignored -> BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()))
          .setCoder(KvCoder.of(VarLongCoder.of(), input.getCoder()))
          .apply(ComputeMetricsFn.class.getSimpleName(), computeMetrics())
          .setCoder(input.getCoder());
    }

    return result;
  }

  private ParDo.SingleOutput<KV<Integer, List<RequestT>>, RequestT> throttle() {
    return ParDo.of(new ThrottleFn<>(configuration));
  }

  /**
   * This {@link DoFn} is inspired by {@link org.apache.beam.sdk.transforms.PeriodicSequence}'s DoFn
   * implementation with the exception that instead of emitting an {@link Instant}, it emits a
   * {@link RequestT}. Additionally, it uses an Integer based {@link OffsetRange} and its associated {@link OffsetRangeTracker}.
   * The reason for using an Integer based offset range is due to Java collection sizes limit to int instead of long.
   * <pre>
   * Splittable DoFns provide access to hold the watermark, and along with an output with timestamp, allow
   * the DoFn to emit elements as prescribed intervals.
   */
  static class ThrottleFn<RequestT> extends DoFn<KV<Integer, List<RequestT>>, RequestT> {

    private final Configuration configuration;
    private @MonotonicNonNull Counter inputElementsCounter = null;
    private @MonotonicNonNull Counter outputElementsCounter = null;

    ThrottleFn(Configuration configuration) {
      this.configuration = configuration;
    }

    @Setup
    public void setup() {
      if (configuration.getCollectMetrics()) {
        inputElementsCounter = Metrics.counter(ThrottleWithoutExternalResource.class, INPUT_ELEMENTS_COUNTER_NAME);
        outputElementsCounter = Metrics.counter(ThrottleWithoutExternalResource.class, OUTPUT_ELEMENTS_COUNTER_NAME);
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
      return OffsetRange.ofSize(size);
    }

    /** Instantiates an {@link OffsetRangeTracker} from an {@link OffsetRange} instance. */
    @NewTracker
    public RestrictionTracker<OffsetRange, Integer> newTracker(
        @Restriction OffsetRange restriction) {
      return new OffsetRangeTracker(restriction);
    }

    /** Simply returns the {@link OffsetRange} restriction. */
    @TruncateRestriction
    public RestrictionTracker.TruncateResult<OffsetRange> truncate(
        @Restriction OffsetRange restriction) {
      return new RestrictionTracker.TruncateResult<OffsetRange>() {
        @Override
        public ThrottleWithoutExternalResource.@Nullable OffsetRange getTruncatedRestriction() {
          return restriction;
        }
      };
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

    @ProcessElement
    public void process(
        @Element KV<Integer, List<RequestT>> element,
        ManualWatermarkEstimator<Instant> estimator,
        RestrictionTracker<OffsetRange, Integer> tracker,
        OutputReceiver<RequestT> receiver) {

      if (element.getValue() == null || element.getValue().isEmpty()) {
        return;
      }

      while (tracker.tryClaim(tracker.currentRestriction().getCurrent() + 1)) {
        Instant nextEmittedTimestamp =
            estimator.currentWatermark().plus(configuration.getMaximumRate().getInterval());
        int index = tracker.currentRestriction().getCurrent();
        RequestT requestT = element.getValue().get(index);
        outputAndSetWatermark(nextEmittedTimestamp, requestT, estimator, receiver);
      }
    }

    /**
     * Emits the element at the nextEmittedTimestamp and sets the watermark to the same when {@link
     * Instant#now()} reaches the nextEmittedTimestamp.
     */
    private void outputAndSetWatermark(
        Instant nextEmittedTimestamp,
        RequestT requestT,
        ManualWatermarkEstimator<Instant> estimator,
        OutputReceiver<RequestT> receiver) {
      Instant now = Instant.now();
      while (now.isBefore(nextEmittedTimestamp)) {
        now = Instant.now();
      }
      estimator.setWatermark(nextEmittedTimestamp);
      receiver.outputWithTimestamp(requestT, nextEmittedTimestamp);
    }
  }

  /**
   * Returns a {@link ParDo.SingleOutput} that assigns each {@link RequestT} a key using {@link
   * AssignChannelFn}.
   */
  ParDo.SingleOutput<RequestT, KV<Integer, RequestT>> assignChannels() {
    return ParDo.of(new AssignChannelFn());
  }

  /**
   * Assigns each {@link RequestT} an {@link KV} key using {@link SplittableRandom#nextInt(int)}
   * from [0, {@link Rate#getNumElements}). The design goals of this {@link DoFn} are to distribute
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

  /**
   * An integer based range [{@link #getFromInclusive()}, {@link #getToExclusive()}) restriction for
   * an {@link OffsetRangeTracker}. During tracking, {@link #getFromInclusive()} and {@link
   * #getToExclusive()} remain fixed, while the {@link OffsetRange} increments {@link #getCurrent()}
   * within [from, to) constraints.
   */
  @AutoValue
  abstract static class OffsetRange
      implements Serializable, HasDefaultTracker<OffsetRange, OffsetRangeTracker> {

    /**
     * Instantiates an {@link OffsetRange} using {@link #of(int, int)} using -1 and size,
     * respectively. Hence {@link #getFromInclusive()} and {@link #getCurrent()} is -1 and {@link
     * #getToExclusive()} is size.
     */
    static OffsetRange ofSize(int size) {
      return of(-1, size);
    }

    /**
     * Instantiates an {@link OffsetRange} assigning the from and to function arguments to {@link
     * #getFromInclusive()} and {@link #getToExclusive()}, respectively. {@link #getCurrent()} is
     * set to {@link #getFromInclusive()}. Throws an {@link IllegalArgumentException} if function
     * arguments do not satisfy [from, to).
     */
    static OffsetRange of(int from, int to) {
      return OffsetRange.builder()
          .setCurrent(from)
          .setFromInclusive(from)
          .setToExclusive(to)
          .build();
    }

    /** Instantiates a {@link OffsetRange} where [from, to) is set to [-1, 0) and current == -1. */
    static OffsetRange empty() {
      return OffsetRange.builder().setCurrent(-1).setFromInclusive(-1).setToExclusive(0).build();
    }

    private static Builder builder() {
      return new AutoValue_ThrottleWithoutExternalResource_OffsetRange.Builder();
    }

    /**
     * The current position of the offset range. Designed to begin a {@link #getFromInclusive()} and
     * increment to {@link #getToExclusive()}.
     */
    abstract Integer getCurrent();

    /** The starting position, inclusive, of the offset range. */
    abstract Integer getFromInclusive();

    /** The ending position, exclusive, of the offset range. */
    abstract Integer getToExclusive();

    abstract Builder toBuilder();

    /** Computes the fraction of {@link #getSize()}. */
    int getFractionOf(double fraction) {
      if (getSize() == 0) {
        return 0;
      }
      double fractionOfSize = getSizeDouble() * fraction;
      return (int) fractionOfSize;
    }

    /**
     * Queries whether we can increment {@link #getCurrent()} i.e. is < {@link #getToExclusive()} -
     * 1.
     */
    boolean hasMoreOffset() {
      return getCurrent() < getToExclusive() - 1;
    }

    /** Queries whether we cannot increment {@link #getCurrent()}. */
    boolean hasNoMoreOffset() {
      return !hasMoreOffset();
    }

    /** The size of the range, inclusive: [from, to-1]. */
    int getSize() {
      return getToExclusive() - getFromInclusive() - 1;
    }

    private double getSizeDouble() {
      return Integer.valueOf(getSize()).doubleValue();
    }

    /** The amount {@link #getCurrent()} has incremented compared to {@link #getFromInclusive()}. */
    int getProgress() {
      return getCurrent() - getFromInclusive();
    }

    /** The amount {@link #getCurrent()} can increment until {@link #getToExclusive()}. */
    int getRemaining() {
      return getSize() - getProgress();
    }

    /**
     * Reports {@link #getProgress()} as a fraction of {@link #getSize()}. Required by {@link
     * OffsetRangeTracker#getProgress()}.
     */
    double getFractionProgress() {
      if (getProgress() == 0) {
        return 0.0;
      }
      return Integer.valueOf(getProgress()).doubleValue()
          / Integer.valueOf(getSize()).doubleValue();
    }

    /**
     * Reports {@link #getRemaining()} as a fraction of {@link #getSize()}. Required by {@link
     * OffsetRangeTracker#getProgress()}.
     */
    double getFractionRemaining() {
      if (getRemaining() == 0) {
        return 0.0;
      }
      return Integer.valueOf(getRemaining()).doubleValue()
          / Integer.valueOf(getSize()).doubleValue();
    }

    /**
     * Offsets {@link #getCurrent()} by position, keeping {@link #getFromInclusive()} and {@link
     * #getToExclusive()} unchanged. Throws an {@link IllegalArgumentException} if {@link
     * #hasMoreOffset()} is false prior to calling this method.
     */
    OffsetRange offset(int position) {
      if (position < getFromInclusive() || position >= getToExclusive()) {
        String message =
            String.format(
                "Illegal value for offset position: %s, must be [%s, %s)",
                position, getFromInclusive(), getToExclusive());
        throw new IllegalArgumentException(message);
      }
      return toBuilder().setCurrent(position).build();
    }

    /**
     * Instantiates a {@link RestrictionTracker} with a {@link OffsetRangeTracker} and {@link
     * OffsetRange} restriction.
     */
    @Override
    public OffsetRangeTracker newTracker() {
      return new OffsetRangeTracker(this);
    }

    @AutoValue.Builder
    abstract static class Builder {

      /** See {@link #getCurrent()}. */
      abstract Builder setCurrent(Integer value);

      /** See {@link #getFromInclusive()}. */
      abstract Builder setFromInclusive(Integer value);

      /** See {@link #getToExclusive()}. */
      abstract Builder setToExclusive(Integer value);

      abstract OffsetRange autoBuild();

      /**
       * Checks whether [{@link #getFromInclusive()}, {@link #getToExclusive()}) and {@link
       * #getCurrent()} is [from, to).
       */
      final OffsetRange build() {
        OffsetRange result = autoBuild();
        checkArgument(
            result.getFromInclusive() < result.getToExclusive(),
            "Malformed range [%s, %s)",
            result.getFromInclusive(),
            result.getToExclusive());
        checkArgument(
            result.getCurrent() >= result.getFromInclusive()
                && result.getCurrent() < result.getToExclusive(),
            "Illegal value for current: %s, must be [%s, %s)",
            result.getCurrent(),
            result.getFromInclusive(),
            result.getToExclusive());
        return result;
      }
    }
  }

  /**
   * An implementation of {@link RestrictionTracker} parameterized with an {@link OffsetRange} and
   * Integer.
   */
  static class OffsetRangeTracker extends RestrictionTracker<OffsetRange, Integer>
      implements Serializable, RestrictionTracker.HasProgress {

    private OffsetRange restriction;

    private OffsetRangeTracker(OffsetRange restriction) {
      this.restriction = restriction;
    }

    /**
     * {@link OffsetRange#offset}s with position. If {@link OffsetRange#hasNoMoreOffset()}, then
     * {@link OffsetRange#getCurrent()} is not incremented. Each invocation of this method increases
     * {@link OffsetRange#getCurrent()} while holding the {@link OffsetRange#getFromInclusive()} and
     * {@link OffsetRange#getToExclusive()} constant.
     */
    @Override
    public boolean tryClaim(Integer position) {
      if (restriction.hasNoMoreOffset()) {
        return false;
      }
      restriction = restriction.offset(position);
      return true;
    }

    /** Returns the current {@link OffsetRange} state as a result of {@link #tryClaim}. */
    @Override
    public OffsetRange currentRestriction() {
      return restriction;
    }

    /** Always return null and hence not splittable. */
    @Override
    public @Nullable SplitResult<OffsetRange> trySplit(double fractionOfRemainder) {
      return null;
    }

    /** A no-op method as checks are performed during {@link #tryClaim}. */
    @Override
    public void checkDone() throws IllegalStateException {}

    /** Always {@link IsBounded#BOUNDED}. */
    @Override
    public IsBounded isBounded() {
      return IsBounded.BOUNDED;
    }

    /**
     * Returns a {@link Progress} instantiated from {@link OffsetRange#getFractionProgress()} and
     * {@link OffsetRange#getFractionRemaining()}.
     */
    @Override
    public Progress getProgress() {
      return Progress.from(restriction.getFractionProgress(), restriction.getFractionRemaining());
    }
  }

  private ParDo.SingleOutput<KV<Long, RequestT>, RequestT> computeMetrics() {
    return ParDo.of(new ComputeMetricsFn());
  }

  private class ComputeMetricsFn extends DoFn<KV<Long, RequestT>, RequestT> {
    private final Distribution durationsBetweenTimestamps =
        Metrics.distribution(ThrottleWithoutExternalResource.class, DISTRIBUTION_METRIC_NAME);
    private static final String LAST_EMITTED_TIMESTAMP_STATE_ID = "last-emitted-timestamp";

    @SuppressWarnings("unused")
    @StateId(LAST_EMITTED_TIMESTAMP_STATE_ID)
    private final StateSpec<ValueState<Long>> lastEmittedTimestampStateSpec = StateSpecs.value();

    @ProcessElement
    public void process(
        @Element KV<Long, RequestT> ignored,
        @AlwaysFetched @StateId(LAST_EMITTED_TIMESTAMP_STATE_ID)
            ValueState<Long> lastEmittedTimestampState) {
      if (lastEmittedTimestampState.read() == null) {
        lastEmittedTimestampState.write(Instant.now().getMillis());
        return;
      }
      long now = Instant.now().getMillis();
      long lastEmittedMilliTimestamp = checkStateNotNull(lastEmittedTimestampState.read());
      durationsBetweenTimestamps.update(now - lastEmittedMilliTimestamp);
      lastEmittedTimestampState.write(now);
    }
  }

  @AutoValue
  abstract static class Configuration implements Serializable {

    static Builder builder() {
      return new AutoValue_ThrottleWithoutExternalResource_Configuration.Builder();
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
