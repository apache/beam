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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
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

  static <RequestT> ThrottleWithoutExternalResource<RequestT> of(
      Configuration<RequestT> configuration) {
    return new ThrottleWithoutExternalResource<>(configuration);
  }

  private final Configuration<RequestT> configuration;

  private ThrottleWithoutExternalResource(Configuration<RequestT> configuration) {
    this.configuration = configuration;
  }

  @Override
  public PCollection<RequestT> expand(PCollection<RequestT> input) {
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
        // Finally apply a splittable DoFn by splitting the Iterable<RequestT>, controlling the
        // output via the watermark estimator.
        .apply(ThrottleFn.class.getSimpleName(), throttle());
  }

  private ParDo.SingleOutput<KV<Integer, List<RequestT>>, RequestT> throttle() {
    return ParDo.of(new ThrottleFn());
  }

  /**
   * This {@link DoFn} is inspired by {@link org.apache.beam.sdk.transforms.PeriodicSequence}'s DoFn
   * implementation with the exception that instead of emitting an {@link Instant}, it emits a
   * {@link RequestT}.
   */
  private class ThrottleFn extends DoFn<KV<Integer, List<RequestT>>, RequestT> {

//    @GetInitialRestriction
//    public OffsetRange getInitialRange(@Element KV<Integer, List<RequestT>> element) {
//      int size = 0;
//      if (element.getValue() != null) {
//        size = element.getValue().size();
//      }
//      return null;
//    }
//
//    @NewTracker
//    public RestrictionTracker<OffsetRange, Integer> newTracker(
//        @Restriction OffsetRange restriction) {
//      return new OffsetRangeTracker(restriction);
//    }
//
//    @TruncateRestriction
//    public RestrictionTracker.TruncateResult<OffsetRange> truncate(
//        @Restriction OffsetRange restriction) {
//      return new RestrictionTracker.TruncateResult<OffsetRange>() {
//        @Override
//        public ThrottleWithoutExternalResource.@Nullable OffsetRange getTruncatedRestriction() {
//          return restriction;
//        }
//      };
//    }

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
        RestrictionTracker<org.apache.beam.sdk.io.range.OffsetRange, Long> restrictionTracker,
        OutputReceiver<RequestT> receiver) {}
  }

  /**
   * Returns a {@link ParDo.SingleOutput} that assigns each {@link RequestT} a key using {@link
   * AssignChannelFn}.
   */
  private ParDo.SingleOutput<RequestT, KV<Integer, RequestT>> assignChannels() {
    return ParDo.of(new AssignChannelFn());
  }

  /**
   * Assigns each {@link RequestT} an {@link KV} key using {@link Random#nextInt(int)} from [0,
   * {@link Rate#getNumElements}). The design goals of this {@link DoFn} are to distribute elements
   * among fixed parallel channels that are each throttled such that the sum total maximum rate of
   * emission is up to {@link Configuration#getMaximumRate()}.
   */
  private class AssignChannelFn extends DoFn<RequestT, KV<Integer, RequestT>> {
    private transient @MonotonicNonNull Random random;

    @Setup
    public void setup() {
      random = new Random(Instant.now().getMillis());
    }

    @ProcessElement
    public void process(@Element RequestT request, OutputReceiver<KV<Integer, RequestT>> receiver) {
      Integer key =
          checkStateNotNull(random).nextInt(configuration.getMaximumRate().getNumElements());
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
  abstract static class OffsetRange
      implements Serializable, HasDefaultTracker<OffsetRange, OffsetRangeTracker> {

    static OffsetRange of(int from, int to) {
      return OffsetRange.builder()
              .setFromInclusive(from)
              .setToExclusive(to)
              .build();
    }

    static Builder builder() {
      return new AutoValue_ThrottleWithoutExternalResource_OffsetRange.Builder();
    }

    public static OffsetRange empty() {
      return OffsetRange.builder()
              .setCurrent(-1)
              .setFromInclusive(-1)
              .setToExclusive(0)
              .build();
    }

    abstract Integer getCurrent();
    abstract Integer getFromInclusive();
    abstract Integer getToExclusive();

    abstract Builder toBuilder();

    int getFractionOf(double fraction) {
      double difference = Integer.valueOf(getSize()).doubleValue();
      return Double.valueOf(difference * fraction).intValue() + getFromInclusive();
    }

    boolean hasMoreOffset() {
      return getFromInclusive() < getToExclusive();
    }

    int getSize() {
      return getToExclusive() - getFromInclusive() - 1;
    }

    int getProgress() {
      return getCurrent() - getFromInclusive();
    }

    int getRemaining() {
      return getSize() - getProgress();
    }

    double getFractionProgress() {
      if (getProgress() == 0) {
        return 0.0;
      }
      return Integer.valueOf(getProgress()).doubleValue() / Integer.valueOf(getSize()).doubleValue();
    }

    double getFractionRemaining() {
      if (getRemaining() == 0) {
        return 0.0;
      }
      return Integer.valueOf(getRemaining()).doubleValue() / Integer.valueOf(getSize()).doubleValue();
    }

    OffsetRange offset(Integer position) {
      return toBuilder().setCurrent(position).build();
    }

    @Override
    public OffsetRangeTracker newTracker() {
      return new OffsetRangeTracker(this);
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCurrent(Integer value);

      abstract Builder setFromInclusive(Integer value);

      abstract Builder setToExclusive(Integer value);
      abstract OffsetRange autoBuild();

      final OffsetRange build() {
        OffsetRange result = autoBuild();
        checkArgument(result.getFromInclusive() < result.getToExclusive(), "Malformed range [%s, %s)", result.getFromInclusive(), result.getToExclusive());
        checkArgument(result.getCurrent() >= result.getFromInclusive() && result.getCurrent() < result.getToExclusive(),
                "Illegal value for current: %s, must be [%s, %s)", result.getCurrent(), result.getFromInclusive(), result.getToExclusive());
        return result;
      }
    }
  }

  static class OffsetRangeTracker extends RestrictionTracker<OffsetRange, Integer>
      implements RestrictionTracker.HasProgress {

    private OffsetRange restriction;

    OffsetRangeTracker(OffsetRange restriction) {
      this.restriction = restriction;
    }

    @Override
    public boolean tryClaim(Integer position) {
      restriction = restriction.offset(position);
      return restriction.hasMoreOffset();
    }

    @Override
    public OffsetRange currentRestriction() {
      return restriction;
    }

    @Override
    public @Nullable SplitResult<OffsetRange> trySplit(double fractionOfRemainder) {
      return null;
    }

    @Override
    public void checkDone() throws IllegalStateException {

    }

    @Override
    public IsBounded isBounded() {
      return IsBounded.BOUNDED;
    }

    @Override
    public Progress getProgress() {
      return Progress.from(restriction.getFractionProgress(), restriction.getFractionRemaining());
    }
  }

  @AutoValue
  abstract static class Configuration<RequestT> {

    /** The maximum */
    abstract Rate getMaximumRate();

    abstract Boolean getCollectMetrics();

    @AutoValue.Builder
    abstract static class Builder<RequestT> {

      abstract Builder<RequestT> setMaximumRate(Rate value);

      abstract Builder<RequestT> setCollectMetrics(Boolean value);

      abstract Optional<Boolean> getCollectMetrics();

      abstract Configuration<RequestT> autoBuild();

      final Configuration<RequestT> build() {
        if (!getCollectMetrics().isPresent()) {
          setCollectMetrics(false);
        }
        return autoBuild();
      }
    }
  }
}
