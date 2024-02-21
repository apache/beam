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
import org.apache.beam.repackaged.core.org.apache.commons.lang3.NotImplementedException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metric;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * {@link Throttle}s a {@link PCollection} of {@link RequestT} elements on a per {@link Window}
 * basis; preserves {@link Window} assignments.
 *
 * <p>{@link Throttle} returns the same {@link RequestT} {@link PCollection}, but with elements
 * emitted maximally at a configured rate.
 *
 * <h2>Basic Usage</h2>
 *
 * {@link Throttle} minimally requires specifying a numElements Integer and interval {@link
 * Duration} parameter.
 *
 * <pre>{@code
 * PCollection<RequestT> original = ...
 *
 * PCollection<RequestT> throttled = original.apply(Throttle.of(10, Duration.standardSeconds(1L)));
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
 *   Throttle.<RequestT>of(10, Duration.standardSeconds(1L))
 *           .withMetricsCollected()
 * );
 *
 * }</pre>
 *
 * <h2>Throttle Algorithm</h2>
 *
 * The following discusses the algorithm for how {@link Throttle} reduces the throughput of a {@code
 * PCollection<RequestT>}.
 *
 * <p>First, the transform processes the original {@code PCollection<RequestT>} into a {@code
 * PCollection<KV<Integer, RequestT>>} via random assignment of the key using {@link
 * RandomDataGenerator#nextInt}. The result is a key space: [0, numElements) such that each keyed
 * channel is throttled at a rate of one element per interval {@link Duration} (See {@link
 * Throttle#of} for context on the numElements and interval parameters). The transform applies
 * resulting {@code PCollection<KV<Integer, RequestT>>} to the appropriate throttle DoFn for
 * Unbounded or Bounded PCollection input.
 *
 * <p>For {@link PCollection.IsBounded#UNBOUNDED} sources, the transform applies a private internal
 * {@link DoFn} that utilizes <a
 * href="https://beam.apache.org/documentation/programming-guide/#state-and-timers">State and
 * Timers</a>. An important implementation detail is the use of a {@link List} of {@link RequestT}
 * {@link ValueState} to hold {@link RequestT} elements until processing time elapses past the
 * {@link Configuration#getInterval()} during an invoked {@link DoFn.OnTimer} method.
 *
 * <p>For {@link PCollection.IsBounded#BOUNDED} sources, this transform applies a private {@link
 * DoFn.BoundedPerElement} <a
 * href="https://beam.apache.org/documentation/programming-guide/#splittable-dofns">Splittable
 * DoFn</a> after {@link GroupByKey} and transforming the {@code PCollection<KV<Integer,
 * Iterable<RequestT>>>} into a {@code PCollection<KV<Integer, List<RequestT>>>}. This DoFn emits an
 * element for every {@link DoFn.ProcessElement} invocation, returning {@link
 * DoFn.ProcessContinuation#withResumeDelay} of {@link Configuration#getInterval()} when
 * to-be-emitted elements remain.
 */
public class Throttle<RequestT> extends PTransform<PCollection<RequestT>, PCollection<RequestT>> {
  static final String INPUT_ELEMENTS_COUNTER_NAME = "input_elements_count";
  static final String OUTPUT_ELEMENTS_COUNTER_NAME = "output_elements_count";

  /**
   * Instantiates a {@link Throttle} with a goal throttling rate of {@param numElements} per {@param
   * interval}.
   */
  static <RequestT> Throttle<RequestT> of(int numElements, Duration interval) {
    return new Throttle<>(
        Configuration.builder().setNumElements(numElements).setInterval(interval).build());
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

    // Step 1. Assign elements into fixed channels assigned to an int key [0,
    // Configuration::numElements).
    PCollection<KV<Integer, RequestT>> kv =
        input.apply(AssignChannelFn.class.getSimpleName(), assignChannels());

    // Step 2. Apply the throttle DoFn to the KV PCollection.
    return throttle(input.getCoder(), kv);
  }

  /**
   * Applies the appropriate throttle DoFn for Unbounded or Bounded PCollection input. See code
   * comment for the Throttle transform for more details.
   */
  private PCollection<RequestT> throttle(
      Coder<RequestT> elementCoder, PCollection<KV<Integer, RequestT>> input) {

    ListCoder<RequestT> listCoder = ListCoder.of(elementCoder);
    Coder<KV<Integer, List<RequestT>>> kvListCoder = KvCoder.of(VarIntCoder.of(), listCoder);

    if (input.isBounded().equals(PCollection.IsBounded.UNBOUNDED)) {
      return input.apply(
          UnboundedThrottleFn.class.getSimpleName(), ParDo.of(new UnboundedThrottleFn()));
    }

    return input
        .apply(GroupByKey.class.getSimpleName(), GroupByKey.create())
        .apply("ConvertToList", toList())
        .setCoder(kvListCoder)
        .apply(BoundedThrottleFn.class.getSimpleName(), ParDo.of(new BoundedThrottleFn()));
  }

  /** The DoFn applied to Bounded PCollection sources. */
  @DoFn.BoundedPerElement
  private class BoundedThrottleFn extends DoFn<KV<Integer, List<RequestT>>, RequestT> {
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

    /**
     * Emits the next {@code element.getValue().get(position)} from the {@link
     * OffsetRange#getFrom()} if {@link RestrictionTracker#tryClaim} is true and sets the watermark.
     * If remaining items exist, it returns {@link
     * DoFn.ProcessContinuation#withResumeDelay(Duration)} with {@link Configuration#getInterval()},
     * otherwise {@link ProcessContinuation#stop()}.
     */
    @ProcessElement
    public ProcessContinuation process(
        @Element KV<Integer, List<RequestT>> element,
        @Timestamp Instant timestamp,
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

      RequestT value = element.getValue().get((int) position);
      estimator.setWatermark(timestamp);
      receiver.output(value);
      incIfPresent(outputElementsCounter);

      // If we know that the next position is at the end, then we don't bother resuming.
      if (position + 1 >= tracker.currentRestriction().getTo()) {
        return ProcessContinuation.stop();
      }

      return ProcessContinuation.resume().withResumeDelay(configuration.getInterval());
    }
  }

  // TODO(damondouglas): implement state and timer DoFn to handle Unbounded PCollection input.
  /** The DoFn applied to Unbounded PCollection sources. */
  private class UnboundedThrottleFn extends DoFn<KV<Integer, RequestT>, RequestT> {
    @ProcessElement
    public void process() {
      throw new NotImplementedException(
          "Support for Unbounded sources not yet implemented. See https://github.com/apache/beam/issues/28930");
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
   * [0, {@link Configuration#getNumElements}). The design goals of this {@link DoFn} are to
   * distribute elements among fixed parallel channels that are each throttled such that the sum
   * total maximum rate of emission is up to {@link Configuration#getNumElements()} per {@link
   * Configuration#getInterval()}.
   */
  private class AssignChannelFn extends DoFn<RequestT, KV<Integer, RequestT>> {
    private transient @MonotonicNonNull RandomDataGenerator random;

    @Setup
    public void setup() {
      random = new RandomDataGenerator();
    }

    @ProcessElement
    public void process(@Element RequestT request, OutputReceiver<KV<Integer, RequestT>> receiver) {
      Integer key = checkStateNotNull(random).nextInt(0, configuration.getNumElements() - 1);
      receiver.output(KV.of(key, request));
    }
  }

  /**
   * The result of {@link GroupByKey} is a {@link PCollection} of {@link KV} with an {@link
   * Iterable} of {@link RequestT}s value. This method converts to a {@link List} for cleaner
   * processing via {@link BoundedThrottleFn} within a Splittable DoFn context.
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
                        "an %s exception thrown when attempting to process a %s result with a count of: %d",
                        OutOfMemoryError.class, GroupByKey.class, count);
                throw new IllegalStateException(message);
              }
            });
  }

  @AutoValue
  abstract static class Configuration implements Serializable {

    static Builder builder() {
      return new AutoValue_Throttle_Configuration.Builder();
    }

    /** The maximum number of elements to emit within a {@link #getInterval()}. */
    abstract Integer getNumElements();

    /** The interval within which to emit the {@link #getNumElements()}. */
    abstract Duration getInterval();

    /** Configures whether to collect {@link Metric}s. Defaults to false. */
    abstract Boolean getCollectMetrics();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      /** See {@link #getNumElements()}. */
      abstract Builder setNumElements(Integer value);

      /** See {@link #getInterval()}. */
      abstract Builder setInterval(Duration value);

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
