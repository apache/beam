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
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metric;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.splittabledofn.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * {@link ThrottleWithoutExternalResource} is a best attempt effort to throttle a {@link RequestT}
 * {@link PCollection} emitting a {@link RequestT} {@link PCollection} at a maximally configured
 * rate, without using an external resource. Users of this transform are responsible for applying
 * their own windowing strategy to the {@link PCollection} of {@link RequestT} elements and elements
 * in different windows will be throttled independently of each other.
 */
class ThrottleWithoutExternalResource<RequestT>
        extends PTransform<PCollection<RequestT>, Result<RequestT>> {

    static final String DISTRIBUTION_METRIC_NAME = "milliseconds_between_element_emissions";
    static final String INPUT_ELEMENTS_COUNTER_NAME = "input_elements_count";
    static final String OUTPUT_ELEMENTS_COUNTER_NAME = "output_elements_count";

    private final TupleTag<RequestT> outputTag = new TupleTag<RequestT>() {
    };
    private final TupleTag<ApiIOError> errorTag = new TupleTag<ApiIOError>() {
    };

    /**
     * Instantiates a {@link ThrottleWithoutExternalResource} with the maximumRate of {@link Rate} and
     * without collecting metrics.
     */
    static <RequestT> ThrottleWithoutExternalResource<RequestT> of(Rate maximumRate) {
        return new ThrottleWithoutExternalResource<>(
                Configuration.builder().setMaximumRate(maximumRate).build());
    }

    /**
     * Returns {@link ThrottleWithoutExternalResource} with metrics collection turned on.
     */
    ThrottleWithoutExternalResource<RequestT> withMetricsCollected() {
        return new ThrottleWithoutExternalResource<>(
                configuration.toBuilder().setCollectMetrics(true).build());
    }

    private final Configuration configuration;

    private ThrottleWithoutExternalResource(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Result<RequestT> expand(PCollection<RequestT> input) {
        ListCoder<RequestT> listCoder = ListCoder.of(input.getCoder());
        Coder<KV<Integer, List<RequestT>>> kvCoder = KvCoder.of(VarIntCoder.of(), listCoder);

               PCollection<RequestT> output = input
                        // Break up the PCollection into fixed channels assigned to an int key [0,
                        // Rate::numElements).
                        .apply(AssignChannelFn.class.getSimpleName(), assignChannels())
                        .apply(Window.<KV<Integer, RequestT>>into(new GlobalWindows())
                                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                                .withAllowedLateness(configuration.getMaximumRate().getInterval().multipliedBy(10L))
                                .discardingFiredPanes())
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
                        .apply(ThrottleFn.class.getSimpleName(), throttle()).setWindowingStrategyInternal(input.getWindowingStrategy());

               PCollection<ApiIOError> empty = input.getPipeline().apply(Create.empty(TypeDescriptor.of(ApiIOError.class)));

        Result<RequestT> result = Result.of(input.getCoder(), outputTag, errorTag, PCollectionTuple.of(outputTag, output).and(errorTag, empty));

        // If configured to collect metrics, assign a single key to the global window timestamp and
        // apply ComputeMetricsFn.
        if (configuration.getCollectMetrics()) {
            result
                    .getResponses()
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
        return ParDo.of(new ThrottleFn<RequestT>(configuration));
    }

    /**
     * This {@link DoFn} uses a Splittable DoFns to achieve throttling since it provides access to hold the
     * watermark, and along with an output with timestamp, allow the DoFn to emit elements as
     * prescribed intervals.
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
                inputElementsCounter = Metrics.counter(ThrottleFn.class, INPUT_ELEMENTS_COUNTER_NAME);
                outputElementsCounter = Metrics.counter(ThrottleFn.class, OUTPUT_ELEMENTS_COUNTER_NAME);
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
         * Instantiates an {@link OffsetRangeTracker} from an {@link OffsetRange} instance.
         */
        @NewTracker
        public RestrictionTracker<OffsetRange, Long> newTracker(
                @Restriction OffsetRange restriction) {
            return new OffsetRangeTracker(restriction);
        }

        /**
         * Simply returns the {@link OffsetRange} restriction.
         */
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
        public ProcessContinuation process(
                @Element KV<Integer, List<RequestT>> element,
                ManualWatermarkEstimator<Instant> estimator,
                RestrictionTracker<OffsetRange, Long> tracker,
                OutputReceiver<RequestT> receiver) {

            long position = tracker.currentRestriction().getFrom() + 1;

            if (!tracker.tryClaim(position)) {
                return ProcessContinuation.stop();
            }
            Instant timestamp = Instant.now();
            estimator.setWatermark(timestamp);
            receiver.outputWithTimestamp(element.getValue().get((int)position), timestamp);
            incIfPresent(outputElementsCounter);

            return ProcessContinuation.resume().withResumeDelay(configuration.getMaximumRate().getInterval());
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
        return MapElements.into(kvs(integers(), new TypeDescriptor<List<RequestT>>() {
                }))
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

    private ParDo.SingleOutput<KV<Long, RequestT>, RequestT> computeMetrics() {
        return ParDo.of(new ComputeMetricsFn());
    }

    private class ComputeMetricsFn extends DoFn<KV<Long, RequestT>, RequestT> {
        private final Distribution durationsBetweenTimestamps =
                Metrics.distribution(ThrottleFn.class, DISTRIBUTION_METRIC_NAME);
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

        /**
         * The maximum {@link Rate} of throughput.
         */
        abstract Rate getMaximumRate();

        /**
         * Configures whether to collect {@link Metric}s. Defaults to false.
         */
        abstract Boolean getCollectMetrics();

        abstract Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {

            /**
             * See {@link #getMaximumRate()}.
             */
            abstract Builder setMaximumRate(Rate value);

            /**
             * See {@link Configuration#getCollectMetrics()}.
             */
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
