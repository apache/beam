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
package org.apache.beam.fn.harness.data;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.fn.harness.HandlesSplits;
import org.apache.beam.fn.harness.control.BundleProgressReporter;
import org.apache.beam.fn.harness.control.ExecutionStateSampler;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionState;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionStateTracker;
import org.apache.beam.fn.harness.control.Metrics;
import org.apache.beam.fn.harness.control.Metrics.BundleCounter;
import org.apache.beam.fn.harness.control.Metrics.BundleDistribution;
import org.apache.beam.fn.harness.debug.DataSampler;
import org.apache.beam.fn.harness.debug.ElementSample;
import org.apache.beam.fn.harness.debug.OutputSampler;
import org.apache.beam.fn.harness.state.WorkCancelledException;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Labels;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowedValues.WindowedValueCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code PCollectionConsumerRegistry} is used to maintain a collection of consuming
 * FnDataReceiver for each pCollectionId. Registering with this class allows inserting an element
 * count counter for every pCollection. A combined MultiplexingConsumer (Wrapped with an
 * ElementCountFnDataReceiver) is returned by calling getMultiplexingConsumer.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PCollectionConsumerRegistry {

  /** Stores metadata about each consumer so that the appropriate metrics tracking can occur. */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  abstract static class ConsumerAndMetadata {
    public static ConsumerAndMetadata forConsumer(
        FnDataReceiver consumer,
        String pTransformId,
        ExecutionState state,
        ExecutionStateTracker stateTracker) {
      return new AutoValue_PCollectionConsumerRegistry_ConsumerAndMetadata(
          consumer, pTransformId, state, stateTracker);
    }

    public abstract FnDataReceiver getConsumer();

    public abstract String getPTransformId();

    public abstract ExecutionState getExecutionState();

    public abstract ExecutionStateTracker getExecutionStateTracker();
  }

  @AutoValue
  abstract static class ExecutionStateKey {
    public static ExecutionStateKey of(String pTransformId, String pTransformUniqueName) {
      return new AutoValue_PCollectionConsumerRegistry_ExecutionStateKey(
          pTransformId, pTransformUniqueName);
    }

    public abstract String getPTransformId();

    public abstract String getPTransformUniqueId();
  }

  private final ExecutionStateTracker stateTracker;
  private final ShortIdMap shortIdMap;
  private final Map<String, List<ConsumerAndMetadata>> pCollectionIdsToConsumers = new HashMap<>();
  private final Map<String, FnDataReceiver> pCollectionIdsToWrappedConsumer = new HashMap<>();
  private final Map<ExecutionStateKey, ExecutionState> executionStates = new HashMap<>();
  private final BundleProgressReporter.Registrar bundleProgressReporterRegistrar;
  private final ProcessBundleDescriptor processBundleDescriptor;
  private final RehydratedComponents rehydratedComponents;
  private final @Nullable DataSampler dataSampler;
  private static final Logger LOG = LoggerFactory.getLogger(PCollectionConsumerRegistry.class);

  public PCollectionConsumerRegistry(
      ExecutionStateTracker stateTracker,
      ShortIdMap shortIdMap,
      BundleProgressReporter.Registrar bundleProgressReporterRegistrar,
      ProcessBundleDescriptor processBundleDescriptor) {
    this(stateTracker, shortIdMap, bundleProgressReporterRegistrar, processBundleDescriptor, null);
  }

  public PCollectionConsumerRegistry(
      ExecutionStateTracker stateTracker,
      ShortIdMap shortIdMap,
      BundleProgressReporter.Registrar bundleProgressReporterRegistrar,
      ProcessBundleDescriptor processBundleDescriptor,
      @Nullable DataSampler dataSampler) {
    this.stateTracker = stateTracker;
    this.shortIdMap = shortIdMap;
    this.bundleProgressReporterRegistrar = bundleProgressReporterRegistrar;
    this.processBundleDescriptor = processBundleDescriptor;
    this.rehydratedComponents =
        RehydratedComponents.forComponents(
            RunnerApi.Components.newBuilder()
                .putAllCoders(processBundleDescriptor.getCodersMap())
                .putAllPcollections(processBundleDescriptor.getPcollectionsMap())
                .putAllWindowingStrategies(processBundleDescriptor.getWindowingStrategiesMap())
                .build());
    this.dataSampler = dataSampler;
  }

  /**
   * Register the specified consumer to handle the elements in the pCollection associated with
   * pCollectionId. All consumers must be registered before extracting the combined consumer by
   * calling getMultiplexingConsumer(), or an exception will be thrown.
   *
   * <p>This will cause both Element Count and Process Bundle Execution time metrics to be
   * collected.
   *
   * @param pCollectionId
   * @param pTransformId
   * @param pTransformUniqueName
   * @param consumer
   * @param <T> the element type of the PCollection
   * @throws RuntimeException if {@code register()} is called after {@code
   *     getMultiplexingConsumer()} is called.
   */
  public <T> void register(
      String pCollectionId,
      String pTransformId,
      String pTransformUniqueName,
      FnDataReceiver<WindowedValue<T>> consumer) {
    // Just save these consumers for now, but package them up later with an
    // ElementCountFnDataReceiver and possibly a MultiplexingFnDataReceiver
    // if there are multiple consumers.
    if (pCollectionIdsToWrappedConsumer.containsKey(pCollectionId)) {
      throw new RuntimeException(
          "New consumers for a pCollectionId cannot be register()-d after "
              + "calling getMultiplexingConsumer.");
    }

    List<ConsumerAndMetadata> consumerAndMetadatas =
        pCollectionIdsToConsumers.computeIfAbsent(pCollectionId, (unused) -> new ArrayList<>());
    consumerAndMetadatas.add(
        ConsumerAndMetadata.forConsumer(
            consumer,
            pTransformId,
            getProcessingExecutionState(pTransformId, pTransformUniqueName),
            stateTracker));
  }

  /**
   * New consumers should not be register()-ed after calling this method. This will cause a
   * RuntimeException, as this would fail to properly wrap the late-added consumer to track metrics.
   *
   * @return A {@link FnDataReceiver} which directly wraps all the registered consumers.
   */
  public FnDataReceiver<WindowedValue<?>> getMultiplexingConsumer(String pCollectionId) {
    return pCollectionIdsToWrappedConsumer.computeIfAbsent(
        pCollectionId,
        pcId -> {
          if (!processBundleDescriptor.containsPcollections(pCollectionId)) {
            throw new IllegalArgumentException(
                String.format("Unknown PCollection id %s", pCollectionId));
          }
          String coderId =
              processBundleDescriptor.getPcollectionsOrThrow(pCollectionId).getCoderId();
          Coder<?> coder;
          OutputSampler<?> sampler = null;
          try {
            Coder<?> maybeWindowedValueInputCoder = rehydratedComponents.getCoder(coderId);

            if (dataSampler != null) {
              sampler = dataSampler.sampleOutput(pCollectionId, maybeWindowedValueInputCoder);
            }

            // TODO: Stop passing windowed value coders within PCollections.
            if (maybeWindowedValueInputCoder instanceof WindowedValues.WindowedValueCoder) {
              coder = ((WindowedValueCoder) maybeWindowedValueInputCoder).getValueCoder();
            } else {
              coder = maybeWindowedValueInputCoder;
            }
          } catch (IOException e) {
            throw new IllegalStateException(
                String.format("Unable to materialize coder %s", coderId), e);
          }
          List<ConsumerAndMetadata> consumerAndMetadatas =
              pCollectionIdsToConsumers.computeIfAbsent(
                  pCollectionId, (unused) -> new ArrayList<>());

          if (consumerAndMetadatas.size() == 1) {
            ConsumerAndMetadata consumerAndMetadata = consumerAndMetadatas.get(0);
            if (consumerAndMetadata.getConsumer() instanceof HandlesSplits) {
              return new SplittingMetricTrackingFnDataReceiver(
                  pcId, coder, consumerAndMetadata, sampler);
            }
            return new MetricTrackingFnDataReceiver(pcId, coder, consumerAndMetadata, sampler);
          } else {
            /* TODO(SDF), Consider supporting splitting each consumer individually. This would never
            come up in the existing SDF expansion, but might be useful to support fused SDF nodes.
            This would require dedicated delivery of the split results to each of the consumers
            separately. */
            return new MultiplexingMetricTrackingFnDataReceiver(
                pcId, coder, consumerAndMetadatas, sampler);
          }
        });
  }

  /**
   * Returns a shared ExecutionState for tracking the process of the given transform.
   *
   * @return A {@link ExecutionState} which should be only activated/deactivated on the processing
   *     thread for the bundle.
   */
  public ExecutionState getProcessingExecutionState(
      String pTransformId, String pTransformUniqueName) {
    return executionStates.computeIfAbsent(
        ExecutionStateKey.of(pTransformId, pTransformUniqueName),
        (key) -> {
          SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
          builder.setUrn(MonitoringInfoConstants.Urns.PROCESS_BUNDLE_MSECS);
          builder.setType(MonitoringInfoConstants.TypeUrns.SUM_INT64_TYPE);
          builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, key.getPTransformId());
          MonitoringInfo mi = builder.build();
          if (mi == null) {
            throw new IllegalStateException(
                String.format(
                    "Unable to construct %s counter for PTransform {id=%s, name=%s}",
                    MonitoringInfoConstants.Urns.PROCESS_BUNDLE_MSECS,
                    key.getPTransformId(),
                    key.getPTransformUniqueId()));
          }
          String shortId = shortIdMap.getOrCreateShortId(mi);
          return stateTracker.create(
              shortId,
              key.getPTransformId(),
              key.getPTransformUniqueId(),
              org.apache.beam.runners.core.metrics.ExecutionStateTracker.PROCESS_STATE_NAME);
        });
  }

  private static <T> void logAndRethrow(
      Exception e,
      ExecutionState executionState,
      ExecutionStateTracker executionStateTracker,
      String ptransformId,
      @Nullable OutputSampler<T> outputSampler,
      @Nullable ElementSample<T> elementSample)
      throws Exception {
    if (!WorkCancelledException.isWorkCancelledException(e)) {
      ExecutionStateSampler.ExecutionStateTrackerStatus status = executionStateTracker.getStatus();
      String processBundleId = status == null ? null : status.getProcessBundleId();
      if (outputSampler != null) {
        outputSampler.exception(elementSample, e, ptransformId, processBundleId);
      }

      if (executionState.error()) {
        LOG.error("Failed to process element for bundle \"{}\"", processBundleId, e);
      }
    }
    throw e;
  }

  /**
   * A wrapping {@code FnDataReceiver<WindowedValue<T>>} which counts the number of elements
   * consumed by the original {@code FnDataReceiver<WindowedValue<T>> consumer} and sets up metrics
   * for tracking PTransform processing time.
   *
   * @param <T> - The receiving type of the PTransform.
   */
  private class MetricTrackingFnDataReceiver<T> implements FnDataReceiver<WindowedValue<T>> {
    private final FnDataReceiver<WindowedValue<T>> delegate;
    private final ExecutionState executionState;
    private final BundleCounter elementCountCounter;
    private final SampleByteSizeDistribution<T> sampledByteSizeDistribution;
    private final Coder<T> coder;
    private final @Nullable OutputSampler<T> outputSampler;
    private final String ptransformId;
    private final ExecutionStateTracker executionStateTracker;

    public MetricTrackingFnDataReceiver(
        String pCollectionId,
        Coder<T> coder,
        ConsumerAndMetadata consumerAndMetadata,
        @Nullable OutputSampler<T> outputSampler) {
      this.delegate = consumerAndMetadata.getConsumer();
      this.executionState = consumerAndMetadata.getExecutionState();
      this.executionStateTracker = consumerAndMetadata.getExecutionStateTracker();
      this.ptransformId = consumerAndMetadata.getPTransformId();

      HashMap<String, String> labels = new HashMap<>();
      labels.put(Labels.PCOLLECTION, pCollectionId);
      MonitoringInfoMetricName elementCountMetricName =
          MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.ELEMENT_COUNT, labels);
      String elementCountShortId =
          shortIdMap.getOrCreateShortId(
              new SimpleMonitoringInfoBuilder()
                  .setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT)
                  .setType(TypeUrns.SUM_INT64_TYPE)
                  .setLabels(labels)
                  .build());
      this.elementCountCounter =
          Metrics.bundleProcessingThreadCounter(elementCountShortId, elementCountMetricName);
      bundleProgressReporterRegistrar.register(elementCountCounter);

      MonitoringInfoMetricName sampledByteSizeMetricName =
          MonitoringInfoMetricName.named(Urns.SAMPLED_BYTE_SIZE, labels);
      String sampledByteSizeShortId =
          shortIdMap.getOrCreateShortId(
              new SimpleMonitoringInfoBuilder()
                  .setUrn(Urns.SAMPLED_BYTE_SIZE)
                  .setType(TypeUrns.DISTRIBUTION_INT64_TYPE)
                  .setLabels(labels)
                  .build());
      BundleDistribution sampledByteSizeUnderlyingDistribution =
          Metrics.bundleProcessingThreadDistribution(
              sampledByteSizeShortId, sampledByteSizeMetricName);
      this.sampledByteSizeDistribution =
          new SampleByteSizeDistribution<>(sampledByteSizeUnderlyingDistribution);
      bundleProgressReporterRegistrar.register(sampledByteSizeUnderlyingDistribution);

      this.coder = coder;
      this.outputSampler = outputSampler;
    }

    @Override
    public void accept(WindowedValue<T> input) throws Exception {
      // Increment the counter for each window the element occurs in.
      this.elementCountCounter.inc(input.getWindows().size());
      // TODO(https://github.com/apache/beam/issues/20730): Consider updating size per window when
      // we have window optimization.
      this.sampledByteSizeDistribution.tryUpdate(input.getValue(), this.coder);

      ElementSample<T> elementSample = null;
      if (outputSampler != null) {
        elementSample = outputSampler.sample(input);
      }

      // Use the ExecutionStateTracker and enter an appropriate state to track the
      // Process Bundle Execution time metric and also ensure user counters can get an appropriate
      // metrics container.
      try (ExecutionState.ActiveState a = executionState.scopedActivate()) {
        this.delegate.accept(input);
      } catch (Exception e) {
        this.sampledByteSizeDistribution.discardPendingLazyUpdate();
        logAndRethrow(
            e, executionState, executionStateTracker, ptransformId, outputSampler, elementSample);
      }
      this.sampledByteSizeDistribution.finishLazyUpdate();
    }
  }

  /**
   * A wrapping {@code FnDataReceiver<WindowedValue<T>>} which counts the number of elements
   * consumed by the original {@code FnDataReceiver<WindowedValue<T>> consumers} and sets up metrics
   * for tracking PTransform processing time.
   *
   * @param <T> - The receiving type of the PTransform.
   */
  private class MultiplexingMetricTrackingFnDataReceiver<T>
      implements FnDataReceiver<WindowedValue<T>> {
    private final List<ConsumerAndMetadata> consumerAndMetadatas;
    private final BundleCounter elementCountCounter;
    private final SampleByteSizeDistribution<T> sampledByteSizeDistribution;
    private final Coder<T> coder;
    private @Nullable OutputSampler<T> outputSampler = null;

    public MultiplexingMetricTrackingFnDataReceiver(
        String pCollectionId,
        Coder<T> coder,
        List<ConsumerAndMetadata> consumerAndMetadatas,
        @Nullable OutputSampler<T> outputSampler) {
      this.consumerAndMetadatas = consumerAndMetadatas;

      HashMap<String, String> labels = new HashMap<>();
      labels.put(Labels.PCOLLECTION, pCollectionId);
      MonitoringInfoMetricName elementCountMetricName =
          MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.ELEMENT_COUNT, labels);
      String elementCountShortId =
          shortIdMap.getOrCreateShortId(
              new SimpleMonitoringInfoBuilder()
                  .setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT)
                  .setType(TypeUrns.SUM_INT64_TYPE)
                  .setLabels(labels)
                  .build());
      this.elementCountCounter =
          Metrics.bundleProcessingThreadCounter(elementCountShortId, elementCountMetricName);
      bundleProgressReporterRegistrar.register(elementCountCounter);

      MonitoringInfoMetricName sampledByteSizeMetricName =
          MonitoringInfoMetricName.named(Urns.SAMPLED_BYTE_SIZE, labels);
      String sampledByteSizeShortId =
          shortIdMap.getOrCreateShortId(
              new SimpleMonitoringInfoBuilder()
                  .setUrn(Urns.SAMPLED_BYTE_SIZE)
                  .setType(TypeUrns.DISTRIBUTION_INT64_TYPE)
                  .setLabels(labels)
                  .build());
      BundleDistribution sampledByteSizeUnderlyingDistribution =
          Metrics.bundleProcessingThreadDistribution(
              sampledByteSizeShortId, sampledByteSizeMetricName);
      this.sampledByteSizeDistribution =
          new SampleByteSizeDistribution<>(sampledByteSizeUnderlyingDistribution);
      bundleProgressReporterRegistrar.register(sampledByteSizeUnderlyingDistribution);

      this.coder = coder;
      this.outputSampler = outputSampler;
    }

    @Override
    public void accept(WindowedValue<T> input) throws Exception {
      // Increment the counter for each window the element occurs in.
      this.elementCountCounter.inc(input.getWindows().size());
      // TODO(https://github.com/apache/beam/issues/20730): Consider updating size per window
      // when we have window optimization.
      this.sampledByteSizeDistribution.tryUpdate(input.getValue(), coder);

      ElementSample<T> elementSample = null;
      if (outputSampler != null) {
        elementSample = outputSampler.sample(input);
      }

      // Use the ExecutionStateTracker and enter an appropriate state to track the
      // Process Bundle Execution time metric and also ensure user counters can get an appropriate
      // metrics container. We specifically don't use a for-each loop since it creates an iterator
      // on a hot path.
      for (int size = consumerAndMetadatas.size(), i = 0; i < size; ++i) {
        ConsumerAndMetadata consumerAndMetadata = consumerAndMetadatas.get(i);
        ExecutionState state = consumerAndMetadata.getExecutionState();
        try (ExecutionState.ActiveState a = state.scopedActivate()) {
          consumerAndMetadata.getConsumer().accept(input);
        } catch (Exception e) {
          this.sampledByteSizeDistribution.discardPendingLazyUpdate();
          logAndRethrow(
              e,
              state,
              consumerAndMetadata.getExecutionStateTracker(),
              consumerAndMetadata.getPTransformId(),
              outputSampler,
              elementSample);
        }
      }
      this.sampledByteSizeDistribution.finishLazyUpdate();
    }
  }

  /**
   * A wrapping {@code FnDataReceiver<WindowedValue<T>>} which counts the number of elements
   * consumed by the original {@code FnDataReceiver<WindowedValue<T>> consumer} and forwards split
   * and progress requests to the original consumer.
   *
   * @param <T> - The receiving type of the PTransform.
   */
  private class SplittingMetricTrackingFnDataReceiver<T> extends MetricTrackingFnDataReceiver<T>
      implements HandlesSplits {
    private final HandlesSplits delegate;

    public SplittingMetricTrackingFnDataReceiver(
        String pCollection,
        Coder<T> coder,
        ConsumerAndMetadata consumerAndMetadata,
        @Nullable OutputSampler<T> outputSampler) {
      super(pCollection, coder, consumerAndMetadata, outputSampler);
      this.delegate = (HandlesSplits) consumerAndMetadata.getConsumer();
    }

    @Override
    public SplitResult trySplit(double fractionOfRemainder) {
      return delegate.trySplit(fractionOfRemainder);
    }

    @Override
    public double getProgress() {
      return delegate.getProgress();
    }
  }

  @VisibleForTesting
  static class SampleByteSizeDistribution<T> {
    /** Basic implementation of {@link ElementByteSizeObserver} for use in size estimation. */
    private static class ByteSizeObserver extends ElementByteSizeObserver {
      private long observedSize = 0;

      @Override
      protected void reportElementSize(long elementSize) {
        observedSize += elementSize;
      }

      @Override
      public void reset() {
        super.reset();
        observedSize = 0;
      }
    }

    private final Distribution distribution;
    private final @Nullable Random randomOverride;
    private ByteSizeObserver reusableObserver = new ByteSizeObserver();
    private @Nullable ByteSizeObserver pendingLazyObserver = null;

    public SampleByteSizeDistribution(Distribution distribution) {
      this(distribution, null);
    }

    @VisibleForTesting
    SampleByteSizeDistribution(Distribution distribution, @Nullable Random randomGenerator) {
      this.distribution = distribution;
      this.randomOverride = randomGenerator;
    }

    private Random random() {
      return randomOverride != null ? randomOverride : ThreadLocalRandom.current();
    }

    public void tryUpdate(T value, Coder<T> coder) throws Exception {
      // Clear any pending lazy observer from a prior element that exited abnormally.
      if (pendingLazyObserver != null) {
        pendingLazyObserver = null;
      }
      if (shouldSampleElement()) {
        recordSample(value, coder);
      }
    }

    private void recordSample(T value, Coder<T> coder) throws Exception {
      ByteSizeObserver observer = reusableObserver;
      observer.reset();
      try {
        coder.registerByteSizeObserver(value, observer);
      } catch (Exception | Error e) {
        if (observer.getIsLazy()) {
          // A partially registered lazy observer may still retain references to an iterable/stream;
          // detach it so late iterator notifications cannot mutate subsequent elements.
          reusableObserver = new ByteSizeObserver();
        }
        throw e;
      }

      if (!observer.getIsLazy()) {
        observer.advance();
        this.distribution.update(observer.observedSize);
      } else {
        // Detach observer for lazy iteration so late or escaped iterator notifications
        // never corrupt subsequent elements on reusableObserver.
        pendingLazyObserver = observer;
        reusableObserver = new ByteSizeObserver();
      }
    }

    public void finishLazyUpdate() {
      // Advance lazy ElementByteSizeObservers, if any.
      ByteSizeObserver observer = pendingLazyObserver;
      if (observer != null) {
        pendingLazyObserver = null;
        observer.advance();
        this.distribution.update(observer.observedSize);
      }
    }

    public void discardPendingLazyUpdate() {
      pendingLazyObserver = null;
    }

    private static final int RESERVOIR_SIZE = 10;
    private static final int SAMPLING_THRESHOLD = 30;
    @VisibleForTesting static final int SAMPLING_TOKEN_UPPER_BOUND = 1_000_000;

    private static final double MIN_LOG_COMPLEMENT =
        Math.log1p(-RESERVOIR_SIZE / (double) SAMPLING_TOKEN_UPPER_BOUND);

    private long samplingToken = 0;
    private long nextSamplingToken = 1;

    private boolean shouldSampleElement() {
      long token = ++samplingToken;
      if (token < nextSamplingToken) {
        return false;
      }
      return shouldSampleElementSlowPath(token);
    }

    private boolean shouldSampleElementSlowPath(long token) {
      // Sampling probability decreases as the element count is increasing.
      // We unconditionally sample the first RESERVOIR_SIZE elements. Calculating
      // nextInt(samplingToken) for each element is expensive, so after a threshold, calculate the
      // gap to next sample.
      // https://erikerlandson.github.io/blog/2015/11/20/very-fast-reservoir-sampling/
      if (token >= SAMPLING_TOKEN_UPPER_BOUND) {
        // Cap effective sampling probability at RESERVOIR_SIZE / SAMPLING_TOKEN_UPPER_BOUND
        // (0.001%)
        // so byte-size tracking remains responsive on long-lived streaming bundles.
        samplingToken = SAMPLING_TOKEN_UPPER_BOUND;
        nextSamplingToken = getNextSamplingToken(SAMPLING_TOKEN_UPPER_BOUND);
        return token < Long.MAX_VALUE;
      }
      if (token <= RESERVOIR_SIZE) {
        nextSamplingToken = token + 1;
        return true;
      }
      if (token < SAMPLING_THRESHOLD) {
        nextSamplingToken = token + 1;
        return random().nextInt((int) token) < RESERVOIR_SIZE;
      }
      if (token == SAMPLING_THRESHOLD) {
        boolean sample = random().nextInt((int) token) < RESERVOIR_SIZE;
        nextSamplingToken = getNextSamplingToken(token);
        return sample;
      }
      nextSamplingToken = getNextSamplingToken(token);
      return true;
    }

    @VisibleForTesting
    long getNextSamplingToken(long samplingToken) {
      Preconditions.checkArgument(
          samplingToken > RESERVOIR_SIZE,
          "samplingToken (%s) must exceed RESERVOIR_SIZE (%s)",
          samplingToken,
          RESERVOIR_SIZE);
      long cappedToken = Math.min(samplingToken, SAMPLING_TOKEN_UPPER_BOUND);
      double logComplement =
          cappedToken == SAMPLING_TOKEN_UPPER_BOUND
              ? MIN_LOG_COMPLEMENT
              : Math.log1p(-RESERVOIR_SIZE / (double) cappedToken);
      double gap = Math.log(1.0 - random().nextDouble()) / logComplement;
      if (!(gap < (double) (Long.MAX_VALUE - 1L - samplingToken))) {
        return Long.MAX_VALUE;
      }
      return samplingToken + 1L + (long) Math.max(0.0, gap);
    }

    @VisibleForTesting
    void setSamplingStateForTest(long samplingToken, long nextSamplingToken) {
      Preconditions.checkArgument(
          samplingToken >= 0 && samplingToken < Long.MAX_VALUE,
          "samplingToken (%s) must be in [0, Long.MAX_VALUE)",
          samplingToken);
      Preconditions.checkArgument(
          nextSamplingToken >= 1, "nextSamplingToken (%s) must be >= 1", nextSamplingToken);
      this.samplingToken = samplingToken;
      this.nextSamplingToken = nextSamplingToken;
    }

    @VisibleForTesting
    long getSamplingTokenForTest() {
      return this.samplingToken;
    }
  }
}
