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
import org.apache.beam.fn.harness.HandlesSplits;
import org.apache.beam.fn.harness.control.BundleProgressReporter;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionState;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionStateTracker;
import org.apache.beam.fn.harness.control.Metrics;
import org.apache.beam.fn.harness.control.Metrics.BundleCounter;
import org.apache.beam.fn.harness.control.Metrics.BundleDistribution;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.RehydratedComponents;
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
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.grpc.v1p48p1.io.netty.util.internal.ThreadLocalRandom;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

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
        FnDataReceiver consumer, String pTransformId, ExecutionState state) {
      return new AutoValue_PCollectionConsumerRegistry_ConsumerAndMetadata(
          consumer, pTransformId, state);
    }

    public abstract FnDataReceiver getConsumer();

    public abstract String getPTransformId();

    public abstract ExecutionState getExecutionState();
  }

  private final ExecutionStateTracker stateTracker;
  private final ShortIdMap shortIdMap;
  private final Map<String, List<ConsumerAndMetadata>> pCollectionIdsToConsumers;
  private final Map<String, FnDataReceiver> pCollectionIdsToWrappedConsumer;
  private final BundleProgressReporter.Registrar bundleProgressReporterRegistrar;
  private final ProcessBundleDescriptor processBundleDescriptor;
  private final RehydratedComponents rehydratedComponents;

  public PCollectionConsumerRegistry(
      ExecutionStateTracker stateTracker,
      ShortIdMap shortIdMap,
      BundleProgressReporter.Registrar bundleProgressReporterRegistrar,
      ProcessBundleDescriptor processBundleDescriptor) {
    this.stateTracker = stateTracker;
    this.shortIdMap = shortIdMap;
    this.pCollectionIdsToConsumers = new HashMap<>();
    this.pCollectionIdsToWrappedConsumer = new HashMap<>();
    this.bundleProgressReporterRegistrar = bundleProgressReporterRegistrar;
    this.processBundleDescriptor = processBundleDescriptor;
    this.rehydratedComponents =
        RehydratedComponents.forComponents(
            RunnerApi.Components.newBuilder()
                .putAllCoders(processBundleDescriptor.getCodersMap())
                .putAllPcollections(processBundleDescriptor.getPcollectionsMap())
                .putAllWindowingStrategies(processBundleDescriptor.getWindowingStrategiesMap())
                .build());
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

    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.PROCESS_BUNDLE_MSECS);
    builder.setType(MonitoringInfoConstants.TypeUrns.SUM_INT64_TYPE);
    builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, pTransformId);
    MonitoringInfo mi = builder.build();
    if (mi == null) {
      throw new IllegalStateException(
          String.format(
              "Unable to construct %s counter for PTransform {id=%s, name=%s}",
              MonitoringInfoConstants.Urns.PROCESS_BUNDLE_MSECS,
              pTransformId,
              pTransformUniqueName));
    }
    String shortId = shortIdMap.getOrCreateShortId(mi);
    ExecutionState executionState =
        stateTracker.create(
            shortId,
            pTransformId,
            pTransformUniqueName,
            org.apache.beam.runners.core.metrics.ExecutionStateTracker.PROCESS_STATE_NAME);

    List<ConsumerAndMetadata> consumerAndMetadatas =
        pCollectionIdsToConsumers.computeIfAbsent(pCollectionId, (unused) -> new ArrayList<>());
    consumerAndMetadatas.add(
        ConsumerAndMetadata.forConsumer(consumer, pTransformId, executionState));
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
          try {
            Coder<?> maybeWindowedValueInputCoder = rehydratedComponents.getCoder(coderId);
            // TODO: Stop passing windowed value coders within PCollections.
            if (maybeWindowedValueInputCoder instanceof WindowedValue.WindowedValueCoder) {
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
              return new SplittingMetricTrackingFnDataReceiver(pcId, coder, consumerAndMetadata);
            }
            return new MetricTrackingFnDataReceiver(pcId, coder, consumerAndMetadata);
          } else {
            /* TODO(SDF), Consider supporting splitting each consumer individually. This would never
            come up in the existing SDF expansion, but might be useful to support fused SDF nodes.
            This would require dedicated delivery of the split results to each of the consumers
            separately. */
            return new MultiplexingMetricTrackingFnDataReceiver(
                pcId, coder, ImmutableList.copyOf(consumerAndMetadatas));
          }
        });
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

    public MetricTrackingFnDataReceiver(
        String pCollectionId, Coder<T> coder, ConsumerAndMetadata consumerAndMetadata) {
      this.delegate = consumerAndMetadata.getConsumer();
      this.executionState = consumerAndMetadata.getExecutionState();

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
    }

    @Override
    public void accept(WindowedValue<T> input) throws Exception {
      // Increment the counter for each window the element occurs in.
      this.elementCountCounter.inc(input.getWindows().size());
      // TODO(https://github.com/apache/beam/issues/20730): Consider updating size per window when
      // we have window optimization.
      this.sampledByteSizeDistribution.tryUpdate(input.getValue(), this.coder);

      // Use the ExecutionStateTracker and enter an appropriate state to track the
      // Process Bundle Execution time metric and also ensure user counters can get an appropriate
      // metrics container.
      executionState.activate();
      try {
        this.delegate.accept(input);
      } finally {
        executionState.deactivate();
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

    public MultiplexingMetricTrackingFnDataReceiver(
        String pCollectionId, Coder<T> coder, List<ConsumerAndMetadata> consumerAndMetadatas) {
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
    }

    @Override
    public void accept(WindowedValue<T> input) throws Exception {
      // Increment the counter for each window the element occurs in.
      this.elementCountCounter.inc(input.getWindows().size());
      // TODO(https://github.com/apache/beam/issues/20730): Consider updating size per window
      // when we have window optimization.
      this.sampledByteSizeDistribution.tryUpdate(input.getValue(), coder);

      // Use the ExecutionStateTracker and enter an appropriate state to track the
      // Process Bundle Execution time metric and also ensure user counters can get an appropriate
      // metrics container.
      for (ConsumerAndMetadata consumerAndMetadata : consumerAndMetadatas) {
        ExecutionState state = consumerAndMetadata.getExecutionState();
        state.activate();
        try {
          consumerAndMetadata.getConsumer().accept(input);
        } finally {
          state.deactivate();
        }
        this.sampledByteSizeDistribution.finishLazyUpdate();
      }
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
        String pCollection, Coder<T> coder, ConsumerAndMetadata consumerAndMetadata) {
      super(pCollection, coder, consumerAndMetadata);
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

  public static class SampleByteSizeDistribution<T> extends ElementByteSizeObserver {
    /** Basic implementation of {@link ElementByteSizeObserver} for use in size estimation. */
    @Override
    protected void reportElementSize(long elementSize) {
      distribution.update(elementSize);
    }

    final Distribution distribution;

    public SampleByteSizeDistribution(Distribution distribution) {
      this.distribution = distribution;
    }

    public void tryUpdate(T value, Coder<T> coder) throws Exception {
      if (shouldSampleElement()) {
        coder.registerByteSizeObserver(value, this);

        if (!getIsLazy()) {
          advance();
        }
      }
    }

    public void finishLazyUpdate() {
      // Advance lazy ElementByteSizeObservers, if any.
      // Note that user's code is allowed to store the element of one
      // DoFn.processElement() call and access it later on. We are still
      // calling next() here, causing an update to byteCount. If user's
      // code really accesses more element's pieces later on, their byte
      // count would accrue against a future element. This is not ideal,
      // but still approximately correct.
      if (getIsLazy()) {
        advance();
      }
    }

    // Lowest sampling probability: 0.001%.
    private static final int SAMPLING_TOKEN_UPPER_BOUND = 1000000;
    private static final int SAMPLING_CUTOFF = 10;
    private int samplingToken = 0;
    private Random randomGenerator = ThreadLocalRandom.current();

    private boolean shouldSampleElement() {
      // Sampling probability decreases as the element count is increasing.
      // We unconditionally sample the first samplingCutoff elements. For the
      // next samplingCutoff elements, the sampling probability drops from 100%
      // to 50%. The probability of sampling the Nth element is:
      // min(1, samplingCutoff / N), with an additional lower bound of
      // samplingCutoff / samplingTokenUpperBound. This algorithm may be refined
      // later.
      samplingToken = Math.min(samplingToken + 1, SAMPLING_TOKEN_UPPER_BOUND);
      return randomGenerator.nextInt(samplingToken) < SAMPLING_CUTOFF;
    }
  }
}
