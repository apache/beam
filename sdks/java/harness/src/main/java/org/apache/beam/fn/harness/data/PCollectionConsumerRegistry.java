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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
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
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.CompiledNode;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowedValues.WindowedValueCoder;
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
      return forConsumer(consumer, pTransformId, state, stateTracker, 5, null);
    }

    public static ConsumerAndMetadata forConsumer(
        FnDataReceiver consumer,
        String pTransformId,
        ExecutionState state,
        ExecutionStateTracker stateTracker,
        int downstreamFrameDepth) {
      return forConsumer(consumer, pTransformId, state, stateTracker, downstreamFrameDepth, null);
    }

    public static ConsumerAndMetadata forConsumer(
        FnDataReceiver consumer,
        String pTransformId,
        ExecutionState state,
        ExecutionStateTracker stateTracker,
        int downstreamFrameDepth,
        @Nullable CompiledNode compiledConsumer) {
      return new AutoValue_PCollectionConsumerRegistry_ConsumerAndMetadata(
          consumer, pTransformId, state, stateTracker, downstreamFrameDepth, compiledConsumer);
    }

    public abstract FnDataReceiver getConsumer();

    public abstract String getPTransformId();

    public abstract ExecutionState getExecutionState();

    public abstract ExecutionStateTracker getExecutionStateTracker();

    public abstract int getDownstreamFrameDepth();

    public abstract @Nullable CompiledNode getCompiledConsumer();
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
  private final Map<String, Integer> pCollectionIdsToEffectiveDepth = new HashMap<>();
  private final Map<String, Integer> pCollectionProducerCounts = new HashMap<>();
  private final Map<String, Integer> pCollectionConsumerCounts = new HashMap<>();
  private final Map<String, String> singleParDoInToOut = new HashMap<>();
  private final Map<String, String> singleParDoOutToIn = new HashMap<>();
  private final Map<String, PendingCompiledChain> pendingCompiledChains = new HashMap<>();
  private final Set<String> flattenOutputPCollections = new HashSet<>();
  private final Map<ExecutionStateKey, ExecutionState> executionStates = new HashMap<>();
  private final BundleProgressReporter.Registrar bundleProgressReporterRegistrar;
  private final ProcessBundleDescriptor processBundleDescriptor;
  private final RehydratedComponents rehydratedComponents;
  private final @Nullable DataSampler dataSampler;
  private final @Nullable HarnessStageTrampoline stageTrampoline;
  private final int trampolineThreshold;
  private final int minCodegenChainSteps;
  private static final Logger LOG = LoggerFactory.getLogger(PCollectionConsumerRegistry.class);

  public PCollectionConsumerRegistry(
      ExecutionStateTracker stateTracker,
      ShortIdMap shortIdMap,
      BundleProgressReporter.Registrar bundleProgressReporterRegistrar,
      ProcessBundleDescriptor processBundleDescriptor) {
    this(
        stateTracker,
        shortIdMap,
        bundleProgressReporterRegistrar,
        processBundleDescriptor,
        null,
        null,
        0,
        FusedSegmentCompiler.DEFAULT_MIN_CODEGEN_CHAIN_STEPS);
  }

  public PCollectionConsumerRegistry(
      ExecutionStateTracker stateTracker,
      ShortIdMap shortIdMap,
      BundleProgressReporter.Registrar bundleProgressReporterRegistrar,
      ProcessBundleDescriptor processBundleDescriptor,
      @Nullable DataSampler dataSampler) {
    this(
        stateTracker,
        shortIdMap,
        bundleProgressReporterRegistrar,
        processBundleDescriptor,
        dataSampler,
        null,
        0,
        FusedSegmentCompiler.DEFAULT_MIN_CODEGEN_CHAIN_STEPS);
  }

  public PCollectionConsumerRegistry(
      ExecutionStateTracker stateTracker,
      ShortIdMap shortIdMap,
      BundleProgressReporter.Registrar bundleProgressReporterRegistrar,
      ProcessBundleDescriptor processBundleDescriptor,
      @Nullable DataSampler dataSampler,
      @Nullable HarnessStageTrampoline stageTrampoline,
      int trampolineThreshold) {
    this(
        stateTracker,
        shortIdMap,
        bundleProgressReporterRegistrar,
        processBundleDescriptor,
        dataSampler,
        stageTrampoline,
        trampolineThreshold,
        FusedSegmentCompiler.DEFAULT_MIN_CODEGEN_CHAIN_STEPS);
  }

  public PCollectionConsumerRegistry(
      ExecutionStateTracker stateTracker,
      ShortIdMap shortIdMap,
      BundleProgressReporter.Registrar bundleProgressReporterRegistrar,
      ProcessBundleDescriptor processBundleDescriptor,
      @Nullable DataSampler dataSampler,
      @Nullable HarnessStageTrampoline stageTrampoline,
      int trampolineThreshold,
      int minCodegenChainSteps) {
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
    this.stageTrampoline = stageTrampoline;
    this.trampolineThreshold = trampolineThreshold;
    this.minCodegenChainSteps = minCodegenChainSteps;
    for (PTransform pTransform : processBundleDescriptor.getTransformsMap().values()) {
      if (PTransformTranslation.FLATTEN_TRANSFORM_URN.equals(pTransform.getSpec().getUrn())) {
        this.flattenOutputPCollections.addAll(pTransform.getOutputsMap().values());
      }
      if (pTransform.getSubtransformsCount() == 0) {
        for (String outPc : pTransform.getOutputsMap().values()) {
          this.pCollectionProducerCounts.merge(outPc, 1, Integer::sum);
        }
        for (String inPc : pTransform.getInputsMap().values()) {
          this.pCollectionConsumerCounts.merge(inPc, 1, Integer::sum);
        }
        if (PTransformTranslation.PAR_DO_TRANSFORM_URN.equals(pTransform.getSpec().getUrn())
            && pTransform.getInputsCount() == 1
            && pTransform.getOutputsCount() == 1) {
          String inPc = pTransform.getInputsMap().values().iterator().next();
          String outPc = pTransform.getOutputsMap().values().iterator().next();
          this.singleParDoInToOut.put(inPc, outPc);
          this.singleParDoOutToIn.put(outPc, inPc);
        }
      }
    }
  }

  private int estimateMaxLinearParDoChainSteps(String inPCollectionId, String outPCollectionId) {
    int count = 1;
    Set<String> seen = new HashSet<>();
    seen.add(inPCollectionId);
    seen.add(outPCollectionId);

    // Walk upstream while single-producer, single-consumer 1-to-1 ParDo
    String curIn = inPCollectionId;
    while (pCollectionProducerCounts.getOrDefault(curIn, 0) == 1
        && pCollectionConsumerCounts.getOrDefault(curIn, 0) == 1
        && !flattenOutputPCollections.contains(curIn)) {
      String prevIn = singleParDoOutToIn.get(curIn);
      if (prevIn == null || !seen.add(prevIn)) {
        break;
      }
      count++;
      curIn = prevIn;
    }

    // Walk downstream while single-producer, single-consumer 1-to-1 ParDo
    String curOut = outPCollectionId;
    while (pCollectionProducerCounts.getOrDefault(curOut, 0) == 1
        && pCollectionConsumerCounts.getOrDefault(curOut, 0) == 1
        && !flattenOutputPCollections.contains(curOut)) {
      String nextOut = singleParDoInToOut.get(curOut);
      if (nextOut == null || !seen.add(nextOut)) {
        break;
      }
      count++;
      curOut = nextOut;
    }
    return count;
  }

  private static final FnDataReceiver<WindowedValue<?>> CLAIMED_INLINE_CONSUMER =
      elem -> {
        throw new IllegalStateException("PCollection was claimed inline by a compiled segment");
      };

  private static final class PendingCompiledChain {
    final List<FusedSegmentCompiler.StepSpec> stepsFromHeadToTail;
    final FusedSegmentCompiler.CompiledNode tailNode;

    PendingCompiledChain(
        List<FusedSegmentCompiler.StepSpec> stepsFromHeadToTail,
        FusedSegmentCompiler.CompiledNode tailNode) {
      this.stepsFromHeadToTail = stepsFromHeadToTail;
      this.tailNode = tailNode;
    }
  }

  private static final class RunnerV2TailReceiver extends FusedSegmentCompiler.TailReceiver {
    private final FnDataReceiver<WindowedValue<?>> downstream;

    RunnerV2TailReceiver(FnDataReceiver<WindowedValue<?>> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void accept(WindowedValue<?> elem) throws Exception {
      downstream.accept(elem);
    }
  }

  /**
   * Concrete {@code public final} per-edge step hook for Portable Java SDK Harness compiled
   * segments so generated classes emit monomorphic {@code INVOKEVIRTUAL} calls directly.
   */
  public static final class RunnerV2StepHook<T> extends FusedSegmentCompiler.StepHook {
    private final ExecutionState executionState;
    private final ExecutionStateTracker executionStateTracker;
    private final String ptransformId;
    private final BundleCounter elementCountCounter;
    private final SampleByteSizeDistribution<T> sampledByteSizeDistribution;
    private final Coder<T> coder;

    RunnerV2StepHook(
        ShortIdMap shortIdMap,
        BundleProgressReporter.Registrar bundleProgressReporterRegistrar,
        String pCollectionId,
        Coder<T> coder,
        ConsumerAndMetadata consumerMeta) {
      this.executionState = consumerMeta.getExecutionState();
      this.executionStateTracker = consumerMeta.getExecutionStateTracker();
      this.ptransformId = consumerMeta.getPTransformId();
      this.coder = coder;

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
    }

    @Override
    @SuppressWarnings("unchecked")
    public @Nullable Object beforeStep(WindowedValue<?> elem) throws Exception {
      this.elementCountCounter.inc(elem.getWindows().size());
      this.sampledByteSizeDistribution.tryUpdate((T) elem.getValue(), this.coder);
      this.executionState.activate();
      return null;
    }

    @Override
    public void afterStepNormal(WindowedValue<?> elem, @Nullable Object stateToken) {
      this.executionState.deactivate();
      this.sampledByteSizeDistribution.finishLazyUpdate();
    }

    @Override
    public void afterStepException(Throwable t, WindowedValue<?> elem, @Nullable Object stateToken)
        throws Exception {
      this.executionState.deactivate();
      if (t instanceof Exception) {
        logAndRethrow(
            (Exception) t, executionState, executionStateTracker, ptransformId, null, null);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public @Nullable Object beforeFunc(
        @Nullable Object rawValue, WindowedValue<?> templateElem, int windowsSize)
        throws Exception {
      this.elementCountCounter.inc(windowsSize);
      this.sampledByteSizeDistribution.tryUpdate((T) rawValue, this.coder);
      this.executionState.activate();
      return null;
    }

    @Override
    public void exitFuncState(@Nullable Object stateToken) {
      this.executionState.deactivate();
    }

    @Override
    public void afterFuncException(
        Throwable t, WindowedValue<?> templateElem, @Nullable Object stateToken) throws Exception {
      this.executionState.deactivate();
      if (t instanceof Exception) {
        logAndRethrow(
            (Exception) t, executionState, executionStateTracker, ptransformId, null, null);
      }
    }

    @Override
    public void finishFuncLazy() {
      this.sampledByteSizeDistribution.finishLazyUpdate();
    }

    @Override
    public void afterRawNormal(@Nullable Object stateToken) {
      this.executionState.deactivate();
      this.sampledByteSizeDistribution.finishLazyUpdate();
    }
  }

  private Coder<?> resolveUnwrappedValueCoder(String pCollectionId) {
    if (!processBundleDescriptor.containsPcollections(pCollectionId)) {
      throw new IllegalArgumentException(String.format("Unknown PCollection id %s", pCollectionId));
    }
    String coderId = processBundleDescriptor.getPcollectionsOrThrow(pCollectionId).getCoderId();
    try {
      Coder<?> maybeWvCoder = rehydratedComponents.getCoder(coderId);
      if (maybeWvCoder instanceof WindowedValueCoder) {
        return ((WindowedValueCoder) maybeWvCoder).getValueCoder();
      }
      return maybeWvCoder;
    } catch (IOException e) {
      throw new IllegalStateException(String.format("Unable to materialize coder %s", coderId), e);
    }
  }

  private static final class LazyCompiledChainHead
      implements FnDataReceiver<WindowedValue<Object>>, CompiledNode {
    private final PendingCompiledChain chain;
    private @Nullable CompiledNode compiled;

    LazyCompiledChainHead(PendingCompiledChain chain) {
      this.chain = chain;
    }

    private FusedSegmentCompiler.CompiledNode resolve() {
      FusedSegmentCompiler.CompiledNode node = this.compiled;
      if (node == null) {
        node = FusedSegmentCompiler.compileChain(chain.stepsFromHeadToTail, chain.tailNode);
        this.compiled = node;
      }
      return node;
    }

    @Override
    public void accept(WindowedValue<Object> elem) throws Exception {
      resolve().acceptRaw(elem.getValue(), elem, elem.getWindows().size());
    }

    @Override
    public void acceptWindowed(WindowedValue<?> elem) throws Exception {
      resolve().acceptRaw(elem.getValue(), elem, elem.getWindows().size());
    }

    @Override
    public void acceptRaw(@Nullable Object rawValue, WindowedValue<?> templateElem, int windowsSize)
        throws Exception {
      resolve().acceptRaw(rawValue, templateElem, windowsSize);
    }
  }

  public boolean tryRegisterCompilableStep(
      String inPCollectionId,
      String pTransformId,
      String pTransformUniqueName,
      String outPCollectionId,
      FusedSegmentCompiler.StepSpec stepSpecWithoutEntryHook,
      int selfFrameWeight) {
    if (minCodegenChainSteps <= 0
        || pCollectionIdsToWrappedConsumer.containsKey(inPCollectionId)
        || !processBundleDescriptor.containsPcollections(outPCollectionId)
        || estimateMaxLinearParDoChainSteps(inPCollectionId, outPCollectionId)
            < minCodegenChainSteps) {
      return false;
    }

    PendingCompiledChain downstreamPending = pendingCompiledChains.get(outPCollectionId);
    List<ConsumerAndMetadata> outConsumers = pCollectionIdsToConsumers.get(outPCollectionId);
    boolean canFuseIntoDownstream = false;
    if (downstreamPending != null
        && outConsumers != null
        && outConsumers.size() == 1
        && !pCollectionIdsToWrappedConsumer.containsKey(outPCollectionId)
        && !flattenOutputPCollections.contains(outPCollectionId)
        && dataSampler == null
        && pCollectionProducerCounts.getOrDefault(outPCollectionId, 0) == 1) {
      ConsumerAndMetadata outMeta = outConsumers.get(0);
      boolean wouldCut =
          stageTrampoline != null
              && trampolineThreshold > 0
              && outMeta.getDownstreamFrameDepth() + 1 > trampolineThreshold;
      if (!(outMeta.getConsumer() instanceof HandlesSplits) && !wouldCut) {
        canFuseIntoDownstream = true;
      }
    }

    final PendingCompiledChain newChain;
    final int downstreamDepth;
    if (canFuseIntoDownstream) {
      pendingCompiledChains.remove(outPCollectionId);
      ConsumerAndMetadata outMeta = outConsumers.get(0);
      Coder<?> outValueCoder = resolveUnwrappedValueCoder(outPCollectionId);
      RunnerV2StepHook<?> edgeHook =
          new RunnerV2StepHook<>(
              shortIdMap,
              bundleProgressReporterRegistrar,
              outPCollectionId,
              (Coder) outValueCoder,
              outMeta);
      pCollectionIdsToWrappedConsumer.put(outPCollectionId, CLAIMED_INLINE_CONSUMER);

      List<FusedSegmentCompiler.StepSpec> combined =
          new ArrayList<>(1 + downstreamPending.stepsFromHeadToTail.size());
      combined.add(stepSpecWithoutEntryHook);
      combined.add(downstreamPending.stepsFromHeadToTail.get(0).withEntryHook(edgeHook));
      for (int i = 1; i < downstreamPending.stepsFromHeadToTail.size(); i++) {
        combined.add(downstreamPending.stepsFromHeadToTail.get(i));
      }
      newChain = new PendingCompiledChain(combined, downstreamPending.tailNode);
      downstreamDepth = outMeta.getDownstreamFrameDepth();
    } else {
      FnDataReceiver<WindowedValue<?>> wrappedOut = getMultiplexingConsumer(outPCollectionId);
      List<FusedSegmentCompiler.StepSpec> single = new ArrayList<>(1);
      single.add(stepSpecWithoutEntryHook);
      FusedSegmentCompiler.CompiledNode tailNode =
          (wrappedOut instanceof FusedSegmentCompiler.CompiledNode)
              ? (FusedSegmentCompiler.CompiledNode) wrappedOut
              : new RunnerV2TailReceiver(wrappedOut);
      newChain = new PendingCompiledChain(single, tailNode);
      downstreamDepth = getEffectiveDepth(outPCollectionId);
    }

    FnDataReceiver<WindowedValue<Object>> lazyCompiledReceiver =
        new LazyCompiledChainHead(newChain);

    int effectiveSelfWeight =
        canFuseIntoDownstream
            ? ((stepSpecWithoutEntryHook instanceof FusedSegmentCompiler.FunctionalStepSpec)
                ? 1
                : 2)
            : selfFrameWeight;
    register(
        inPCollectionId,
        pTransformId,
        pTransformUniqueName,
        lazyCompiledReceiver,
        effectiveSelfWeight + downstreamDepth);
    if (pCollectionIdsToConsumers.get(inPCollectionId).size() == 1) {
      pendingCompiledChains.put(inPCollectionId, newChain);
    } else {
      pendingCompiledChains.remove(inPCollectionId);
    }
    return true;
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
    register(pCollectionId, pTransformId, pTransformUniqueName, consumer, 5);
  }

  public <T> void register(
      String pCollectionId,
      String pTransformId,
      String pTransformUniqueName,
      FnDataReceiver<WindowedValue<T>> consumer,
      int downstreamFrameDepth) {
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
    if (!consumerAndMetadatas.isEmpty()) {
      pendingCompiledChains.remove(pCollectionId);
    }
    consumerAndMetadatas.add(
        ConsumerAndMetadata.forConsumer(
            consumer,
            pTransformId,
            getProcessingExecutionState(pTransformId, pTransformUniqueName),
            stateTracker,
            downstreamFrameDepth));
  }

  public int getEffectiveDepth(String pCollectionId) {
    Integer depth = pCollectionIdsToEffectiveDepth.get(pCollectionId);
    return depth != null ? depth : 1;
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
          PendingCompiledChain pending = pendingCompiledChains.remove(pcId);
          if (pending != null) {
            List<ConsumerAndMetadata> existing = pCollectionIdsToConsumers.get(pcId);
            if (existing != null && existing.size() == 1) {
              ConsumerAndMetadata oldMeta = existing.get(0);
              FusedSegmentCompiler.CompiledNode compiledHead =
                  FusedSegmentCompiler.compileChain(pending.stepsFromHeadToTail, pending.tailNode);
              FnDataReceiver<WindowedValue<?>> compiledReceiver = compiledHead::acceptWindowed;
              existing.set(
                  0,
                  ConsumerAndMetadata.forConsumer(
                      compiledReceiver,
                      oldMeta.getPTransformId(),
                      oldMeta.getExecutionState(),
                      oldMeta.getExecutionStateTracker(),
                      oldMeta.getDownstreamFrameDepth(),
                      compiledHead));
            }
          }
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

          int maxConsumerDepth = 0;
          boolean hasHandlesSplits = false;
          for (ConsumerAndMetadata cm : consumerAndMetadatas) {
            maxConsumerDepth = Math.max(maxConsumerDepth, cm.getDownstreamFrameDepth());
            if (cm.getConsumer() instanceof HandlesSplits) {
              hasHandlesSplits = true;
            }
          }

          boolean shouldCut =
              !hasHandlesSplits
                  && !flattenOutputPCollections.contains(pcId)
                  && stageTrampoline != null
                  && trampolineThreshold > 0
                  && maxConsumerDepth + 1 > trampolineThreshold;

          // Never cut immediately after Flatten so predecessor PCollection lazy byte-size observers
          // remain atomic across FlattenRunner into the consuming PTransform.
          // Subclassing MetricTrackingFnDataReceiver keeps upstream FnDataReceiver call sites
          // within
          // the MetricTrackingFnDataReceiver class hierarchy instead of introducing a third
          // receiver
          // implementation.
          if (consumerAndMetadatas.size() == 1) {
            ConsumerAndMetadata consumerAndMetadata = consumerAndMetadatas.get(0);
            if (consumerAndMetadata.getConsumer() instanceof HandlesSplits) {
              pCollectionIdsToEffectiveDepth.put(pcId, maxConsumerDepth + 1);
              return new SplittingMetricTrackingFnDataReceiver(
                  pcId, coder, consumerAndMetadata, sampler);
            }
            if (shouldCut) {
              pCollectionIdsToEffectiveDepth.put(pcId, 1);
              return new CutMetricTrackingFnDataReceiver(
                  pcId, coder, consumerAndMetadata, sampler, stageTrampoline);
            }
            pCollectionIdsToEffectiveDepth.put(pcId, maxConsumerDepth + 1);
            return new MetricTrackingFnDataReceiver(pcId, coder, consumerAndMetadata, sampler);
          } else {
            /* TODO(SDF), Consider supporting splitting each consumer individually. This would never
            come up in the existing SDF expansion, but might be useful to support fused SDF nodes.
            This would require dedicated delivery of the split results to each of the consumers
            separately. */
            if (shouldCut) {
              pCollectionIdsToEffectiveDepth.put(pcId, 1);
              return new CutMultiplexingMetricTrackingFnDataReceiver(
                  pcId, coder, consumerAndMetadatas, sampler, stageTrampoline);
            }
            pCollectionIdsToEffectiveDepth.put(pcId, maxConsumerDepth + 2);
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
    ExecutionStateSampler.ExecutionStateTrackerStatus status = executionStateTracker.getStatus();
    String processBundleId = status == null ? null : status.getProcessBundleId();
    if (outputSampler != null) {
      outputSampler.exception(elementSample, e, ptransformId, processBundleId);
    }

    if (executionState.error()) {
      LOG.error("Failed to process element for bundle \"{}\"", processBundleId, e);
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
  public class MetricTrackingFnDataReceiver<T>
      implements FnDataReceiver<WindowedValue<T>>, CompiledNode {
    private final FnDataReceiver<WindowedValue<T>> delegate;
    private final @Nullable CompiledNode compiledDelegate;
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
      this.compiledDelegate = consumerAndMetadata.getCompiledConsumer();
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
    public void acceptWindowed(WindowedValue<?> elem) throws Exception {
      accept((WindowedValue<T>) elem);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void acceptRaw(@Nullable Object rawValue, WindowedValue<?> templateElem, int windowsSize)
        throws Exception {
      this.elementCountCounter.inc(windowsSize);
      this.sampledByteSizeDistribution.tryUpdate((T) rawValue, this.coder);

      WindowedValue<T> input =
          (rawValue == templateElem.getValue())
              ? (WindowedValue<T>) templateElem
              : (WindowedValue<T>) templateElem.withValue(rawValue);

      ElementSample<T> elementSample = null;
      if (outputSampler != null) {
        elementSample = outputSampler.sample(input);
      }

      executionState.activate();
      try {
        if (this.compiledDelegate != null) {
          this.compiledDelegate.acceptRaw(rawValue, input, windowsSize);
        } else {
          this.delegate.accept(input);
        }
      } catch (Exception e) {
        executionState.deactivate();
        logAndRethrow(
            e, executionState, executionStateTracker, ptransformId, outputSampler, elementSample);
        return;
      } catch (Throwable t) {
        executionState.deactivate();
        throw t;
      }
      executionState.deactivate();
      this.sampledByteSizeDistribution.finishLazyUpdate();
    }

    @Override
    public void accept(WindowedValue<T> input) throws Exception {
      // Increment the counter for each window the element occurs in.
      int windowsSize = input.getWindows().size();
      this.elementCountCounter.inc(windowsSize);
      // TODO(https://github.com/apache/beam/issues/20730): Consider updating size per window when
      // we have window optimization.
      T rawValue = input.getValue();
      this.sampledByteSizeDistribution.tryUpdate(rawValue, this.coder);

      ElementSample<T> elementSample = null;
      if (outputSampler != null) {
        elementSample = outputSampler.sample(input);
      }

      // Use the ExecutionStateTracker and enter an appropriate state to track the
      // Process Bundle Execution time metric and also ensure user counters can get an appropriate
      // metrics container.
      executionState.activate();
      try {
        if (this.compiledDelegate != null) {
          this.compiledDelegate.acceptRaw(rawValue, input, windowsSize);
        } else {
          this.delegate.accept(input);
        }
      } catch (Exception e) {
        executionState.deactivate();
        logAndRethrow(
            e, executionState, executionStateTracker, ptransformId, outputSampler, elementSample);
        return;
      } catch (Throwable t) {
        executionState.deactivate();
        throw t;
      }
      executionState.deactivate();
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
  public class MultiplexingMetricTrackingFnDataReceiver<T>
      implements FnDataReceiver<WindowedValue<T>>, FusedSegmentCompiler.CompiledNode {
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
          logAndRethrow(
              e,
              state,
              consumerAndMetadata.getExecutionStateTracker(),
              consumerAndMetadata.getPTransformId(),
              outputSampler,
              elementSample);
        }
        this.sampledByteSizeDistribution.finishLazyUpdate();
      }
    }

    @Override
    public void acceptWindowed(WindowedValue<?> elem) throws Exception {
      accept((WindowedValue<T>) elem);
    }

    @Override
    public void acceptRaw(@Nullable Object rawValue, WindowedValue<?> templateElem, int windowsSize)
        throws Exception {
      WindowedValue<T> elem =
          (rawValue == templateElem.getValue())
              ? (WindowedValue<T>) templateElem
              : (WindowedValue<T>) templateElem.withValue(rawValue);
      accept(elem);
    }
  }

  /**
   * A wrapping {@code FnDataReceiver<WindowedValue<T>>} which counts the number of elements
   * consumed by the original {@code FnDataReceiver<WindowedValue<T>> consumer} and forwards split
   * and progress requests to the original consumer.
   *
   * @param <T> - The receiving type of the PTransform.
   */
  public class SplittingMetricTrackingFnDataReceiver<T> extends MetricTrackingFnDataReceiver<T>
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

  public class CutMetricTrackingFnDataReceiver<T> extends MetricTrackingFnDataReceiver<T> {
    private final FnDataReceiver<WindowedValue<T>> cut;

    public CutMetricTrackingFnDataReceiver(
        String pCollection,
        Coder<T> coder,
        ConsumerAndMetadata consumerAndMetadata,
        @Nullable OutputSampler<T> outputSampler,
        HarnessStageTrampoline trampoline) {
      super(pCollection, coder, consumerAndMetadata, outputSampler);
      this.cut = trampoline.createCut(super::accept);
    }

    @Override
    public void accept(WindowedValue<T> input) throws Exception {
      this.cut.accept(input);
    }

    @Override
    public void acceptRaw(@Nullable Object rawValue, WindowedValue<?> templateElem, int windowsSize)
        throws Exception {
      WindowedValue<T> elem =
          (rawValue == templateElem.getValue())
              ? (WindowedValue<T>) templateElem
              : (WindowedValue<T>) templateElem.withValue(rawValue);
      this.cut.accept(elem);
    }
  }

  public class CutMultiplexingMetricTrackingFnDataReceiver<T>
      extends MultiplexingMetricTrackingFnDataReceiver<T> {
    private final FnDataReceiver<WindowedValue<T>> cut;

    public CutMultiplexingMetricTrackingFnDataReceiver(
        String pCollection,
        Coder<T> coder,
        List<ConsumerAndMetadata> consumerAndMetadatas,
        @Nullable OutputSampler<T> outputSampler,
        HarnessStageTrampoline trampoline) {
      super(pCollection, coder, consumerAndMetadatas, outputSampler);
      this.cut = trampoline.createCut(super::accept);
    }

    @Override
    public void accept(WindowedValue<T> input) throws Exception {
      this.cut.accept(input);
    }
  }

  private static class SampleByteSizeDistribution<T> {
    /** Basic implementation of {@link ElementByteSizeObserver} for use in size estimation. */
    private static class ByteSizeObserver extends ElementByteSizeObserver {
      private long observedSize = 0;

      @Override
      protected void reportElementSize(long elementSize) {
        observedSize += elementSize;
      }
    }

    final Distribution distribution;
    ByteSizeObserver byteCountObserver;

    public SampleByteSizeDistribution(Distribution distribution) {
      this.distribution = distribution;
      this.byteCountObserver = null;
    }

    public void tryUpdate(T value, Coder<T> coder) throws Exception {
      if (shouldSampleElement()) {
        // First try using byte size observer
        byteCountObserver = new ByteSizeObserver();
        coder.registerByteSizeObserver(value, byteCountObserver);

        if (!byteCountObserver.getIsLazy()) {
          byteCountObserver.advance();
          this.distribution.update(byteCountObserver.observedSize);
        }
      } else {
        byteCountObserver = null;
      }
    }

    public void finishLazyUpdate() {
      // Advance lazy ElementByteSizeObservers, if any.
      if (byteCountObserver != null && byteCountObserver.getIsLazy()) {
        byteCountObserver.advance();
        this.distribution.update(byteCountObserver.observedSize);
      }
    }

    private static final int RESERVOIR_SIZE = 10;
    private static final int SAMPLING_THRESHOLD = 30;
    private long samplingToken = 0;
    private long nextSamplingToken = 0;
    private Random randomGenerator = new Random();

    private boolean shouldSampleElement() {
      // Sampling probability decreases as the element count is increasing.
      // We unconditionally sample the first samplingCutoff elements. Calculating
      // nextInt(samplingToken) for each element is expensive, so after a threshold, calculate the
      // gap to next sample.
      // https://erikerlandson.github.io/blog/2015/11/20/very-fast-reservoir-sampling/

      // Reset samplingToken if it's going to exceed the max value.
      if (samplingToken + 1 == Long.MAX_VALUE) {
        samplingToken = 0;
        nextSamplingToken = getNextSamplingToken(samplingToken);
      }

      samplingToken++;
      // Use traditional sampling until the threshold of 30
      if (nextSamplingToken == 0) {
        if (samplingToken <= RESERVOIR_SIZE
            || randomGenerator.nextInt((int) samplingToken) < RESERVOIR_SIZE) {
          if (samplingToken > SAMPLING_THRESHOLD) {
            nextSamplingToken = getNextSamplingToken(samplingToken);
          }
          return true;
        }
      } else if (samplingToken >= nextSamplingToken) {
        nextSamplingToken = getNextSamplingToken(samplingToken);
        return true;
      }
      return false;
    }

    private long getNextSamplingToken(long samplingToken) {
      double gap =
          Math.log(1.0 - randomGenerator.nextDouble())
              / Math.log(1.0 - RESERVOIR_SIZE / (double) samplingToken);
      return samplingToken + (int) gap;
    }
  }
}
