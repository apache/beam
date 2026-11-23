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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFnOutputReceivers;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.InlineableFunctionProvider;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.OutputBuilderSupplier;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValueReceiver;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.OutputBuilder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.PeriodFormat;

/**
 * Specialized flat-stack {@link ParDoFn} for stateless, non-side-input, non-window-observing {@link
 * DoFn}s. Collapses the 12-to-16-frame {@code SimpleParDoFn -> SimpleDoFnRunner ->
 * DoFnProcessContext -> WindowedContextOutputReceiver -> SimpleParDoFnHelpers} adapter chain down
 * to a reusable context with direct receiver dispatch (and 1-frame direct function invocation for
 * pure {@link InlineableFunctionProvider} {@code MapElements}/{@code Filter} transforms).
 */
@SuppressWarnings({"rawtypes", "unchecked", "nullness"})
final class FastStatelessParDoFn<InputT, OutputT>
    extends SimpleParDoFn<InputT, OutputT, BoundedWindow>
    implements FusedSegmentCompiler.StepContextFallback<InputT, OutputT> {
  private static final String COUNTER_NAME = "per-element-output-count";

  static final class ChainLink {
    final @Nullable ElementCounter edgeCounter;
    final OperationContext.FastProcessContext fastContext;
    final FastStatelessParDoFn<?, ?> targetFn;

    ChainLink(
        @Nullable ElementCounter edgeCounter,
        OperationContext.FastProcessContext fastContext,
        FastStatelessParDoFn<?, ?> targetFn) {
      this.edgeCounter = edgeCounter;
      this.fastContext = fastContext;
      this.targetFn = targetFn;
    }
  }

  private static final class RunnerV1StepHook extends FusedSegmentCompiler.StepHook {
    private final @Nullable ElementCounter counter;
    private final @Nullable DataflowOutputCounter dfCounter;
    private final OperationContext.FastProcessContext fastCtx;

    RunnerV1StepHook(
        @Nullable ElementCounter counter, OperationContext.FastProcessContext fastCtx) {
      this.counter = counter;
      this.dfCounter =
          (counter instanceof DataflowOutputCounter) ? (DataflowOutputCounter) counter : null;
      this.fastCtx = fastCtx;
    }

    @Override
    public @Nullable Object beforeStep(WindowedValue<?> elem) throws Exception {
      if (counter != null) {
        counter.update(elem);
      }
      return fastCtx.enterProcessUnscoped();
    }

    @Override
    public void afterStepNormal(WindowedValue<?> elem, @Nullable Object stateToken) {
      fastCtx.exitProcessUnscoped((ExecutionState) stateToken);
      if (counter != null) {
        counter.finishLazyUpdate(elem);
      }
    }

    @Override
    public void afterStepException(
        Throwable t, WindowedValue<?> elem, @Nullable Object stateToken) {
      fastCtx.exitProcessUnscoped((ExecutionState) stateToken);
    }

    @Override
    public @Nullable Object beforeFunc(
        @Nullable Object rawValue, WindowedValue<?> templateElem, int windowsSize)
        throws Exception {
      if (dfCounter != null) {
        dfCounter.updateRawValue(rawValue, templateElem, windowsSize);
      } else if (counter != null) {
        WindowedValue<?> materialized =
            (rawValue == templateElem.getValue()) ? templateElem : templateElem.withValue(rawValue);
        counter.update(materialized);
      }
      return fastCtx.enterProcessUnscoped();
    }

    @Override
    public void exitFuncState(@Nullable Object stateToken) {
      fastCtx.exitProcessUnscoped((ExecutionState) stateToken);
    }

    @Override
    public void afterFuncException(
        Throwable t, WindowedValue<?> templateElem, @Nullable Object stateToken) {
      fastCtx.exitProcessUnscoped((ExecutionState) stateToken);
    }

    @Override
    public void finishFuncLazy() {
      if (dfCounter != null) {
        dfCounter.finishLazyRawUpdate();
      } else if (counter != null) {
        counter.finishLazyUpdate(null);
      }
    }

    @Override
    public void afterRawNormal(@Nullable Object stateToken) {
      fastCtx.exitProcessUnscoped((ExecutionState) stateToken);
      if (dfCounter != null) {
        dfCounter.finishLazyRawUpdate();
      } else if (counter != null) {
        counter.finishLazyUpdate(null);
      }
    }
  }

  private static final class RunnerV1TailReceiver extends FusedSegmentCompiler.TailReceiver {
    private final @Nullable Receiver receiver;

    RunnerV1TailReceiver(@Nullable Receiver receiver) {
      this.receiver = receiver;
    }

    @Override
    public void accept(WindowedValue<?> elem) throws Exception {
      if (receiver != null) {
        receiver.process(elem);
      }
    }
  }

  private final PipelineOptions options;
  private final DoFnInstanceManager doFnInstanceManager;
  private final TupleTag<OutputT> mainOutputTag;
  private final Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices;
  private final DataflowExecutionContext.DataflowStepContext stepContext;
  private final DataflowExecutionContext.DataflowStepContext userStepContext;
  private final DataflowOperationContext operationContext;
  private final DoFnSchemaInformation doFnSchemaInformation;
  private final boolean isStreaming;
  private final boolean hasBundleLifecycle;
  private final boolean compilable;
  private final int frameWeight;
  private final @Nullable Counter<Long, CounterFactory.CounterDistribution> outputsPerElemCounter;
  private long currentElemOutputs;

  // Optional ByteBuddy-compiled linear segment state (non-null only on segment head)
  private @Nullable List<ChainLink> downstreamChainLinks;
  private FusedSegmentCompiler.@Nullable StepHook[] cachedChainHooks;
  private FusedSegmentCompiler.@Nullable CompiledNode cachedCompiledHead;
  private DoFn<?, ?> @Nullable [] boundChainFns;
  private @Nullable Receiver boundTailReceiver;
  private boolean chainVerifiedThisBundle;

  // Per-bundle state
  private Receiver @Nullable [] receivers;
  private @Nullable Receiver mainReceiver;
  private @Nullable Map<TupleTag<?>, OutputReceiver> undeclaredOutputs;
  private @Nullable DoFnInfo<InputT, OutputT> fnInfo;
  private @Nullable DoFn<InputT, OutputT> fn;
  private @Nullable DoFnInvoker<InputT, OutputT> invoker;
  private boolean isSystemDoFn;
  private boolean bundleStarted;
  private @Nullable SchemaCoder<OutputT> mainOutputSchemaCoder;
  private @Nullable ProcessFunction<InputT, OutputT> directMapFn;
  private @Nullable ProcessFunction<InputT, Boolean> directFilterFn;
  private @Nullable FastProcessContext processContext;

  static boolean canUse(
      PipelineOptions options,
      DoFnInfo<?, ?> doFnInfo,
      boolean hasSideInputs,
      DoFnRunnerFactory runnerFactory) {
    if (hasSideInputs || runnerFactory != SimpleDoFnRunnerFactory.INSTANCE) {
      return false;
    }
    List<String> experiments = options.as(DataflowPipelineDebugOptions.class).getExperiments();
    if (experiments != null && experiments.contains("disable_fast_fused_execution")) {
      return false;
    }
    DoFn<?, ?> fn = doFnInfo.getDoFn();
    DoFnSignature sig = DoFnSignatures.getSignature(fn.getClass());
    return sig.stateDeclarations().isEmpty()
        && sig.timerDeclarations().isEmpty()
        && sig.timerFamilyDeclarations().isEmpty()
        && !sig.processElement().isSplittable()
        && !sig.processElement().observesWindow()
        && sig.onWindowExpiration() == null;
  }

  FastStatelessParDoFn(
      PipelineOptions options,
      DoFnInstanceManager doFnInstanceManager,
      TupleTag<OutputT> mainOutputTag,
      Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices,
      DataflowExecutionContext.DataflowStepContext stepContext,
      DataflowOperationContext operationContext,
      DoFnSchemaInformation doFnSchemaInformation) {
    super();
    checkArgument(!outputTupleTagsToReceiverIndices.isEmpty(), "expected at least one output");
    this.options = options;
    this.doFnInstanceManager = doFnInstanceManager;
    this.mainOutputTag = mainOutputTag;
    this.outputTupleTagsToReceiverIndices = outputTupleTagsToReceiverIndices;
    this.stepContext = stepContext;
    this.userStepContext = stepContext.namespacedToUser();
    this.operationContext = operationContext;
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.isStreaming = options.as(StreamingOptions.class).isStreaming();

    DoFnInfo<?, ?> peekInfo;
    try {
      peekInfo = doFnInstanceManager.peek();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    DoFnSignature sig = DoFnSignatures.getSignature(peekInfo.getDoFn().getClass());
    this.hasBundleLifecycle = sig.startBundle() != null || sig.finishBundle() != null;

    List<String> experiments = options.as(DataflowPipelineDebugOptions.class).getExperiments();
    if (experiments != null
        && experiments.contains(SimpleParDoFnHelpers.OUTPUTS_PER_ELEMENT_EXPERIMENT)) {
      this.outputsPerElemCounter =
          operationContext
              .counterFactory()
              .distribution(
                  CounterName.named(COUNTER_NAME).withOriginalName(stepContext.getNameContext()));
    } else {
      this.outputsPerElemCounter = null;
    }

    boolean codegenDisabled =
        experiments != null
            && experiments.contains(FusedSegmentCompiler.DISABLE_CODEGEN_EXPERIMENT);
    Map<TupleTag<?>, Coder<?>> peekCoders = peekInfo.getOutputCoders();
    Coder<?> peekMainCoder = (peekCoders != null) ? peekCoders.get(mainOutputTag) : null;
    this.compilable =
        !codegenDisabled
            && !hasBundleLifecycle
            && outputsPerElemCounter == null
            && outputTupleTagsToReceiverIndices.size() == 1
            && outputTupleTagsToReceiverIndices.containsKey(mainOutputTag)
            && !(peekMainCoder instanceof SchemaCoder);
    boolean isInlineable = peekInfo.getDoFn() instanceof InlineableFunctionProvider;
    if (this.compilable) {
      this.frameWeight = isInlineable ? 2 : 4;
    } else {
      this.frameWeight = isInlineable ? 3 : 5;
    }
  }

  @Override
  public void startBundle(Receiver... receivers) throws Exception {
    checkArgument(
        receivers.length == outputTupleTagsToReceiverIndices.size(),
        "unexpected number of receivers for DoFn");
    this.receivers = receivers;
    Integer mainIdx = outputTupleTagsToReceiverIndices.get(mainOutputTag);
    this.mainReceiver = mainIdx != null ? receivers[mainIdx] : null;
    if (!hasBundleLifecycle) {
      initializeBundleState(false);
    }
  }

  private void initializeBundleState(boolean enterStartState) throws Exception {
    checkState(fnInfo == null, "bundle already started (or not properly finished)");
    fnInfo = (DoFnInfo<InputT, OutputT>) doFnInstanceManager.get();
    fn = fnInfo.getDoFn();
    invoker = DoFnInvokers.invokerFor(fn);
    isSystemDoFn = invoker.getClass().isAnnotationPresent(SystemDoFnInternal.class);

    Map<TupleTag<?>, Coder<?>> outputCoders = fnInfo.getOutputCoders();
    if (outputCoders != null && !outputCoders.isEmpty()) {
      Coder<OutputT> outCoder = (Coder<OutputT>) outputCoders.get(mainOutputTag);
      mainOutputSchemaCoder =
          (outCoder instanceof SchemaCoder) ? (SchemaCoder<OutputT>) outCoder : null;
    } else {
      mainOutputSchemaCoder = null;
    }

    directMapFn = null;
    directFilterFn = null;
    if (fn instanceof InlineableFunctionProvider && mainOutputSchemaCoder == null) {
      InlineableFunctionProvider<InputT, OutputT> provider =
          (InlineableFunctionProvider<InputT, OutputT>) fn;
      if (provider.kind() == InlineableFunctionProvider.Kind.MAP) {
        directMapFn = (ProcessFunction<InputT, OutputT>) provider.function();
      } else if (provider.kind() == InlineableFunctionProvider.Kind.FILTER) {
        directFilterFn = (ProcessFunction<InputT, Boolean>) provider.function();
      }
    }
    if (directMapFn == null && directFilterFn == null) {
      processContext = new FastProcessContext();
    }

    if (hasBundleLifecycle) {
      if (enterStartState) {
        try (Closeable start = operationContext.enterStart()) {
          invokeStartBundle();
        }
      } else {
        invokeStartBundle();
      }
    }
    bundleStarted = true;
  }

  private void invokeStartBundle() {
    try {
      invoker.invokeStartBundle(new StartBundleContextProvider());
    } catch (Exception e) {
      throw UserCodeException.wrapIf(!isSystemDoFn, e);
    }
  }

  @Override
  public void processElement(Object untypedElem) throws Exception {
    if (downstreamChainLinks != null) {
      if (!chainVerifiedThisBundle) {
        bindCompiledChainForBundle();
      }
      WindowedValue<InputT> wv = (WindowedValue<InputT>) untypedElem;
      cachedCompiledHead.acceptRaw(wv.getValue(), wv, wv.getWindows().size());
      return;
    }
    if (!bundleStarted) {
      initializeBundleState(true);
    }
    if (outputsPerElemCounter != null) {
      currentElemOutputs = 0;
    }

    WindowedValue<InputT> elem = (WindowedValue<InputT>) untypedElem;
    Context otel = elem.getOpenTelemetryContext();
    if (otel != null && otel != Context.root()) {
      processElementWithOtel(elem, otel);
    } else if (directMapFn != null) {
      processDirectMap(elem);
    } else if (directFilterFn != null) {
      processDirectFilter(elem);
    } else {
      FastProcessContext ctx = processContext;
      WindowedValue<InputT> prev = ctx.elem;
      ctx.elem = elem;
      try {
        invoker.invokeProcessElement(ctx);
      } catch (Exception e) {
        throw UserCodeException.wrapIf(!isSystemDoFn, e);
      } finally {
        ctx.elem = prev;
      }
    }

    if (outputsPerElemCounter != null) {
      outputsPerElemCounter.addValue(currentElemOutputs);
    }
  }

  private void processElementWithOtel(WindowedValue<InputT> elem, Context otel) throws Exception {
    try (Scope ignore = otel.makeCurrent()) {
      if (directMapFn != null) {
        processDirectMap(elem);
      } else if (directFilterFn != null) {
        processDirectFilter(elem);
      } else {
        FastProcessContext ctx = processContext;
        WindowedValue<InputT> prev = ctx.elem;
        ctx.elem = elem;
        try {
          invoker.invokeProcessElement(ctx);
        } catch (Exception e) {
          throw UserCodeException.wrapIf(!isSystemDoFn, e);
        } finally {
          ctx.elem = prev;
        }
      }
    }
  }

  private void processDirectMap(WindowedValue<InputT> elem) throws Exception {
    OutputT mapped;
    try {
      mapped = directMapFn.apply(elem.getValue());
    } catch (Exception e) {
      throw UserCodeException.wrapIf(!isSystemDoFn, e);
    }
    Context currentOtel = Context.current();
    WindowedValue<OutputT> outVal =
        (currentOtel != Context.root() && currentOtel != elem.getOpenTelemetryContext())
            ? WindowedValues.builder(elem).withValue(mapped).build()
            : elem.withValue(mapped);
    if (outputsPerElemCounter != null) {
      currentElemOutputs++;
    }
    Receiver r = mainReceiver;
    if (r != null) {
      try {
        r.process(outVal);
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Exception e) {
        throw UserCodeException.wrapIf(!isSystemDoFn, e);
      }
    }
  }

  private void processDirectFilter(WindowedValue<InputT> elem) throws Exception {
    boolean keep;
    try {
      keep = directFilterFn.apply(elem.getValue());
    } catch (Exception e) {
      throw UserCodeException.wrapIf(!isSystemDoFn, e);
    }
    if (!keep) {
      return;
    }
    Context currentOtel = Context.current();
    WindowedValue<OutputT> outVal =
        (currentOtel != Context.root() && currentOtel != elem.getOpenTelemetryContext())
            ? (WindowedValue<OutputT>) WindowedValues.builder(elem).build()
            : (WindowedValue<OutputT>) elem;
    if (outputsPerElemCounter != null) {
      currentElemOutputs++;
    }
    Receiver r = mainReceiver;
    if (r != null) {
      try {
        r.process(outVal);
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Exception e) {
        throw UserCodeException.wrapIf(!isSystemDoFn, e);
      }
    }
  }

  @Override
  public void processTimers() {}

  @Override
  public void finishKey(Object key) {}

  @Override
  public void finishBundle() throws Exception {
    if (bundleStarted) {
      if (hasBundleLifecycle) {
        try {
          invoker.invokeFinishBundle(new FinishBundleContextProvider());
        } catch (Exception e) {
          throw UserCodeException.wrapIf(!isSystemDoFn, e);
        }
      }
      doFnInstanceManager.complete(fnInfo);
      resetBundleState();
    }
  }

  @Override
  public void abort() throws Exception {
    if (fnInfo != null) {
      doFnInstanceManager.abort(fnInfo);
    }
    resetBundleState();
  }

  int estimatedFrameWeight() {
    return frameWeight;
  }

  boolean canCompileInSegment() {
    return compilable;
  }

  int compiledLinkFrameWeight() {
    return frameWeight == 2 ? 1 : 3;
  }

  void setDownstreamChain(List<ChainLink> links) {
    this.downstreamChainLinks = links;
    this.cachedChainHooks = new FusedSegmentCompiler.StepHook[links.size()];
    for (int i = 0; i < links.size(); i++) {
      ChainLink link = links.get(i);
      this.cachedChainHooks[i] = new RunnerV1StepHook(link.edgeCounter, link.fastContext);
    }
  }

  private FusedSegmentCompiler.StepSpec toStepSpec(
      FusedSegmentCompiler.@Nullable StepHook entryHook) throws Exception {
    if (!bundleStarted) {
      initializeBundleState(false);
    }
    if (directMapFn != null) {
      return new FusedSegmentCompiler.FunctionalStepSpec(
          InlineableFunctionProvider.Kind.MAP, directMapFn, entryHook, isSystemDoFn, false);
    } else if (directFilterFn != null) {
      return new FusedSegmentCompiler.FunctionalStepSpec(
          InlineableFunctionProvider.Kind.FILTER, directFilterFn, entryHook, isSystemDoFn, false);
    }
    return new FusedSegmentCompiler.DoFnStepSpec<>(
        fn, invoker, entryHook, this, isSystemDoFn, false);
  }

  private void bindCompiledChainForBundle() throws Exception {
    if (!bundleStarted) {
      initializeBundleState(false);
    }
    List<ChainLink> links = this.downstreamChainLinks;
    int totalSteps = 1 + links.size();
    FastStatelessParDoFn<?, ?> lastStep = links.get(links.size() - 1).targetFn;
    if (!lastStep.bundleStarted) {
      lastStep.initializeBundleState(false);
    }
    Receiver currentTail = lastStep.mainReceiver;

    if (cachedCompiledHead != null
        && boundChainFns != null
        && boundTailReceiver == currentTail
        && boundChainFns[0] == this.fn) {
      boolean allMatch = true;
      for (int i = 0; i < links.size(); i++) {
        FastStatelessParDoFn<?, ?> target = links.get(i).targetFn;
        if (!target.bundleStarted) {
          target.initializeBundleState(false);
        }
        if (boundChainFns[i + 1] != target.fn) {
          allMatch = false;
          break;
        }
      }
      if (allMatch) {
        chainVerifiedThisBundle = true;
        return;
      }
    }

    DoFn<?, ?>[] newFns = new DoFn<?, ?>[totalSteps];
    List<FusedSegmentCompiler.StepSpec> specs = new ArrayList<>(totalSteps);
    newFns[0] = this.fn;
    specs.add(this.toStepSpec(null));
    for (int i = 0; i < links.size(); i++) {
      FastStatelessParDoFn<?, ?> target = links.get(i).targetFn;
      if (!target.bundleStarted) {
        target.initializeBundleState(false);
      }
      newFns[i + 1] = target.fn;
      specs.add(target.toStepSpec(cachedChainHooks[i]));
    }
    FusedSegmentCompiler.CompiledNode tailNode =
        (currentTail instanceof FusedSegmentCompiler.CompiledNode)
            ? (FusedSegmentCompiler.CompiledNode) currentTail
            : new RunnerV1TailReceiver(currentTail);
    this.cachedCompiledHead = FusedSegmentCompiler.compileChain(specs, tailNode);
    this.boundChainFns = newFns;
    this.boundTailReceiver = currentTail;
    this.chainVerifiedThisBundle = true;
  }

  @Override
  public PipelineOptions pipelineOptions() {
    return options;
  }

  @Override
  public BundleFinalizer bundleFinalizer() {
    return userStepContext.bundleFinalizer();
  }

  @Override
  public void checkTimestamp(WindowedValue<InputT> elem, Instant timestamp) {
    checkTimestamp(elem.getTimestamp(), timestamp);
  }

  @Override
  public boolean isMainOutputTag(TupleTag<?> tag) {
    return mainOutputTag.equals(tag);
  }

  @Override
  public <T> void outputTagged(WindowedValue<InputT> elem, TupleTag<T> tag, T output) {
    Context currentOtel = Context.current();
    if (currentOtel != Context.root() && currentOtel != elem.getOpenTelemetryContext()) {
      outputTaggedWindowedValue(
          elem, tag, output, elem.getTimestamp(), elem.getWindows(), elem.getPaneInfo());
      return;
    }
    dispatchTagged(tag, elem.withValue(output));
  }

  @Override
  public <T> void outputTaggedWithTimestamp(
      WindowedValue<InputT> elem, TupleTag<T> tag, T output, Instant timestamp) {
    checkTimestamp(elem.getTimestamp(), timestamp);
    Context currentOtel = Context.current();
    if (currentOtel != Context.root() && currentOtel != elem.getOpenTelemetryContext()) {
      outputTaggedWindowedValue(
          elem, tag, output, timestamp, elem.getWindows(), elem.getPaneInfo());
      return;
    }
    dispatchTagged(
        tag,
        WindowedValues.of(
            output,
            timestamp,
            elem.getWindows(),
            elem.getPaneInfo(),
            elem.getRecordId(),
            elem.getRecordOffset(),
            elem.causedByDrain(),
            elem.getOpenTelemetryContext(),
            elem.getValueKind()));
  }

  @Override
  public <T> void outputTaggedWindowedValue(
      WindowedValue<InputT> elem,
      TupleTag<T> tag,
      T output,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo paneInfo) {
    WindowedValues.builder((WindowedValue<T>) elem)
        .withValue(output)
        .setTimestamp(timestamp)
        .setWindows(windows)
        .setPaneInfo(paneInfo)
        .setReceiver(
            wv -> {
              checkTimestamp(elem.getTimestamp(), wv.getTimestamp());
              dispatchTagged(tag, wv);
            })
        .output();
  }

  @Override
  public <T> void outputTaggedWindowedValue(
      WindowedValue<InputT> elem, TupleTag<T> tag, WindowedValue<T> windowedValue) {
    checkTimestamp(elem.getTimestamp(), windowedValue.getTimestamp());
    dispatchTagged(tag, windowedValue);
  }

  @Override
  public Object schemaElement(WindowedValue<InputT> elem, int index) {
    checkStateNotNull(
        doFnSchemaInformation,
        "attempt to access element via schema when no schema information provided");
    SerializableFunction<InputT, Object> converter =
        (SerializableFunction<InputT, Object>)
            doFnSchemaInformation.getElementConverters().get(index);
    return converter.apply(elem.getValue());
  }

  @Override
  @Nullable DoFnInfo<?, ?> getDoFnInfo() {
    return fnInfo;
  }

  private void resetBundleState() {
    fnInfo = null;
    fn = null;
    invoker = null;
    processContext = null;
    directMapFn = null;
    directFilterFn = null;
    undeclaredOutputs = null;
    bundleStarted = false;
    chainVerifiedThisBundle = false;
  }

  private void dispatchMain(WindowedValue<OutputT> output) {
    if (outputsPerElemCounter != null) {
      currentElemOutputs++;
    }
    Receiver r = mainReceiver;
    if (r == null) {
      dispatchTagged(mainOutputTag, output);
      return;
    }
    try {
      r.process(output);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private <TagT> void dispatchTagged(TupleTag<TagT> tag, WindowedValue<TagT> output) {
    if (outputsPerElemCounter != null) {
      currentElemOutputs++;
    }
    Integer idx = outputTupleTagsToReceiverIndices.get(tag);
    Receiver r;
    if (idx != null && receivers != null) {
      r = receivers[idx];
    } else {
      if (receivers == null) {
        // Test invocation without startBundle(receivers)
        if (!outputTupleTagsToReceiverIndices.containsKey(tag)) {
          throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
        }
        return;
      }
      if (undeclaredOutputs == null) {
        undeclaredOutputs = new HashMap<>();
      }
      OutputReceiver undeclared = undeclaredOutputs.get(tag);
      if (undeclared == null) {
        String outputName = "implicit-" + tag.getId();
        undeclared = new OutputReceiver();
        ElementCounter outputCounter =
            DataflowOutputCounter.create(
                outputName,
                operationContext.counterFactory(),
                stepContext.getNameContext(),
                isStreaming);
        undeclared.addOutputCounter(outputCounter);
        undeclaredOutputs.put(tag, undeclared);
      }
      r = undeclared;
    }
    if (r != null) {
      try {
        r.process(output);
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @SuppressWarnings("deprecation")
  private void checkTimestamp(Instant elemTimestamp, Instant timestamp) {
    Duration skew = fn.getAllowedTimestampSkew();
    Instant lowerBound;
    try {
      lowerBound = elemTimestamp.minus(skew);
    } catch (ArithmeticException e) {
      lowerBound = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }
    if (timestamp.isBefore(lowerBound) || timestamp.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot output with timestamp %s. Output timestamps must be no earlier than the "
                  + "timestamp of the current input or timer (%s) minus the allowed skew (%s) and no "
                  + "later than %s. See the DoFn#getAllowedTimestampSkew() Javadoc for details "
                  + "on changing the allowed skew.",
              timestamp,
              elemTimestamp,
              skew.getMillis() >= Integer.MAX_VALUE
                  ? skew
                  : PeriodFormat.getDefault().print(skew.toPeriod()),
              BoundedWindow.TIMESTAMP_MAX_VALUE));
    }
  }

  private final class FastStartBundleContext extends DoFn<InputT, OutputT>.StartBundleContext {
    FastStartBundleContext() {
      fn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }
  }

  private final class StartBundleContextProvider
      extends DoFnInvoker.BaseArgumentProvider<InputT, OutputT> {
    private final DoFn<InputT, OutputT>.StartBundleContext ctx = new FastStartBundleContext();

    @Override
    public PipelineOptions pipelineOptions() {
      return options;
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      return ctx;
    }

    @Override
    public BundleFinalizer bundleFinalizer() {
      return userStepContext.bundleFinalizer();
    }

    @Override
    public String getErrorContext() {
      return "FastStatelessParDoFn/StartBundle";
    }
  }

  private final class FastFinishBundleContext extends DoFn<InputT, OutputT>.FinishBundleContext {
    FastFinishBundleContext() {
      fn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public void output(OutputT output, Instant timestamp, BoundedWindow window) {
      dispatchMain(WindowedValues.of(output, timestamp, window, PaneInfo.NO_FIRING));
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
      checkArgument(
          outputTupleTagsToReceiverIndices.containsKey(tag), "Unknown output tag %s", tag);
      dispatchTagged(tag, WindowedValues.of(output, timestamp, window, PaneInfo.NO_FIRING));
    }
  }

  private final class FinishBundleContextProvider
      extends DoFnInvoker.BaseArgumentProvider<InputT, OutputT> {
    private final DoFn<InputT, OutputT>.FinishBundleContext ctx = new FastFinishBundleContext();

    @Override
    public PipelineOptions pipelineOptions() {
      return options;
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      return ctx;
    }

    @Override
    public BundleFinalizer bundleFinalizer() {
      return userStepContext.bundleFinalizer();
    }

    @Override
    public String getErrorContext() {
      return "FastStatelessParDoFn/FinishBundle";
    }
  }

  private final class FastProcessContext extends DoFn<InputT, OutputT>.ProcessContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT>,
          DoFn.OutputReceiver<OutputT>,
          WindowedValueReceiver<OutputT> {

    WindowedValue<InputT> elem;
    private final OutputBuilderSupplier dynamicBuilderSupplier =
        new OutputBuilderSupplier() {
          @Override
          public <T> WindowedValues.Builder<T> builder(T value) {
            return WindowedValues.builder((WindowedValue<T>) elem).withValue(value);
          }
        };
    private DoFn.@Nullable OutputReceiver<Row> cachedRowReceiver;
    private @Nullable MultiOutputReceiver cachedMultiReceiver;

    FastProcessContext() {
      fn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return options;
    }

    @Override
    public InputT element() {
      return elem.getValue();
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      return elem.getValue();
    }

    @Override
    public Instant timestamp() {
      return elem.getTimestamp();
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return elem.getTimestamp();
    }

    @Override
    public PaneInfo pane() {
      return elem.getPaneInfo();
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      return elem.getPaneInfo();
    }

    @Override
    public CausedByDrain causedByDrain() {
      return elem.causedByDrain();
    }

    @Override
    public CausedByDrain causedByDrain(DoFn<InputT, OutputT> doFn) {
      return elem.causedByDrain();
    }

    @Override
    public @Nullable String currentRecordId() {
      return elem.getRecordId();
    }

    @Override
    public @Nullable String currentRecordId(DoFn<InputT, OutputT> doFn) {
      return elem.getRecordId();
    }

    @Override
    public @Nullable Long currentRecordOffset() {
      return elem.getRecordOffset();
    }

    @Override
    public @Nullable Long currentRecordOffset(DoFn<InputT, OutputT> doFn) {
      return elem.getRecordOffset();
    }

    @Override
    public ValueKind valueKind() {
      return elem.getValueKind();
    }

    @Override
    public ValueKind valueKind(DoFn<InputT, OutputT> doFn) {
      return elem.getValueKind();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      throw new IllegalArgumentException("calling sideInput() with unknown view");
    }

    @Override
    public Object sideInput(String tagId) {
      throw new IllegalArgumentException("calling sideInput() with unknown tag " + tagId);
    }

    @Override
    public OutputBuilder<OutputT> builder(OutputT value) {
      return WindowedValues.builder(dynamicBuilderSupplier.builder(value)).setReceiver(this);
    }

    @Override
    public void output(OutputT output) {
      Context currentOtel = Context.current();
      if (currentOtel != Context.root() && currentOtel != elem.getOpenTelemetryContext()) {
        builder(output).output();
        return;
      }
      dispatchMain(elem.withValue(output));
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      checkTimestamp(elem.getTimestamp(), timestamp);
      Context currentOtel = Context.current();
      if (currentOtel != Context.root() && currentOtel != elem.getOpenTelemetryContext()) {
        builder(output).setTimestamp(timestamp).output();
        return;
      }
      dispatchMain(
          WindowedValues.of(
              output,
              timestamp,
              elem.getWindows(),
              elem.getPaneInfo(),
              elem.getRecordId(),
              elem.getRecordOffset(),
              elem.causedByDrain(),
              elem.getOpenTelemetryContext(),
              elem.getValueKind()));
    }

    @Override
    public void output(WindowedValue<OutputT> windowedValue) {
      outputWindowedValue(windowedValue);
    }

    @Override
    public void outputWindowedValue(WindowedValue<OutputT> windowedValue) {
      checkTimestamp(elem.getTimestamp(), windowedValue.getTimestamp());
      dispatchMain(windowedValue);
    }

    @Override
    public void outputWindowedValue(
        OutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo) {
      outputWindowedValue(mainOutputTag, output, timestamp, windows, paneInfo);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      checkNotNull(tag, "Tag passed to output cannot be null");
      if (mainOutputTag.equals(tag)) {
        output((OutputT) output);
        return;
      }
      checkArgument(
          outputTupleTagsToReceiverIndices.containsKey(tag), "Unknown output tag %s", tag);
      Context currentOtel = Context.current();
      if (currentOtel != Context.root() && currentOtel != elem.getOpenTelemetryContext()) {
        outputWindowedValue(
            tag, output, elem.getTimestamp(), elem.getWindows(), elem.getPaneInfo());
        return;
      }
      dispatchTagged(tag, elem.withValue(output));
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkNotNull(tag, "Tag passed to outputWithTimestamp cannot be null");
      if (mainOutputTag.equals(tag)) {
        outputWithTimestamp((OutputT) output, timestamp);
        return;
      }
      checkArgument(
          outputTupleTagsToReceiverIndices.containsKey(tag), "Unknown output tag %s", tag);
      checkTimestamp(elem.getTimestamp(), timestamp);
      Context currentOtel = Context.current();
      if (currentOtel != Context.root() && currentOtel != elem.getOpenTelemetryContext()) {
        outputWindowedValue(tag, output, timestamp, elem.getWindows(), elem.getPaneInfo());
        return;
      }
      dispatchTagged(
          tag,
          WindowedValues.of(
              output,
              timestamp,
              elem.getWindows(),
              elem.getPaneInfo(),
              elem.getRecordId(),
              elem.getRecordOffset(),
              elem.causedByDrain(),
              elem.getOpenTelemetryContext(),
              elem.getValueKind()));
    }

    @Override
    public <T> void outputWindowedValue(TupleTag<T> tag, WindowedValue<T> windowedValue) {
      checkArgument(
          outputTupleTagsToReceiverIndices.containsKey(tag), "Unknown output tag %s", tag);
      checkTimestamp(elem.getTimestamp(), windowedValue.getTimestamp());
      dispatchTagged(tag, windowedValue);
    }

    @Override
    public <T> void outputWindowedValue(
        TupleTag<T> tag,
        T output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo) {
      checkArgument(
          outputTupleTagsToReceiverIndices.containsKey(tag), "Unknown output tag %s", tag);
      dynamicBuilderSupplier
          .builder(output)
          .setTimestamp(timestamp)
          .setWindows(windows)
          .setPaneInfo(paneInfo)
          .setReceiver(
              wv -> {
                checkTimestamp(elem.getTimestamp(), wv.getTimestamp());
                dispatchTagged(tag, wv);
              })
          .output();
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public DoFn.OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public DoFn.OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
      checkStateNotNull(mainOutputSchemaCoder, "cannot provide row receiver without schema coder");
      if (cachedRowReceiver == null) {
        cachedRowReceiver =
            DoFnOutputReceivers.rowReceiver(
                this, dynamicBuilderSupplier, null, mainOutputSchemaCoder);
      }
      return cachedRowReceiver;
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      if (cachedMultiReceiver == null) {
        cachedMultiReceiver =
            DoFnOutputReceivers.windowedMultiReceiver(
                this, dynamicBuilderSupplier, fnInfo.getOutputCoders());
      }
      return cachedMultiReceiver;
    }

    @Override
    public Object schemaElement(int index) {
      checkStateNotNull(
          doFnSchemaInformation,
          "attempt to access element via schema when no schema information provided");
      SerializableFunction<InputT, Object> converter =
          (SerializableFunction<InputT, Object>)
              doFnSchemaInformation.getElementConverters().get(index);
      return converter.apply(elem.getValue());
    }

    @Override
    public BundleFinalizer bundleFinalizer() {
      return userStepContext.bundleFinalizer();
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException("Window parameter not supported in non-windowed fn");
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("StartBundleContext parameters are not supported.");
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("FinishBundleContext parameters are not supported.");
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("OnTimerContext parameters are not supported.");
    }

    @Override
    public DoFn<InputT, OutputT>.OnWindowExpirationContext onWindowExpirationContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "OnWindowExpirationContext parameters are not supported.");
    }

    @Override
    public Object key() {
      throw new UnsupportedOperationException("Key parameter not supported outside @OnTimer.");
    }

    @Override
    public Instant fireTimestamp(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("fireTimestamp not supported outside @OnTimer.");
    }

    @Override
    public String timerId(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("timerId not supported outside @OnTimer.");
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("timeDomain not supported outside @OnTimer.");
    }

    @Override
    public Object restriction() {
      throw new UnsupportedOperationException("Restriction parameter not supported.");
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
      throw new UnsupportedOperationException("RestrictionTracker parameter not supported.");
    }

    @Override
    public Object watermarkEstimatorState() {
      throw new UnsupportedOperationException("WatermarkEstimatorState parameter not supported.");
    }

    @Override
    public WatermarkEstimator<?> watermarkEstimator() {
      throw new UnsupportedOperationException("WatermarkEstimator parameter not supported.");
    }

    @Override
    public State state(String stateId, boolean alwaysFetched) {
      throw new UnsupportedOperationException("State not supported in stateless DoFn.");
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException("Timer not supported in stateless DoFn.");
    }

    @Override
    public TimerMap timerFamily(String timerFamilyId) {
      throw new UnsupportedOperationException("TimerFamily not supported in stateless DoFn.");
    }
  }
}
