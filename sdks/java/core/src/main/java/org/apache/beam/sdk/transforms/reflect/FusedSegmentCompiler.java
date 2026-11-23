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
package org.apache.beam.sdk.transforms.reflect;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Ownership;
import net.bytebuddy.description.modifier.TypeManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.jar.asm.ClassWriter;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.InlineableFunctionProvider;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.MapMaker;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Compiles linear segments of stateless {@link DoFn}s and {@link InlineableFunctionProvider}
 * transforms ({@code MapElements}, {@code Filter}, {@code Values}, {@code Keys}, {@code WithKeys})
 * into specialized ByteBuddy glue classes so that every step has its own monomorphic bytecode call
 * site (BCI) and exact downstream receiver type.
 *
 * <p>We never modify or rewrite user {@link DoFn} bytecode. Generated glue classes are cached per
 * {@link ClassLoader} in a bounded cache keyed by step class tuples so repeated bundles reuse the
 * exact same generated classes without Metaspace growth.
 */
@Internal
@SuppressWarnings({"rawtypes", "unchecked", "nullness"})
public final class FusedSegmentCompiler {

  public static final String DISABLE_CODEGEN_EXPERIMENT = "disable_codegen_fused_segments";

  /** Maximum number of consecutive functional (MAP/FILTER) steps fused into a single method. */
  static final int MAX_FUNCTIONS_PER_CHUNK = 16;

  private static final int MAX_CACHED_CLASSES_PER_LOADER = 4096;

  private static final ConcurrentMap<ClassLoader, LoaderNamespace> LOADER_NAMESPACES =
      new MapMaker().weakKeys().makeMap();

  private static final LoaderNamespace BOOTSTRAP_NAMESPACE =
      new LoaderNamespace(FusedSegmentCompiler.class.getClassLoader());

  private static final AtomicLong GENERATED_CLASS_COUNTER = new AtomicLong();

  private static final Context ROOT_OTEL_CONTEXT = Context.root();

  private static final ThreadLocal<MethodHandle> PENDING_CLINIT_MH = new ThreadLocal<>();

  private FusedSegmentCompiler() {}

  /**
   * Compiled executable node accepting a {@link WindowedValue} or unpacked raw value + template.
   */
  public interface CompiledNode {
    void acceptWindowed(WindowedValue<?> elem) throws Exception;

    void acceptRaw(@Nullable Object rawValue, WindowedValue<?> templateElem, int windowsSize)
        throws Exception;
  }

  /** Internal callback interface implemented by generated functional segment classes. */
  public interface FuncSegmentBody extends CompiledNode {
    void runBody(@Nullable Object rawValue, WindowedValue<?> templateElem, int windowsSize)
        throws Exception;
  }

  /** Terminal consumer at the exit of a compiled segment. */
  public abstract static class TailReceiver implements CompiledNode {
    public abstract void accept(WindowedValue<?> elem) throws Exception;

    @Override
    public void acceptWindowed(WindowedValue<?> elem) throws Exception {
      accept(elem);
    }

    @Override
    public void acceptRaw(@Nullable Object rawValue, WindowedValue<?> templateElem, int windowsSize)
        throws Exception {
      WindowedValue<?> out =
          (rawValue == templateElem.getValue()) ? templateElem : templateElem.withValue(rawValue);
      accept(out);
    }
  }

  /**
   * Runner-specific per-edge metrics and {@code ExecutionState} lifecycle hook invoked on
   * transitions between compiled steps.
   */
  public abstract static class StepHook {
    public abstract @Nullable Object beforeStep(WindowedValue<?> elem) throws Exception;

    public abstract void afterStepNormal(WindowedValue<?> elem, @Nullable Object stateToken);

    public abstract void afterStepException(
        Throwable t, WindowedValue<?> elem, @Nullable Object stateToken) throws Exception;

    public abstract @Nullable Object beforeFunc(
        @Nullable Object rawValue, WindowedValue<?> templateElem, int windowsSize) throws Exception;

    public abstract void exitFuncState(@Nullable Object stateToken);

    public abstract void afterFuncException(
        Throwable t, WindowedValue<?> templateElem, @Nullable Object stateToken) throws Exception;

    public abstract void finishFuncLazy();

    public void afterRawNormal(@Nullable Object stateToken) {
      exitFuncState(stateToken);
      finishFuncLazy();
    }
  }

  /** Cold-path context fallback for uncommon {@link DoFn.ProcessContext} operations. */
  public interface StepContextFallback<InputT, OutputT> {
    PipelineOptions pipelineOptions();

    BundleFinalizer bundleFinalizer();

    void checkTimestamp(WindowedValue<InputT> elem, Instant timestamp);

    boolean isMainOutputTag(TupleTag<?> tag);

    <T> void outputTagged(WindowedValue<InputT> elem, TupleTag<T> tag, T output);

    <T> void outputTaggedWithTimestamp(
        WindowedValue<InputT> elem, TupleTag<T> tag, T output, Instant timestamp);

    <T> void outputTaggedWindowedValue(
        WindowedValue<InputT> elem,
        TupleTag<T> tag,
        T output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo);

    <T> void outputTaggedWindowedValue(
        WindowedValue<InputT> elem, TupleTag<T> tag, WindowedValue<T> windowedValue);

    Object schemaElement(WindowedValue<InputT> elem, int index);
  }

  /** Specification for a single step in a linear compilable segment. */
  public abstract static class StepSpec {
    final @Nullable StepHook entryHook;

    StepSpec(@Nullable StepHook entryHook) {
      this.entryHook = entryHook;
    }

    abstract ClassLoader stepClassLoader();

    public abstract StepSpec withEntryHook(@Nullable StepHook newEntryHook);
  }

  /**
   * Specification for an {@link InlineableFunctionProvider} ({@code MAP} or {@code FILTER}) step.
   */
  public static final class FunctionalStepSpec extends StepSpec {
    final InlineableFunctionProvider.Kind kind;
    final ProcessFunction<?, ?> fn;
    final boolean isSystemDoFn;
    final boolean wrapExceptionsAsUserCode;

    public FunctionalStepSpec(
        InlineableFunctionProvider.Kind kind,
        ProcessFunction<?, ?> fn,
        @Nullable StepHook entryHook,
        boolean isSystemDoFn,
        boolean wrapExceptionsAsUserCode) {
      super(entryHook);
      this.kind = checkNotNull(kind);
      this.fn = checkNotNull(fn);
      this.isSystemDoFn = isSystemDoFn;
      this.wrapExceptionsAsUserCode = wrapExceptionsAsUserCode;
    }

    @Override
    ClassLoader stepClassLoader() {
      return fn.getClass().getClassLoader();
    }

    @Override
    public FunctionalStepSpec withEntryHook(@Nullable StepHook newEntryHook) {
      return new FunctionalStepSpec(kind, fn, newEntryHook, isSystemDoFn, wrapExceptionsAsUserCode);
    }
  }

  /** Specification for a general stateless single-output {@link DoFn} step. */
  public static final class DoFnStepSpec<InputT, OutputT> extends StepSpec {
    final DoFn<InputT, OutputT> fn;
    final DoFnInvoker<InputT, OutputT> invoker;
    final StepContextFallback<InputT, OutputT> fallback;
    final boolean isSystemDoFn;
    final boolean wrapDownstreamInUserCode;

    public DoFnStepSpec(
        DoFn<InputT, OutputT> fn,
        DoFnInvoker<InputT, OutputT> invoker,
        @Nullable StepHook entryHook,
        StepContextFallback<InputT, OutputT> fallback,
        boolean isSystemDoFn,
        boolean wrapDownstreamInUserCode) {
      super(entryHook);
      this.fn = checkNotNull(fn);
      this.invoker = checkNotNull(invoker);
      this.fallback = checkNotNull(fallback);
      this.isSystemDoFn = isSystemDoFn;
      this.wrapDownstreamInUserCode = wrapDownstreamInUserCode;
    }

    @Override
    ClassLoader stepClassLoader() {
      return fn.getClass().getClassLoader();
    }

    @Override
    public DoFnStepSpec<InputT, OutputT> withEntryHook(@Nullable StepHook newEntryHook) {
      return new DoFnStepSpec<>(
          fn, invoker, newEntryHook, fallback, isSystemDoFn, wrapDownstreamInUserCode);
    }
  }

  /**
   * Base class for generated per-edge {@link DoFn} context/receiver glue nodes.
   *
   * <p>Subclasses override {@link #acceptWindowed}, {@link #output(Object)}, and {@link
   * #emitWindowed} with specialized monomorphic bytecode targeting the exact downstream node class.
   */
  public abstract static class FusedDoFnNodeBase<InputT, OutputT>
      extends DoFn<InputT, OutputT>.ProcessContext
      implements CompiledNode,
          DoFnInvoker.ArgumentProvider<InputT, OutputT>,
          DoFn.OutputReceiver<OutputT>,
          WindowedValueReceiver<OutputT> {

    public @Nullable InputT currentVal;
    public @Nullable WindowedValue<InputT> templateElem;
    public int currentWinSize;
    protected final StepContextFallback<InputT, OutputT> fallback;
    private @Nullable MultiOutputReceiver cachedMultiReceiver;

    protected FusedDoFnNodeBase(
        DoFn<InputT, OutputT> fn, StepContextFallback<InputT, OutputT> fallback) {
      fn.super();
      this.fallback = fallback;
    }

    @Override
    public final void acceptWindowed(WindowedValue<?> elem) throws Exception {
      acceptRaw(elem.getValue(), elem, elem.getWindows().size());
    }

    protected abstract void emitWindowed(WindowedValue<?> wv) throws Exception;

    private WindowedValue<InputT> currentMaterializedElem() {
      WindowedValue<InputT> tmpl = this.templateElem;
      if (tmpl == null) {
        throw new IllegalStateException(
            "Attempting to emit an element outside of a @ProcessElement context.");
      }
      InputT v = this.currentVal;
      return (v == tmpl.getValue()) ? tmpl : tmpl.withValue(v);
    }

    public final void outputSlowOtel(OutputT value) {
      builder(value).output();
    }

    @Override
    public final OutputBuilder<OutputT> builder(OutputT value) {
      WindowedValue<InputT> tmpl = this.templateElem;
      if (tmpl == null) {
        throw new IllegalStateException(
            "Attempting to emit an element outside of a @ProcessElement context.");
      }
      return WindowedValues.builder((WindowedValue<OutputT>) tmpl)
          .withValue(value)
          .setReceiver(this);
    }

    @Override
    public void output(WindowedValue<OutputT> windowedValue) {
      outputWindowedValue(windowedValue);
    }

    @Override
    public void outputWindowedValue(WindowedValue<OutputT> windowedValue) {
      WindowedValue<InputT> tmpl = this.templateElem;
      if (tmpl != null) {
        fallback.checkTimestamp(tmpl, windowedValue.getTimestamp());
      }
      try {
        emitWindowed(windowedValue);
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      WindowedValue<InputT> tmpl = this.templateElem;
      if (tmpl == null) {
        throw new IllegalStateException(
            "Attempting to emit an element outside of a @ProcessElement context.");
      }
      fallback.checkTimestamp(tmpl, timestamp);
      if (hasChangedOtel(tmpl)) {
        builder(output).setTimestamp(timestamp).output();
        return;
      }
      try {
        emitWindowed(
            WindowedValues.of(
                output,
                timestamp,
                tmpl.getWindows(),
                tmpl.getPaneInfo(),
                tmpl.getRecordId(),
                tmpl.getRecordOffset(),
                tmpl.causedByDrain(),
                tmpl.getOpenTelemetryContext(),
                tmpl.getValueKind()));
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void outputWindowedValue(
        OutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo) {
      builder(output).setTimestamp(timestamp).setWindows(windows).setPaneInfo(paneInfo).output();
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      checkNotNull(tag, "Tag passed to output cannot be null");
      if (fallback.isMainOutputTag(tag)) {
        output((OutputT) output);
        return;
      }
      fallback.outputTagged(currentMaterializedElem(), tag, output);
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkNotNull(tag, "Tag passed to outputWithTimestamp cannot be null");
      if (fallback.isMainOutputTag(tag)) {
        outputWithTimestamp((OutputT) output, timestamp);
        return;
      }
      fallback.outputTaggedWithTimestamp(currentMaterializedElem(), tag, output, timestamp);
    }

    @Override
    public <T> void outputWindowedValue(
        TupleTag<T> tag,
        T output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo paneInfo) {
      checkNotNull(tag, "Tag passed to outputWindowedValue cannot be null");
      if (fallback.isMainOutputTag(tag)) {
        outputWindowedValue((OutputT) output, timestamp, windows, paneInfo);
        return;
      }
      fallback.outputTaggedWindowedValue(
          currentMaterializedElem(), tag, output, timestamp, windows, paneInfo);
    }

    @Override
    public <T> void outputWindowedValue(TupleTag<T> tag, WindowedValue<T> windowedValue) {
      checkNotNull(tag, "Tag passed to outputWindowedValue cannot be null");
      if (fallback.isMainOutputTag(tag)) {
        outputWindowedValue((WindowedValue<OutputT>) windowedValue);
        return;
      }
      fallback.outputTaggedWindowedValue(currentMaterializedElem(), tag, windowedValue);
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return fallback.pipelineOptions();
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return fallback.pipelineOptions();
    }

    @Override
    public InputT element() {
      return currentVal;
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      return currentVal;
    }

    @Override
    public Object schemaElement(int index) {
      return fallback.schemaElement(currentMaterializedElem(), index);
    }

    @Override
    public Instant timestamp() {
      return templateElem.getTimestamp();
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return templateElem.getTimestamp();
    }

    @Override
    public PaneInfo pane() {
      return templateElem.getPaneInfo();
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      return templateElem.getPaneInfo();
    }

    @Override
    public CausedByDrain causedByDrain() {
      return templateElem.causedByDrain();
    }

    @Override
    public CausedByDrain causedByDrain(DoFn<InputT, OutputT> doFn) {
      return templateElem.causedByDrain();
    }

    @Override
    public @Nullable String currentRecordId() {
      return templateElem.getRecordId();
    }

    @Override
    public @Nullable String currentRecordId(DoFn<InputT, OutputT> doFn) {
      return templateElem.getRecordId();
    }

    @Override
    public @Nullable Long currentRecordOffset() {
      return templateElem.getRecordOffset();
    }

    @Override
    public @Nullable Long currentRecordOffset(DoFn<InputT, OutputT> doFn) {
      return templateElem.getRecordOffset();
    }

    @Override
    public ValueKind valueKind() {
      return templateElem.getValueKind();
    }

    @Override
    public ValueKind valueKind(DoFn<InputT, OutputT> doFn) {
      return templateElem.getValueKind();
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
      throw new UnsupportedOperationException("Row output receiver is not supported in fast edge");
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      if (cachedMultiReceiver == null) {
        cachedMultiReceiver =
            new MultiOutputReceiver() {
              @Override
              public <T> DoFn.OutputReceiver<T> get(TupleTag<T> tag) {
                if (fallback.isMainOutputTag(tag)) {
                  return (DoFn.OutputReceiver<T>) FusedDoFnNodeBase.this;
                }
                throw new UnsupportedOperationException(
                    "Multi-output tags are not compiled into linear segments: " + tag);
              }

              @Override
              public <T> DoFn.OutputReceiver<Row> getRowReceiver(TupleTag<T> tag) {
                throw new UnsupportedOperationException("Row receiver not supported");
              }
            };
      }
      return cachedMultiReceiver;
    }

    @Override
    public BundleFinalizer bundleFinalizer() {
      return fallback.bundleFinalizer();
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException("Window not available in non-window-observing step");
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      throw new UnsupportedOperationException("Side inputs not supported in compiled segment");
    }

    @Override
    public Object sideInput(String tagId) {
      throw new UnsupportedOperationException("Side inputs not supported in compiled segment");
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("Not in StartBundle");
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("Not in FinishBundle");
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("Not in OnTimer");
    }

    @Override
    public DoFn<InputT, OutputT>.OnWindowExpirationContext onWindowExpirationContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("Not in OnWindowExpiration");
    }

    @Override
    public Object key() {
      throw new UnsupportedOperationException("Key not available");
    }

    @Override
    public Instant fireTimestamp(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("FireTimestamp not available");
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("TimeDomain not available");
    }

    @Override
    public String timerId(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("TimerId not available");
    }

    @Override
    public Object restriction() {
      throw new UnsupportedOperationException("Not an SDF");
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
      throw new UnsupportedOperationException("Not an SDF");
    }

    @Override
    public WatermarkEstimator<?> watermarkEstimator() {
      throw new UnsupportedOperationException("Not an SDF");
    }

    @Override
    public Object watermarkEstimatorState() {
      throw new UnsupportedOperationException("Not an SDF");
    }

    @Override
    public State state(String stateId, boolean alwaysFetched) {
      throw new UnsupportedOperationException("Stateful DoFn not supported in compiled segment");
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException("Timer DoFn not supported in compiled segment");
    }

    @Override
    public TimerMap timerFamily(String timerFamilyId) {
      throw new UnsupportedOperationException("TimerFamily DoFn not supported in compiled segment");
    }
  }

  // ---- Static runtime helpers invoked by generated bytecode ----

  public static MethodHandle consumePendingMethodHandle() {
    MethodHandle mh = PENDING_CLINIT_MH.get();
    if (mh == null) {
      throw new IllegalStateException("No pending MethodHandle for generated FusedDoFnNode");
    }
    return mh;
  }

  public static boolean hasNonRootOtel(WindowedValue<?> elem) {
    Context otel = elem.getOpenTelemetryContext();
    return otel != null && otel != ROOT_OTEL_CONTEXT;
  }

  public static boolean hasChangedOtel(@Nullable WindowedValue<?> elem) {
    if (elem == null) {
      throw new IllegalStateException(
          "Attempting to emit an element outside of a @ProcessElement context.");
    }
    Context cur = Context.current();
    return cur != ROOT_OTEL_CONTEXT && cur != elem.getOpenTelemetryContext();
  }

  public static boolean hasChangedOtelFast(WindowedValue<?> elem) {
    Context cur = Context.current();
    return cur != ROOT_OTEL_CONTEXT && cur != elem.getOpenTelemetryContext();
  }

  public static void invokeDoFnWithOtel(
      WindowedValue<?> elem, DoFnInvoker invoker, DoFnInvoker.ArgumentProvider argProvider) {
    try (Scope ignored = elem.getOpenTelemetryContext().makeCurrent()) {
      invoker.invokeProcessElement(argProvider);
    }
  }

  public static void runFuncSegmentWithOtel(
      FuncSegmentBody segment,
      @Nullable Object rawValue,
      WindowedValue<?> templateElem,
      int windowsSize)
      throws Exception {
    try (Scope ignored = templateElem.getOpenTelemetryContext().makeCurrent()) {
      segment.runBody(rawValue, templateElem, windowsSize);
    }
  }

  public static WindowedValue<?> buildChangedOtelOutput(
      WindowedValue<?> templateElem, @Nullable Object value, boolean allFilters) {
    return allFilters
        ? WindowedValues.builder((WindowedValue<Object>) templateElem).build()
        : WindowedValues.builder((WindowedValue<Object>) templateElem).withValue(value).build();
  }

  public static RuntimeException rethrowDownstreamOutputException(
      Throwable t, boolean wrapInUserCode) {
    if (t instanceof OutOfMemoryError) {
      throw (OutOfMemoryError) t;
    }
    if (wrapInUserCode) {
      throw UserCodeException.wrap(t);
    }
    if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    }
    if (t instanceof Error) {
      throw (Error) t;
    }
    throw new RuntimeException(t);
  }

  public static Exception onDoFnStepException(
      Throwable t,
      WindowedValue<?> elem,
      @Nullable Object stateToken,
      @Nullable StepHook hook,
      boolean isSystemDoFn)
      throws Exception {
    Throwable toPropagate = t;
    if (!(t instanceof Error)) {
      toPropagate = UserCodeException.wrapIf(!isSystemDoFn, t);
    }
    if (hook != null) {
      hook.afterStepException(toPropagate, elem, stateToken);
    }
    if (toPropagate instanceof Error) {
      throw (Error) toPropagate;
    }
    throw (Exception) toPropagate;
  }

  public static Exception onFuncStepException(
      Throwable t,
      WindowedValue<?> elem,
      @Nullable Object stateToken,
      @Nullable StepHook hook,
      boolean isSystemDoFn,
      boolean wrapErrorsAsUserCode)
      throws Exception {
    if (t instanceof OutOfMemoryError) {
      if (hook != null) {
        hook.exitFuncState(stateToken);
      }
      throw (OutOfMemoryError) t;
    }
    Throwable toPropagate;
    if (t instanceof Error && !wrapErrorsAsUserCode) {
      toPropagate = t;
    } else {
      toPropagate = UserCodeException.wrapIf(!isSystemDoFn, t);
    }
    if (hook != null) {
      hook.afterFuncException(toPropagate, elem, stateToken);
    }
    if (toPropagate instanceof Error) {
      throw (Error) toPropagate;
    }
    throw (Exception) toPropagate;
  }

  // ---- Chain Compiler ----

  @VisibleForTesting
  public static long generatedClassCount() {
    return GENERATED_CLASS_COUNTER.get();
  }

  /**
   * Compiles {@code steps} (ordered from upstream head {@code 0} to downstream {@code n - 1})
   * terminating at {@code tailReceiver} into a chain of specialized monomorphic ByteBuddy nodes.
   */
  public static CompiledNode compileChain(List<StepSpec> steps, TailReceiver tailReceiver) {
    return compileChain(steps, (CompiledNode) checkNotNull(tailReceiver, "tailReceiver"));
  }

  /**
   * Compiles {@code steps} (ordered from upstream head {@code 0} to downstream {@code n - 1})
   * terminating directly at {@code tailNode} without an intermediate tail adapter frame.
   */
  public static CompiledNode compileChain(List<StepSpec> steps, CompiledNode tailNode) {
    checkArgument(!steps.isEmpty(), "steps must not be empty");
    checkNotNull(tailNode, "tailNode must not be null");

    LoaderNamespace namespace = selectNamespace(steps, tailNode);
    CompiledNode currentNext = tailNode;

    int idx = steps.size() - 1;
    while (idx >= 0) {
      StepSpec spec = steps.get(idx);
      if (spec instanceof FunctionalStepSpec) {
        int endInclusive = idx;
        int startInclusive = idx;
        while (startInclusive > 0
            && (endInclusive - startInclusive + 1) < MAX_FUNCTIONS_PER_CHUNK
            && steps.get(startInclusive - 1) instanceof FunctionalStepSpec) {
          startInclusive--;
        }
        List<FunctionalStepSpec> chunk = new ArrayList<>(endInclusive - startInclusive + 1);
        for (int k = startInclusive; k <= endInclusive; k++) {
          chunk.add((FunctionalStepSpec) steps.get(k));
        }
        currentNext = instantiateFunctionalSegment(namespace, chunk, currentNext);
        idx = startInclusive - 1;
      } else if (spec instanceof DoFnStepSpec) {
        currentNext = instantiateDoFnNode(namespace, (DoFnStepSpec<?, ?>) spec, currentNext);
        idx--;
      } else {
        throw new IllegalArgumentException("Unsupported StepSpec: " + spec.getClass());
      }
    }
    return currentNext;
  }

  private static LoaderNamespace selectNamespace(List<StepSpec> steps, CompiledNode tailNode) {
    ClassLoader chosen = FusedSegmentCompiler.class.getClassLoader();
    for (StepSpec step : steps) {
      ClassLoader candidate = step.stepClassLoader();
      if (candidate != null && candidate != chosen && isAncestor(chosen, candidate)) {
        chosen = candidate;
      }
    }
    ClassLoader tailLoader = tailNode.getClass().getClassLoader();
    if (tailLoader != null && tailLoader != chosen && isAncestor(chosen, tailLoader)) {
      chosen = tailLoader;
    }
    if (chosen == null || chosen == FusedSegmentCompiler.class.getClassLoader()) {
      return BOOTSTRAP_NAMESPACE;
    }
    return LOADER_NAMESPACES.computeIfAbsent(chosen, LoaderNamespace::new);
  }

  private static Class<?> resolveNextNodeFieldClass(
      Class<?> nextNodeRuntimeClass, SegmentClassLoader loader) {
    if (isPubliclyAccessibleFrom(nextNodeRuntimeClass, loader)) {
      return nextNodeRuntimeClass;
    }
    if (TailReceiver.class.isAssignableFrom(nextNodeRuntimeClass)) {
      return TailReceiver.class;
    }
    return CompiledNode.class;
  }

  private static boolean isAncestor(@Nullable ClassLoader ancestor, ClassLoader descendant) {
    if (ancestor == null) {
      return true;
    }
    for (ClassLoader cur = descendant.getParent(); cur != null; cur = cur.getParent()) {
      if (cur == ancestor) {
        return true;
      }
    }
    return false;
  }

  static boolean isPubliclyAccessibleFrom(Class<?> target, ClassLoader loader) {
    if (target.getName().indexOf('/') >= 0 || !Modifier.isPublic(target.getModifiers())) {
      return false;
    }
    for (Class<?> enc = target.getEnclosingClass(); enc != null; enc = enc.getEnclosingClass()) {
      if (enc.getName().indexOf('/') >= 0 || !Modifier.isPublic(enc.getModifiers())) {
        return false;
      }
    }
    try {
      return Class.forName(target.getName(), false, loader) == target;
    } catch (Throwable t) {
      return false;
    }
  }

  private enum DirectParamKind {
    ELEMENT,
    OUTPUT_RECEIVER,
    PROCESS_CONTEXT,
    TIMESTAMP,
    PANE_INFO,
    PIPELINE_OPTIONS,
    TAGGED_OUTPUT_RECEIVER;

    Class<?> erasedPublicType() {
      switch (this) {
        case ELEMENT:
          return Object.class;
        case OUTPUT_RECEIVER:
          return DoFn.OutputReceiver.class;
        case PROCESS_CONTEXT:
          return DoFn.ProcessContext.class;
        case TIMESTAMP:
          return Instant.class;
        case PANE_INFO:
          return PaneInfo.class;
        case PIPELINE_OPTIONS:
          return PipelineOptions.class;
        case TAGGED_OUTPUT_RECEIVER:
          return MultiOutputReceiver.class;
        default:
          throw new AssertionError(this);
      }
    }
  }

  private static final class DirectProcessPlan {
    final boolean isDirectVirtual;
    final Class<?> fnFieldClass;
    final Method targetMethod;
    final DirectParamKind[] paramKinds;
    final Class<?>[] jvmParamTypes;
    final boolean needsCurrentVal;
    final @Nullable MethodHandle methodHandle;
    final @Nullable String mhErasedDescriptor;

    DirectProcessPlan(
        boolean isDirectVirtual,
        Class<?> fnFieldClass,
        Method targetMethod,
        DirectParamKind[] paramKinds,
        Class<?>[] jvmParamTypes,
        boolean needsCurrentVal,
        @Nullable MethodHandle methodHandle,
        @Nullable String mhErasedDescriptor) {
      this.isDirectVirtual = isDirectVirtual;
      this.fnFieldClass = fnFieldClass;
      this.targetMethod = targetMethod;
      this.paramKinds = paramKinds;
      this.jvmParamTypes = jvmParamTypes;
      this.needsCurrentVal = needsCurrentVal;
      this.methodHandle = methodHandle;
      this.mhErasedDescriptor = mhErasedDescriptor;
    }
  }

  private static @Nullable DirectProcessPlan resolveDirectProcessPlan(
      Class<?> fnClass, SegmentClassLoader segmentLoader) {
    try {
      DoFnSignature sig = DoFnSignatures.getSignature((Class) fnClass);
      DoFnSignature.ProcessElementMethod proc = sig.processElement();
      Method targetMethod = proc.targetMethod();
      if (targetMethod.getReturnType() != void.class
          || Modifier.isStatic(targetMethod.getModifiers())) {
        return null;
      }
      List<DoFnSignature.Parameter> extraParams = proc.extraParameters();
      Class<?>[] jvmParamTypes = targetMethod.getParameterTypes();
      if (jvmParamTypes.length != extraParams.size()) {
        return null;
      }
      DirectParamKind[] kinds = new DirectParamKind[extraParams.size()];
      boolean needsCurrentVal = false;
      boolean allElementTypesPublic = true;
      for (int i = 0; i < extraParams.size(); i++) {
        DoFnSignature.Parameter p = extraParams.get(i);
        Class<?> jvmParam = jvmParamTypes[i];
        if (jvmParam.isPrimitive()) {
          return null;
        }
        if (p instanceof DoFnSignature.Parameter.ElementParameter) {
          kinds[i] = DirectParamKind.ELEMENT;
          if (!isPubliclyAccessibleFrom(jvmParam, segmentLoader)) {
            allElementTypesPublic = false;
          }
        } else if (p instanceof DoFnSignature.Parameter.OutputReceiverParameter) {
          if (jvmParam != DoFn.OutputReceiver.class) {
            return null;
          }
          kinds[i] = DirectParamKind.OUTPUT_RECEIVER;
        } else if (p instanceof DoFnSignature.Parameter.ProcessContextParameter) {
          if (jvmParam != DoFn.ProcessContext.class) {
            return null;
          }
          kinds[i] = DirectParamKind.PROCESS_CONTEXT;
          needsCurrentVal = true;
        } else if (p instanceof DoFnSignature.Parameter.TimestampParameter) {
          if (jvmParam != Instant.class) {
            return null;
          }
          kinds[i] = DirectParamKind.TIMESTAMP;
        } else if (p instanceof DoFnSignature.Parameter.PaneInfoParameter) {
          if (jvmParam != PaneInfo.class) {
            return null;
          }
          kinds[i] = DirectParamKind.PANE_INFO;
        } else if (p instanceof DoFnSignature.Parameter.PipelineOptionsParameter) {
          if (jvmParam != PipelineOptions.class) {
            return null;
          }
          kinds[i] = DirectParamKind.PIPELINE_OPTIONS;
        } else if (p instanceof DoFnSignature.Parameter.TaggedOutputReceiverParameter) {
          if (jvmParam != MultiOutputReceiver.class) {
            return null;
          }
          kinds[i] = DirectParamKind.TAGGED_OUTPUT_RECEIVER;
        } else {
          return null;
        }
      }

      if (Modifier.isPublic(targetMethod.getModifiers())
          && isPubliclyAccessibleFrom(fnClass, segmentLoader)
          && isPubliclyAccessibleFrom(targetMethod.getDeclaringClass(), segmentLoader)
          && allElementTypesPublic) {
        return new DirectProcessPlan(
            true, fnClass, targetMethod, kinds, jvmParamTypes, needsCurrentVal, null, null);
      }

      MethodHandles.Lookup privateLookup =
          MethodHandles.privateLookupIn(targetMethod.getDeclaringClass(), MethodHandles.lookup());
      MethodHandle rawMh = privateLookup.unreflect(targetMethod);
      Class<?>[] erasedArgs = new Class<?>[1 + kinds.length];
      erasedArgs[0] = DoFn.class;
      for (int i = 0; i < kinds.length; i++) {
        erasedArgs[i + 1] = kinds[i].erasedPublicType();
      }
      MethodType erasedType = MethodType.methodType(void.class, erasedArgs);
      MethodHandle adapted = rawMh.asType(erasedType);
      return new DirectProcessPlan(
          false,
          DoFn.class,
          targetMethod,
          kinds,
          jvmParamTypes,
          needsCurrentVal,
          adapted,
          erasedType.toMethodDescriptorString());
    } catch (Throwable t) {
      return null;
    }
  }

  private static CompiledNode instantiateDoFnNode(
      LoaderNamespace ns, DoFnStepSpec<?, ?> spec, CompiledNode nextNode) {
    Class<?> fnClass = spec.fn.getClass();
    Class<?> invokerRuntimeClass = spec.invoker.getClass();
    Class<?> invokerFieldClass =
        isPubliclyAccessibleFrom(invokerRuntimeClass, ns.segmentLoader)
            ? invokerRuntimeClass
            : DoFnInvoker.class;
    Class<?> nextNodeRuntimeClass = nextNode.getClass();
    Class<?> nextNodeFieldClass = resolveNextNodeFieldClass(nextNodeRuntimeClass, ns.segmentLoader);
    boolean hasEntryHook = spec.entryHook != null;

    DoFnNodeCacheKey key =
        new DoFnNodeCacheKey(
            fnClass,
            invokerRuntimeClass,
            invokerFieldClass,
            nextNodeRuntimeClass,
            nextNodeFieldClass,
            hasEntryHook);
    try {
      Constructor<?> ctor =
          ns.constructorCache.get(
              key,
              () ->
                  generateDoFnNodeConstructor(
                      ns.segmentLoader,
                      fnClass,
                      invokerFieldClass,
                      nextNodeFieldClass,
                      hasEntryHook));
      return (CompiledNode)
          ctor.newInstance(
              spec.fn,
              spec.fallback,
              spec.invoker,
              spec.entryHook,
              spec.isSystemDoFn,
              spec.wrapDownstreamInUserCode,
              nextNode);
    } catch (ExecutionException | ReflectiveOperationException e) {
      throw new RuntimeException("Failed to instantiate compiled DoFn fusion node", e);
    }
  }

  private static CompiledNode instantiateFunctionalSegment(
      LoaderNamespace ns, List<FunctionalStepSpec> chunk, CompiledNode nextNode) {
    int k = chunk.size();
    InlineableFunctionProvider.Kind[] kinds = new InlineableFunctionProvider.Kind[k];
    Class<?>[] runtimeFnClasses = new Class<?>[k];
    Class<?>[] fieldFnClasses = new Class<?>[k];
    boolean[] hasHookFlags = new boolean[k];
    boolean[] isSystemFlags = new boolean[k];
    boolean[] wrapFlags = new boolean[k];
    ProcessFunction<?, ?>[] fns = new ProcessFunction<?, ?>[k];
    StepHook[] hooks = new StepHook[k];

    for (int i = 0; i < k; i++) {
      FunctionalStepSpec s = chunk.get(i);
      kinds[i] = s.kind;
      Class<?> fnCls = s.fn.getClass();
      runtimeFnClasses[i] = fnCls;
      fieldFnClasses[i] =
          isPubliclyAccessibleFrom(fnCls, ns.segmentLoader) ? fnCls : ProcessFunction.class;
      hasHookFlags[i] = s.entryHook != null;
      isSystemFlags[i] = s.isSystemDoFn;
      wrapFlags[i] = s.wrapExceptionsAsUserCode;
      fns[i] = s.fn;
      hooks[i] = s.entryHook;
    }

    Class<?> nextNodeRuntimeClass = nextNode.getClass();
    Class<?> nextNodeFieldClass = resolveNextNodeFieldClass(nextNodeRuntimeClass, ns.segmentLoader);
    FuncSegmentCacheKey key =
        new FuncSegmentCacheKey(
            kinds,
            runtimeFnClasses,
            fieldFnClasses,
            hasHookFlags,
            isSystemFlags,
            wrapFlags,
            nextNodeRuntimeClass,
            nextNodeFieldClass);
    try {
      Constructor<?> ctor =
          ns.constructorCache.get(
              key,
              () ->
                  generateFunctionalSegmentConstructor(
                      ns.segmentLoader,
                      kinds,
                      fieldFnClasses,
                      hasHookFlags,
                      isSystemFlags,
                      wrapFlags,
                      nextNodeFieldClass));
      return (CompiledNode) ctor.newInstance(fns, hooks, nextNode);
    } catch (ExecutionException | ReflectiveOperationException e) {
      throw new RuntimeException("Failed to instantiate compiled functional fusion segment", e);
    }
  }

  // ---- ByteBuddy Class Generators ----

  private static Constructor<?> generateDoFnNodeConstructor(
      SegmentClassLoader loader,
      Class<?> fnClass,
      Class<?> invokerFieldClass,
      Class<?> nextNodeClass,
      boolean hasEntryHook)
      throws NoSuchMethodException, ClassNotFoundException {
    long id = GENERATED_CLASS_COUNTER.incrementAndGet();
    String className = "org.apache.beam.sdk.transforms.reflect.generated.FusedDoFnNode$" + id;
    DirectProcessPlan directPlan = resolveDirectProcessPlan(fnClass, loader);
    Class<?> fnFieldClass = (directPlan != null) ? directPlan.fnFieldClass : DoFn.class;

    DynamicType.Builder<?> builder =
        new ByteBuddy()
            .subclass(FusedDoFnNodeBase.class, ConstructorStrategy.Default.NO_CONSTRUCTORS)
            .name(className)
            .modifiers(Visibility.PUBLIC, TypeManifestation.FINAL)
            .defineField("fn", fnFieldClass, Visibility.PRIVATE, FieldManifestation.FINAL)
            .defineField("invoker", invokerFieldClass, Visibility.PRIVATE, FieldManifestation.FINAL)
            .defineField(
                "isSystemDoFn", boolean.class, Visibility.PRIVATE, FieldManifestation.FINAL)
            .defineField(
                "wrapDownstreamInUserCode",
                boolean.class,
                Visibility.PRIVATE,
                FieldManifestation.FINAL)
            .defineField("nextNode", nextNodeClass, Visibility.PRIVATE, FieldManifestation.FINAL);

    if (hasEntryHook) {
      builder =
          builder.defineField(
              "entryHook", StepHook.class, Visibility.PRIVATE, FieldManifestation.FINAL);
    }

    if (directPlan != null && !directPlan.isDirectVirtual) {
      builder =
          builder
              .defineField(
                  "MH_PROCESS",
                  MethodHandle.class,
                  Visibility.PRIVATE,
                  Ownership.STATIC,
                  FieldManifestation.FINAL)
              .invokable(ElementMatchers.isTypeInitializer())
              .intercept(
                  new SimpleByteCodeImplementation(new DoFnClinitMethodHandleAppender(className)));
    }

    DynamicType.Unloaded<?> unloaded =
        builder
            .defineConstructor(Visibility.PUBLIC)
            .withParameters(
                DoFn.class,
                StepContextFallback.class,
                invokerFieldClass,
                StepHook.class,
                boolean.class,
                boolean.class,
                nextNodeClass)
            .intercept(
                new SimpleByteCodeImplementation(
                    new DoFnNodeConstructorAppender(
                        className, fnFieldClass, invokerFieldClass, nextNodeClass, hasEntryHook)))
            .method(ElementMatchers.named("emitWindowed"))
            .intercept(
                new SimpleByteCodeImplementation(
                    new DoFnEmitWindowedAppender(className, nextNodeClass)))
            .defineMethod("output", void.class, Visibility.PUBLIC, Ownership.MEMBER)
            .withParameters(Object.class)
            .intercept(
                new SimpleByteCodeImplementation(new DoFnOutputAppender(className, nextNodeClass)))
            .method(ElementMatchers.named("acceptRaw"))
            .intercept(
                new SimpleByteCodeImplementation(
                    new DoFnAcceptRawAppender(
                        className, invokerFieldClass, directPlan, hasEntryHook)))
            .visit(
                new AsmVisitorWrapper.ForDeclaredMethods()
                    .writerFlags(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS))
            .make();

    Class<?> generated = loader.defineGeneratedClass(className, unloaded.getBytes());
    if (directPlan != null && !directPlan.isDirectVirtual) {
      PENDING_CLINIT_MH.set(directPlan.methodHandle);
      try {
        Class.forName(className, true, loader);
      } finally {
        PENDING_CLINIT_MH.remove();
      }
    }
    return generated.getConstructor(
        DoFn.class,
        StepContextFallback.class,
        invokerFieldClass,
        StepHook.class,
        boolean.class,
        boolean.class,
        nextNodeClass);
  }

  private static Constructor<?> generateFunctionalSegmentConstructor(
      SegmentClassLoader loader,
      InlineableFunctionProvider.Kind[] kinds,
      Class<?>[] fieldFnClasses,
      boolean[] hasHookFlags,
      boolean[] isSystemFlags,
      boolean[] wrapFlags,
      Class<?> nextNodeClass)
      throws NoSuchMethodException {
    long id = GENERATED_CLASS_COUNTER.incrementAndGet();
    String className = "org.apache.beam.sdk.transforms.reflect.generated.FusedFuncSegment$" + id;

    DynamicType.Builder<?> builder =
        new ByteBuddy()
            .subclass(Object.class, ConstructorStrategy.Default.NO_CONSTRUCTORS)
            .name(className)
            .modifiers(Visibility.PUBLIC, TypeManifestation.FINAL)
            .implement(FuncSegmentBody.class)
            .defineField("nextNode", nextNodeClass, Visibility.PRIVATE, FieldManifestation.FINAL);

    for (int i = 0; i < kinds.length; i++) {
      builder =
          builder.defineField(
              "fn_" + i, fieldFnClasses[i], Visibility.PRIVATE, FieldManifestation.FINAL);
      if (hasHookFlags[i]) {
        builder =
            builder.defineField(
                "hook_" + i, StepHook.class, Visibility.PRIVATE, FieldManifestation.FINAL);
      }
    }

    DynamicType.Unloaded<?> unloaded =
        builder
            .defineConstructor(Visibility.PUBLIC)
            .withParameters(ProcessFunction[].class, StepHook[].class, nextNodeClass)
            .intercept(
                new SimpleByteCodeImplementation(
                    new FuncSegmentConstructorAppender(
                        className, fieldFnClasses, hasHookFlags, nextNodeClass)))
            .method(ElementMatchers.named("acceptWindowed"))
            .intercept(
                new SimpleByteCodeImplementation(new FuncSegmentAcceptWindowedAppender(className)))
            .method(ElementMatchers.named("acceptRaw"))
            .intercept(
                new SimpleByteCodeImplementation(
                    new FuncSegmentBodyAppender(
                        true,
                        className,
                        kinds,
                        fieldFnClasses,
                        hasHookFlags,
                        isSystemFlags,
                        wrapFlags,
                        nextNodeClass)))
            .method(ElementMatchers.named("runBody"))
            .intercept(
                new SimpleByteCodeImplementation(
                    new FuncSegmentBodyAppender(
                        false,
                        className,
                        kinds,
                        fieldFnClasses,
                        hasHookFlags,
                        isSystemFlags,
                        wrapFlags,
                        nextNodeClass)))
            .visit(
                new AsmVisitorWrapper.ForDeclaredMethods()
                    .writerFlags(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS))
            .make();

    Class<?> generated = loader.defineGeneratedClass(className, unloaded.getBytes());
    return generated.getConstructor(ProcessFunction[].class, StepHook[].class, nextNodeClass);
  }

  // ---- ASM ByteCodeAppenders ----

  private static final String INTERNAL_COMPILER = Type.getInternalName(FusedSegmentCompiler.class);
  private static final String INTERNAL_DOFN_BASE = Type.getInternalName(FusedDoFnNodeBase.class);
  private static final String INTERNAL_FUNC_BODY = Type.getInternalName(FuncSegmentBody.class);
  private static final String INTERNAL_STEP_HOOK = Type.getInternalName(StepHook.class);
  private static final String INTERNAL_WINDOWED_VALUE = Type.getInternalName(WindowedValue.class);
  private static final String INTERNAL_DOFN_INVOKER = Type.getInternalName(DoFnInvoker.class);
  private static final String INTERNAL_ARG_PROVIDER =
      Type.getInternalName(DoFnInvoker.ArgumentProvider.class);
  private static final String DESC_PROCESS_CONTINUATION =
      Type.getDescriptor(DoFn.ProcessContinuation.class);

  private static final String DESC_OBJECT = Type.getDescriptor(Object.class);
  private static final String DESC_DOFN = Type.getDescriptor(DoFn.class);
  private static final String DESC_FALLBACK = Type.getDescriptor(StepContextFallback.class);
  private static final String DESC_STEP_HOOK = Type.getDescriptor(StepHook.class);
  private static final String DESC_WINDOWED_VALUE = Type.getDescriptor(WindowedValue.class);
  private static final String DESC_THROWABLE = Type.getDescriptor(Throwable.class);
  private static final String DESC_EXCEPTION = Type.getDescriptor(Exception.class);
  private static final String DESC_RUNTIME_EX = Type.getDescriptor(RuntimeException.class);

  private static final class SimpleByteCodeImplementation implements Implementation {
    private final ByteCodeAppender appender;

    SimpleByteCodeImplementation(ByteCodeAppender appender) {
      this.appender = appender;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(Target implementationTarget) {
      return appender;
    }
  }

  private static final class DoFnClinitMethodHandleAppender implements ByteCodeAppender {
    private final String internalClassName;

    DoFnClinitMethodHandleAppender(String className) {
      this.internalClassName = className.replace('.', '/');
    }

    @Override
    public Size apply(
        MethodVisitor mv, Implementation.Context ctx, MethodDescription instrumentedMethod) {
      mv.visitMethodInsn(
          Opcodes.INVOKESTATIC,
          INTERNAL_COMPILER,
          "consumePendingMethodHandle",
          "()Ljava/lang/invoke/MethodHandle;",
          false);
      mv.visitFieldInsn(
          Opcodes.PUTSTATIC, internalClassName, "MH_PROCESS", "Ljava/lang/invoke/MethodHandle;");
      mv.visitInsn(Opcodes.RETURN);
      return new Size(1, 0);
    }
  }

  private static final class DoFnNodeConstructorAppender implements ByteCodeAppender {
    private final String internalClassName;
    private final Class<?> fnFieldClass;
    private final Class<?> invokerFieldClass;
    private final Class<?> nextNodeClass;
    private final boolean hasEntryHook;

    DoFnNodeConstructorAppender(
        String className,
        Class<?> fnFieldClass,
        Class<?> invokerFieldClass,
        Class<?> nextNodeClass,
        boolean hasEntryHook) {
      this.internalClassName = className.replace('.', '/');
      this.fnFieldClass = fnFieldClass;
      this.invokerFieldClass = invokerFieldClass;
      this.nextNodeClass = nextNodeClass;
      this.hasEntryHook = hasEntryHook;
    }

    @Override
    public Size apply(
        MethodVisitor mv, Implementation.Context ctx, MethodDescription instrumentedMethod) {
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ALOAD, 1);
      mv.visitVarInsn(Opcodes.ALOAD, 2);
      mv.visitMethodInsn(
          Opcodes.INVOKESPECIAL,
          INTERNAL_DOFN_BASE,
          "<init>",
          "(" + DESC_DOFN + DESC_FALLBACK + ")V",
          false);

      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ALOAD, 1);
      if (fnFieldClass != DoFn.class) {
        mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(fnFieldClass));
      }
      mv.visitFieldInsn(
          Opcodes.PUTFIELD, internalClassName, "fn", Type.getDescriptor(fnFieldClass));

      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ALOAD, 3);
      mv.visitFieldInsn(
          Opcodes.PUTFIELD, internalClassName, "invoker", Type.getDescriptor(invokerFieldClass));

      if (hasEntryHook) {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitVarInsn(Opcodes.ALOAD, 4);
        mv.visitFieldInsn(Opcodes.PUTFIELD, internalClassName, "entryHook", DESC_STEP_HOOK);
      }

      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ILOAD, 5);
      mv.visitFieldInsn(Opcodes.PUTFIELD, internalClassName, "isSystemDoFn", "Z");

      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ILOAD, 6);
      mv.visitFieldInsn(Opcodes.PUTFIELD, internalClassName, "wrapDownstreamInUserCode", "Z");

      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ALOAD, 7);
      mv.visitFieldInsn(
          Opcodes.PUTFIELD, internalClassName, "nextNode", Type.getDescriptor(nextNodeClass));

      mv.visitInsn(Opcodes.RETURN);
      return new Size(3, 8);
    }
  }

  private static final class DoFnEmitWindowedAppender implements ByteCodeAppender {
    private final String internalClassName;
    private final Class<?> nextNodeClass;

    DoFnEmitWindowedAppender(String className, Class<?> nextNodeClass) {
      this.internalClassName = className.replace('.', '/');
      this.nextNodeClass = nextNodeClass;
    }

    @Override
    public Size apply(
        MethodVisitor mv, Implementation.Context ctx, MethodDescription instrumentedMethod) {
      boolean nextIsInterface = nextNodeClass.isInterface();
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitFieldInsn(
          Opcodes.GETFIELD, internalClassName, "nextNode", Type.getDescriptor(nextNodeClass));
      mv.visitVarInsn(Opcodes.ALOAD, 1);
      mv.visitMethodInsn(
          nextIsInterface ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL,
          Type.getInternalName(nextNodeClass),
          "acceptWindowed",
          "(" + DESC_WINDOWED_VALUE + ")V",
          nextIsInterface);
      mv.visitInsn(Opcodes.RETURN);
      return new Size(2, 2);
    }
  }

  private static final class DoFnOutputAppender implements ByteCodeAppender {
    private final String internalClassName;
    private final Class<?> nextNodeClass;

    DoFnOutputAppender(String className, Class<?> nextNodeClass) {
      this.internalClassName = className.replace('.', '/');
      this.nextNodeClass = nextNodeClass;
    }

    @Override
    public Size apply(
        MethodVisitor mv, Implementation.Context ctx, MethodDescription instrumentedMethod) {
      boolean nextIsInterface = nextNodeClass.isInterface();
      Label fastLabel = new Label();
      Label tryStart = new Label();
      Label tryEnd = new Label();
      Label catchHandler = new Label();
      mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, "java/lang/Throwable");

      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitFieldInsn(Opcodes.GETFIELD, INTERNAL_DOFN_BASE, "templateElem", DESC_WINDOWED_VALUE);
      mv.visitInsn(Opcodes.DUP);
      mv.visitVarInsn(Opcodes.ASTORE, 2);
      mv.visitMethodInsn(
          Opcodes.INVOKESTATIC,
          INTERNAL_COMPILER,
          "hasChangedOtelFast",
          "(" + DESC_WINDOWED_VALUE + ")Z",
          false);
      mv.visitJumpInsn(Opcodes.IFEQ, fastLabel);

      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ALOAD, 1);
      mv.visitMethodInsn(
          Opcodes.INVOKEVIRTUAL,
          INTERNAL_DOFN_BASE,
          "outputSlowOtel",
          "(" + DESC_OBJECT + ")V",
          false);
      mv.visitInsn(Opcodes.RETURN);

      mv.visitLabel(fastLabel);
      mv.visitLabel(tryStart);
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitFieldInsn(
          Opcodes.GETFIELD, internalClassName, "nextNode", Type.getDescriptor(nextNodeClass));
      mv.visitVarInsn(Opcodes.ALOAD, 1);
      mv.visitVarInsn(Opcodes.ALOAD, 2);
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitFieldInsn(Opcodes.GETFIELD, INTERNAL_DOFN_BASE, "currentWinSize", "I");
      mv.visitMethodInsn(
          nextIsInterface ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL,
          Type.getInternalName(nextNodeClass),
          "acceptRaw",
          "(" + DESC_OBJECT + DESC_WINDOWED_VALUE + "I)V",
          nextIsInterface);
      mv.visitLabel(tryEnd);
      mv.visitInsn(Opcodes.RETURN);

      mv.visitLabel(catchHandler);
      mv.visitVarInsn(Opcodes.ASTORE, 3);
      mv.visitVarInsn(Opcodes.ALOAD, 3);
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitFieldInsn(Opcodes.GETFIELD, internalClassName, "wrapDownstreamInUserCode", "Z");
      mv.visitMethodInsn(
          Opcodes.INVOKESTATIC,
          INTERNAL_COMPILER,
          "rethrowDownstreamOutputException",
          "(" + DESC_THROWABLE + "Z)" + DESC_RUNTIME_EX,
          false);
      mv.visitInsn(Opcodes.ATHROW);

      return new Size(4, 4);
    }
  }

  private static final class DoFnAcceptRawAppender implements ByteCodeAppender {
    private final String internalClassName;
    private final Class<?> invokerFieldClass;
    private final @Nullable DirectProcessPlan directPlan;
    private final boolean hasEntryHook;

    DoFnAcceptRawAppender(
        String className,
        Class<?> invokerFieldClass,
        @Nullable DirectProcessPlan directPlan,
        boolean hasEntryHook) {
      this.internalClassName = className.replace('.', '/');
      this.invokerFieldClass = invokerFieldClass;
      this.directPlan = directPlan;
      this.hasEntryHook = hasEntryHook;
    }

    private void emitDirectArgument(MethodVisitor mv, int idx, boolean castExactElement) {
      DirectParamKind kind = directPlan.paramKinds[idx];
      switch (kind) {
        case ELEMENT:
          mv.visitVarInsn(Opcodes.ALOAD, 1);
          if (castExactElement && directPlan.jvmParamTypes[idx] != Object.class) {
            mv.visitTypeInsn(
                Opcodes.CHECKCAST, Type.getInternalName(directPlan.jvmParamTypes[idx]));
          }
          break;
        case OUTPUT_RECEIVER:
        case PROCESS_CONTEXT:
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          break;
        case TIMESTAMP:
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitMethodInsn(
              Opcodes.INVOKEINTERFACE,
              INTERNAL_WINDOWED_VALUE,
              "getTimestamp",
              "()Lorg/joda/time/Instant;",
              true);
          break;
        case PANE_INFO:
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitMethodInsn(
              Opcodes.INVOKEINTERFACE,
              INTERNAL_WINDOWED_VALUE,
              "getPaneInfo",
              "()Lorg/apache/beam/sdk/transforms/windowing/PaneInfo;",
              true);
          break;
        case PIPELINE_OPTIONS:
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL,
              INTERNAL_DOFN_BASE,
              "pipelineOptions",
              "()Lorg/apache/beam/sdk/options/PipelineOptions;",
              false);
          break;
        case TAGGED_OUTPUT_RECEIVER:
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitInsn(Opcodes.ACONST_NULL);
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL,
              INTERNAL_DOFN_BASE,
              "taggedOutputReceiver",
              "(" + DESC_DOFN + ")Lorg/apache/beam/sdk/transforms/DoFn$MultiOutputReceiver;",
              false);
          break;
        default:
          throw new AssertionError(kind);
      }
    }

    @Override
    public Size apply(
        MethodVisitor mv, Implementation.Context ctx, MethodDescription instrumentedMethod) {
      Label afterTmplUpdate = new Label();
      Label fastInvoke = new Label();
      Label tryStart = new Label();
      Label tryEnd = new Label();
      Label catchHandler = new Label();

      mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, "java/lang/Throwable");

      if (hasEntryHook) {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, internalClassName, "entryHook", DESC_STEP_HOOK);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitVarInsn(Opcodes.ILOAD, 3);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL,
            INTERNAL_STEP_HOOK,
            "beforeFunc",
            "(" + DESC_OBJECT + DESC_WINDOWED_VALUE + "I)" + DESC_OBJECT,
            false);
        mv.visitVarInsn(Opcodes.ASTORE, 4);
      }

      // Only execute reference PUTFIELD (and its G1GC barrier) when templateElem changes
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitFieldInsn(Opcodes.GETFIELD, INTERNAL_DOFN_BASE, "templateElem", DESC_WINDOWED_VALUE);
      mv.visitVarInsn(Opcodes.ALOAD, 2);
      mv.visitJumpInsn(Opcodes.IF_ACMPEQ, afterTmplUpdate);

      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ALOAD, 2);
      mv.visitFieldInsn(Opcodes.PUTFIELD, INTERNAL_DOFN_BASE, "templateElem", DESC_WINDOWED_VALUE);
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ILOAD, 3);
      mv.visitFieldInsn(Opcodes.PUTFIELD, INTERNAL_DOFN_BASE, "currentWinSize", "I");

      mv.visitLabel(afterTmplUpdate);
      boolean needsCurrentVal = directPlan == null || directPlan.needsCurrentVal;
      if (needsCurrentVal) {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitFieldInsn(Opcodes.PUTFIELD, INTERNAL_DOFN_BASE, "currentVal", DESC_OBJECT);
      }

      mv.visitLabel(tryStart);
      mv.visitVarInsn(Opcodes.ALOAD, 2);
      mv.visitMethodInsn(
          Opcodes.INVOKESTATIC,
          INTERNAL_COMPILER,
          "hasNonRootOtel",
          "(" + DESC_WINDOWED_VALUE + ")Z",
          false);
      mv.visitJumpInsn(Opcodes.IFEQ, fastInvoke);

      if (!needsCurrentVal) {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitFieldInsn(Opcodes.PUTFIELD, INTERNAL_DOFN_BASE, "currentVal", DESC_OBJECT);
      }
      mv.visitVarInsn(Opcodes.ALOAD, 2);
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitFieldInsn(
          Opcodes.GETFIELD, internalClassName, "invoker", Type.getDescriptor(invokerFieldClass));
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitMethodInsn(
          Opcodes.INVOKESTATIC,
          INTERNAL_COMPILER,
          "invokeDoFnWithOtel",
          "("
              + DESC_WINDOWED_VALUE
              + "L"
              + INTERNAL_DOFN_INVOKER
              + ";L"
              + INTERNAL_ARG_PROVIDER
              + ";)V",
          false);
      mv.visitJumpInsn(Opcodes.GOTO, tryEnd);

      mv.visitLabel(fastInvoke);
      if (directPlan != null && directPlan.isDirectVirtual) {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(
            Opcodes.GETFIELD, internalClassName, "fn", Type.getDescriptor(directPlan.fnFieldClass));
        for (int i = 0; i < directPlan.paramKinds.length; i++) {
          emitDirectArgument(mv, i, true);
        }
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL,
            Type.getInternalName(directPlan.fnFieldClass),
            directPlan.targetMethod.getName(),
            Type.getMethodDescriptor(directPlan.targetMethod),
            false);
      } else if (directPlan != null) {
        mv.visitFieldInsn(
            Opcodes.GETSTATIC, internalClassName, "MH_PROCESS", "Ljava/lang/invoke/MethodHandle;");
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, internalClassName, "fn", DESC_DOFN);
        for (int i = 0; i < directPlan.paramKinds.length; i++) {
          emitDirectArgument(mv, i, false);
        }
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL,
            "java/lang/invoke/MethodHandle",
            "invokeExact",
            directPlan.mhErasedDescriptor,
            false);
      } else {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(
            Opcodes.GETFIELD, internalClassName, "invoker", Type.getDescriptor(invokerFieldClass));
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        boolean invokerIsInterface = invokerFieldClass.isInterface();
        mv.visitMethodInsn(
            invokerIsInterface ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL,
            Type.getInternalName(invokerFieldClass),
            "invokeProcessElement",
            "(L" + INTERNAL_ARG_PROVIDER + ";)" + DESC_PROCESS_CONTINUATION,
            invokerIsInterface);
        mv.visitInsn(Opcodes.POP);
      }

      mv.visitLabel(tryEnd);
      if (hasEntryHook) {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, internalClassName, "entryHook", DESC_STEP_HOOK);
        mv.visitVarInsn(Opcodes.ALOAD, 4);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL,
            INTERNAL_STEP_HOOK,
            "afterRawNormal",
            "(" + DESC_OBJECT + ")V",
            false);
      }
      mv.visitInsn(Opcodes.RETURN);

      mv.visitLabel(catchHandler);
      mv.visitVarInsn(Opcodes.ASTORE, 5);
      mv.visitVarInsn(Opcodes.ALOAD, 5);
      mv.visitVarInsn(Opcodes.ALOAD, 2);
      if (hasEntryHook) {
        mv.visitVarInsn(Opcodes.ALOAD, 4);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, internalClassName, "entryHook", DESC_STEP_HOOK);
      } else {
        mv.visitInsn(Opcodes.ACONST_NULL);
        mv.visitInsn(Opcodes.ACONST_NULL);
      }
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitFieldInsn(Opcodes.GETFIELD, internalClassName, "isSystemDoFn", "Z");
      mv.visitInsn(directPlan != null ? Opcodes.ICONST_1 : Opcodes.ICONST_0);
      mv.visitMethodInsn(
          Opcodes.INVOKESTATIC,
          INTERNAL_COMPILER,
          "onFuncStepException",
          "("
              + DESC_THROWABLE
              + DESC_WINDOWED_VALUE
              + DESC_OBJECT
              + DESC_STEP_HOOK
              + "ZZ)"
              + DESC_EXCEPTION,
          false);
      mv.visitInsn(Opcodes.ATHROW);

      return new Size(8, 6);
    }
  }

  private static final class FuncSegmentConstructorAppender implements ByteCodeAppender {
    private final String internalClassName;
    private final Class<?>[] fieldFnClasses;
    private final boolean[] hasHookFlags;
    private final Class<?> nextNodeClass;

    FuncSegmentConstructorAppender(
        String className,
        Class<?>[] fieldFnClasses,
        boolean[] hasHookFlags,
        Class<?> nextNodeClass) {
      this.internalClassName = className.replace('.', '/');
      this.fieldFnClasses = fieldFnClasses;
      this.hasHookFlags = hasHookFlags;
      this.nextNodeClass = nextNodeClass;
    }

    @Override
    public Size apply(
        MethodVisitor mv, Implementation.Context ctx, MethodDescription instrumentedMethod) {
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);

      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ALOAD, 3);
      mv.visitFieldInsn(
          Opcodes.PUTFIELD, internalClassName, "nextNode", Type.getDescriptor(nextNodeClass));

      for (int i = 0; i < fieldFnClasses.length; i++) {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        pushInt(mv, i);
        mv.visitInsn(Opcodes.AALOAD);
        if (fieldFnClasses[i] != ProcessFunction.class) {
          mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(fieldFnClasses[i]));
        }
        mv.visitFieldInsn(
            Opcodes.PUTFIELD, internalClassName, "fn_" + i, Type.getDescriptor(fieldFnClasses[i]));

        if (hasHookFlags[i]) {
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          pushInt(mv, i);
          mv.visitInsn(Opcodes.AALOAD);
          mv.visitFieldInsn(Opcodes.PUTFIELD, internalClassName, "hook_" + i, DESC_STEP_HOOK);
        }
      }

      mv.visitInsn(Opcodes.RETURN);
      return new Size(4, 4);
    }
  }

  private static final class FuncSegmentAcceptWindowedAppender implements ByteCodeAppender {
    private final String internalClassName;

    FuncSegmentAcceptWindowedAppender(String className) {
      this.internalClassName = className.replace('.', '/');
    }

    @Override
    public Size apply(
        MethodVisitor mv, Implementation.Context ctx, MethodDescription instrumentedMethod) {
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ALOAD, 1);
      mv.visitMethodInsn(
          Opcodes.INVOKEINTERFACE, INTERNAL_WINDOWED_VALUE, "getValue", "()" + DESC_OBJECT, true);
      mv.visitVarInsn(Opcodes.ALOAD, 1);
      mv.visitVarInsn(Opcodes.ALOAD, 1);
      mv.visitMethodInsn(
          Opcodes.INVOKEINTERFACE,
          INTERNAL_WINDOWED_VALUE,
          "getWindows",
          "()Ljava/util/Collection;",
          true);
      mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/Collection", "size", "()I", true);
      mv.visitMethodInsn(
          Opcodes.INVOKEVIRTUAL,
          internalClassName,
          "acceptRaw",
          "(" + DESC_OBJECT + DESC_WINDOWED_VALUE + "I)V",
          false);
      mv.visitInsn(Opcodes.RETURN);
      return new Size(4, 2);
    }
  }

  private static final class FuncSegmentBodyAppender implements ByteCodeAppender {
    private final boolean includeOtelEntryCheck;
    private final String internalClassName;
    private final InlineableFunctionProvider.Kind[] kinds;
    private final Class<?>[] fieldFnClasses;
    private final boolean[] hasHookFlags;
    private final boolean[] isSystemFlags;
    private final boolean[] wrapFlags;
    private final Class<?> nextNodeClass;

    FuncSegmentBodyAppender(
        boolean includeOtelEntryCheck,
        String className,
        InlineableFunctionProvider.Kind[] kinds,
        Class<?>[] fieldFnClasses,
        boolean[] hasHookFlags,
        boolean[] isSystemFlags,
        boolean[] wrapFlags,
        Class<?> nextNodeClass) {
      this.includeOtelEntryCheck = includeOtelEntryCheck;
      this.internalClassName = className.replace('.', '/');
      this.kinds = kinds;
      this.fieldFnClasses = fieldFnClasses;
      this.hasHookFlags = hasHookFlags;
      this.isSystemFlags = isSystemFlags;
      this.wrapFlags = wrapFlags;
      this.nextNodeClass = nextNodeClass;
    }

    private void emitFinishHooksUpTo(MethodVisitor mv, int maxInclusive) {
      for (int j = maxInclusive; j >= 0; j--) {
        if (hasHookFlags[j]) {
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitFieldInsn(Opcodes.GETFIELD, internalClassName, "hook_" + j, DESC_STEP_HOOK);
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL, INTERNAL_STEP_HOOK, "finishFuncLazy", "()V", false);
        }
      }
    }

    @Override
    public Size apply(
        MethodVisitor mv, Implementation.Context ctx, MethodDescription instrumentedMethod) {
      // Locals: 0=this, 1=v (rawValue), 2=templateElem, 3=winSize, 4=hook, 5=tok, 6=t, 7=keep
      int k = kinds.length;
      boolean nextIsInterface = nextNodeClass.isInterface();
      Label[] tryStarts = new Label[k];
      Label[] tryEnds = new Label[k];
      Label[] catchHandlers = new Label[k];
      boolean allFilters = true;
      for (int i = 0; i < k; i++) {
        tryStarts[i] = new Label();
        tryEnds[i] = new Label();
        catchHandlers[i] = new Label();
        mv.visitTryCatchBlock(tryStarts[i], tryEnds[i], catchHandlers[i], "java/lang/Throwable");
        if (kinds[i] != InlineableFunctionProvider.Kind.FILTER) {
          allFilters = false;
        }
      }

      if (includeOtelEntryCheck) {
        Label fastLabel = new Label();
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            INTERNAL_COMPILER,
            "hasNonRootOtel",
            "(" + DESC_WINDOWED_VALUE + ")Z",
            false);
        mv.visitJumpInsn(Opcodes.IFEQ, fastLabel);

        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitVarInsn(Opcodes.ILOAD, 3);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            INTERNAL_COMPILER,
            "runFuncSegmentWithOtel",
            "(L" + INTERNAL_FUNC_BODY + ";" + DESC_OBJECT + DESC_WINDOWED_VALUE + "I)V",
            false);
        mv.visitInsn(Opcodes.RETURN);

        mv.visitLabel(fastLabel);
      }

      for (int i = 0; i < k; i++) {
        Label nextStep = new Label();

        if (hasHookFlags[i]) {
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitFieldInsn(Opcodes.GETFIELD, internalClassName, "hook_" + i, DESC_STEP_HOOK);
          mv.visitInsn(Opcodes.DUP);
          mv.visitVarInsn(Opcodes.ASTORE, 4);
          mv.visitVarInsn(Opcodes.ALOAD, 1);
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitVarInsn(Opcodes.ILOAD, 3);
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL,
              INTERNAL_STEP_HOOK,
              "beforeFunc",
              "(" + DESC_OBJECT + DESC_WINDOWED_VALUE + "I)" + DESC_OBJECT,
              false);
          mv.visitVarInsn(Opcodes.ASTORE, 5);
        }

        mv.visitLabel(tryStarts[i]);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(
            Opcodes.GETFIELD, internalClassName, "fn_" + i, Type.getDescriptor(fieldFnClasses[i]));
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        boolean fnIsInterface = fieldFnClasses[i].isInterface();
        mv.visitMethodInsn(
            fnIsInterface ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL,
            Type.getInternalName(fieldFnClasses[i]),
            "apply",
            "(" + DESC_OBJECT + ")" + DESC_OBJECT,
            fnIsInterface);
        mv.visitLabel(tryEnds[i]);

        if (kinds[i] == InlineableFunctionProvider.Kind.MAP) {
          mv.visitVarInsn(Opcodes.ASTORE, 1);
          if (hasHookFlags[i]) {
            mv.visitVarInsn(Opcodes.ALOAD, 4);
            mv.visitVarInsn(Opcodes.ALOAD, 5);
            mv.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                INTERNAL_STEP_HOOK,
                "exitFuncState",
                "(" + DESC_OBJECT + ")V",
                false);
          }
        } else {
          mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Boolean");
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue", "()Z", false);
          mv.visitVarInsn(Opcodes.ISTORE, 7);

          if (hasHookFlags[i]) {
            mv.visitVarInsn(Opcodes.ALOAD, 4);
            mv.visitVarInsn(Opcodes.ALOAD, 5);
            mv.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                INTERNAL_STEP_HOOK,
                "exitFuncState",
                "(" + DESC_OBJECT + ")V",
                false);
          }

          mv.visitVarInsn(Opcodes.ILOAD, 7);
          mv.visitJumpInsn(Opcodes.IFNE, nextStep);

          emitFinishHooksUpTo(mv, i);
          mv.visitInsn(Opcodes.RETURN);
        }

        mv.visitLabel(nextStep);
      }

      // Forward final output to nextNode (zero WindowedValue allocation unless OTel changed)
      Label fastExit = new Label();
      Label finishHooks = new Label();
      mv.visitVarInsn(Opcodes.ALOAD, 2);
      mv.visitMethodInsn(
          Opcodes.INVOKESTATIC,
          INTERNAL_COMPILER,
          "hasChangedOtelFast",
          "(" + DESC_WINDOWED_VALUE + ")Z",
          false);
      mv.visitJumpInsn(Opcodes.IFEQ, fastExit);

      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitFieldInsn(
          Opcodes.GETFIELD, internalClassName, "nextNode", Type.getDescriptor(nextNodeClass));
      mv.visitVarInsn(Opcodes.ALOAD, 2);
      mv.visitVarInsn(Opcodes.ALOAD, 1);
      mv.visitInsn(allFilters ? Opcodes.ICONST_1 : Opcodes.ICONST_0);
      mv.visitMethodInsn(
          Opcodes.INVOKESTATIC,
          INTERNAL_COMPILER,
          "buildChangedOtelOutput",
          "(" + DESC_WINDOWED_VALUE + DESC_OBJECT + "Z)" + DESC_WINDOWED_VALUE,
          false);
      mv.visitMethodInsn(
          nextIsInterface ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL,
          Type.getInternalName(nextNodeClass),
          "acceptWindowed",
          "(" + DESC_WINDOWED_VALUE + ")V",
          nextIsInterface);
      mv.visitJumpInsn(Opcodes.GOTO, finishHooks);

      mv.visitLabel(fastExit);
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitFieldInsn(
          Opcodes.GETFIELD, internalClassName, "nextNode", Type.getDescriptor(nextNodeClass));
      mv.visitVarInsn(Opcodes.ALOAD, 1);
      mv.visitVarInsn(Opcodes.ALOAD, 2);
      mv.visitVarInsn(Opcodes.ILOAD, 3);
      mv.visitMethodInsn(
          nextIsInterface ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL,
          Type.getInternalName(nextNodeClass),
          "acceptRaw",
          "(" + DESC_OBJECT + DESC_WINDOWED_VALUE + "I)V",
          nextIsInterface);

      mv.visitLabel(finishHooks);
      emitFinishHooksUpTo(mv, k - 1);
      mv.visitInsn(Opcodes.RETURN);

      // Catch blocks for each step i
      for (int i = 0; i < k; i++) {
        mv.visitLabel(catchHandlers[i]);
        mv.visitVarInsn(Opcodes.ASTORE, 6);
        mv.visitVarInsn(Opcodes.ALOAD, 6);
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        if (hasHookFlags[i]) {
          mv.visitVarInsn(Opcodes.ALOAD, 5);
          mv.visitVarInsn(Opcodes.ALOAD, 4);
        } else {
          mv.visitInsn(Opcodes.ACONST_NULL);
          mv.visitInsn(Opcodes.ACONST_NULL);
        }
        mv.visitInsn(isSystemFlags[i] ? Opcodes.ICONST_1 : Opcodes.ICONST_0);
        mv.visitInsn(wrapFlags[i] ? Opcodes.ICONST_1 : Opcodes.ICONST_0);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            INTERNAL_COMPILER,
            "onFuncStepException",
            "("
                + DESC_THROWABLE
                + DESC_WINDOWED_VALUE
                + DESC_OBJECT
                + DESC_STEP_HOOK
                + "ZZ)"
                + DESC_EXCEPTION,
            false);
        mv.visitInsn(Opcodes.ATHROW);
      }

      return new Size(6, 8);
    }
  }

  private static void pushInt(MethodVisitor mv, int value) {
    if (value >= -1 && value <= 5) {
      mv.visitInsn(Opcodes.ICONST_0 + value);
    } else if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
      mv.visitIntInsn(Opcodes.BIPUSH, value);
    } else {
      mv.visitIntInsn(Opcodes.SIPUSH, value);
    }
  }

  // ---- ClassLoader & Cache Infrastructure ----

  private static final class SegmentClassLoader extends ClassLoader {
    SegmentClassLoader(@Nullable ClassLoader parent) {
      super(parent);
    }

    Class<?> defineGeneratedClass(String name, byte[] bytes) {
      return defineClass(name, bytes, 0, bytes.length);
    }
  }

  private static final class LoaderNamespace {
    final SegmentClassLoader segmentLoader;
    final Cache<Object, Constructor<?>> constructorCache;

    LoaderNamespace(@Nullable ClassLoader parent) {
      this.segmentLoader = new SegmentClassLoader(parent);
      this.constructorCache =
          CacheBuilder.newBuilder().maximumSize(MAX_CACHED_CLASSES_PER_LOADER).build();
    }
  }

  private static final class DoFnNodeCacheKey {
    private final Class<?> doFnClass;
    private final Class<?> invokerRuntimeClass;
    private final Class<?> invokerFieldClass;
    private final Class<?> nextNodeRuntimeClass;
    private final Class<?> nextNodeFieldClass;
    private final boolean hasEntryHook;
    private final int hash;

    DoFnNodeCacheKey(
        Class<?> doFnClass,
        Class<?> invokerRuntimeClass,
        Class<?> invokerFieldClass,
        Class<?> nextNodeRuntimeClass,
        Class<?> nextNodeFieldClass,
        boolean hasEntryHook) {
      this.doFnClass = doFnClass;
      this.invokerRuntimeClass = invokerRuntimeClass;
      this.invokerFieldClass = invokerFieldClass;
      this.nextNodeRuntimeClass = nextNodeRuntimeClass;
      this.nextNodeFieldClass = nextNodeFieldClass;
      this.hasEntryHook = hasEntryHook;
      this.hash =
          Objects.hash(
              doFnClass,
              invokerRuntimeClass,
              invokerFieldClass,
              nextNodeRuntimeClass,
              nextNodeFieldClass,
              hasEntryHook);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof DoFnNodeCacheKey)) {
        return false;
      }
      DoFnNodeCacheKey other = (DoFnNodeCacheKey) obj;
      return doFnClass == other.doFnClass
          && invokerRuntimeClass == other.invokerRuntimeClass
          && invokerFieldClass == other.invokerFieldClass
          && nextNodeRuntimeClass == other.nextNodeRuntimeClass
          && nextNodeFieldClass == other.nextNodeFieldClass
          && hasEntryHook == other.hasEntryHook;
    }
  }

  private static final class FuncSegmentCacheKey {
    private final InlineableFunctionProvider.Kind[] kinds;
    private final Class<?>[] runtimeFnClasses;
    private final Class<?>[] fieldFnClasses;
    private final boolean[] hasHookFlags;
    private final boolean[] isSystemFlags;
    private final boolean[] wrapFlags;
    private final Class<?> nextNodeRuntimeClass;
    private final Class<?> nextNodeFieldClass;
    private final int hash;

    FuncSegmentCacheKey(
        InlineableFunctionProvider.Kind[] kinds,
        Class<?>[] runtimeFnClasses,
        Class<?>[] fieldFnClasses,
        boolean[] hasHookFlags,
        boolean[] isSystemFlags,
        boolean[] wrapFlags,
        Class<?> nextNodeRuntimeClass,
        Class<?> nextNodeFieldClass) {
      this.kinds = kinds;
      this.runtimeFnClasses = runtimeFnClasses;
      this.fieldFnClasses = fieldFnClasses;
      this.hasHookFlags = hasHookFlags;
      this.isSystemFlags = isSystemFlags;
      this.wrapFlags = wrapFlags;
      this.nextNodeRuntimeClass = nextNodeRuntimeClass;
      this.nextNodeFieldClass = nextNodeFieldClass;
      int h = Arrays.hashCode(kinds);
      h = 31 * h + Arrays.hashCode(runtimeFnClasses);
      h = 31 * h + Arrays.hashCode(fieldFnClasses);
      h = 31 * h + Arrays.hashCode(hasHookFlags);
      h = 31 * h + Arrays.hashCode(isSystemFlags);
      h = 31 * h + Arrays.hashCode(wrapFlags);
      h = 31 * h + nextNodeRuntimeClass.hashCode();
      h = 31 * h + nextNodeFieldClass.hashCode();
      this.hash = h;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof FuncSegmentCacheKey)) {
        return false;
      }
      FuncSegmentCacheKey other = (FuncSegmentCacheKey) obj;
      return nextNodeRuntimeClass == other.nextNodeRuntimeClass
          && nextNodeFieldClass == other.nextNodeFieldClass
          && Arrays.equals(kinds, other.kinds)
          && Arrays.equals(runtimeFnClasses, other.runtimeFnClasses)
          && Arrays.equals(fieldFnClasses, other.fieldFnClasses)
          && Arrays.equals(hasHookFlags, other.hasHookFlags)
          && Arrays.equals(isSystemFlags, other.isSystemFlags)
          && Arrays.equals(wrapFlags, other.wrapFlags);
    }
  }
}
