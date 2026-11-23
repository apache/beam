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
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Ownership;
import net.bytebuddy.description.modifier.TypeManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassInjector;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
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

  /**
   * Default minimum 1-to-1 chain length before ByteBuddy segment compilation activates. Chains of
   * 1–5 steps already fit within HotSpot C2's {@code -XX:MaxInlineLevel=15} and {@code
   * -XX:TypeProfileWidth=2} budgets without paying segment/hook bridge or classgen overhead.
   */
  public static final int DEFAULT_MIN_CODEGEN_CHAIN_STEPS = 6;

  public static final String MIN_CODEGEN_CHAIN_STEPS_PREFIX = "codegen_min_chain_steps=";

  private static final String GENERATED_PACKAGE =
      "org.apache.beam.sdk.transforms.reflect.generated";

  /** Maximum number of consecutive functional (MAP/FILTER) steps fused into a single method. */
  static final int MAX_FUNCTIONS_PER_CHUNK = 16;

  /**
   * Resolves the minimum chain length required to trigger segment codegen, or {@code 0} when {@link
   * #DISABLE_CODEGEN_EXPERIMENT} is active.
   */
  public static int resolveMinCodegenChainSteps(@Nullable List<String> experiments) {
    if (experiments != null) {
      if (experiments.contains(DISABLE_CODEGEN_EXPERIMENT)) {
        return 0;
      }
      for (String exp : experiments) {
        if (exp != null && exp.startsWith(MIN_CODEGEN_CHAIN_STEPS_PREFIX)) {
          return Integer.parseInt(exp.substring(MIN_CODEGEN_CHAIN_STEPS_PREFIX.length()));
        }
      }
    }
    return DEFAULT_MIN_CODEGEN_CHAIN_STEPS;
  }

  @VisibleForTesting
  static boolean hasGeneratedMethodHandleField(CompiledNode node) {
    try {
      return node.getClass().getDeclaredField("MH_PROCESS") != null;
    } catch (NoSuchFieldException e) {
      return false;
    }
  }

  private static final int MAX_INJECTED_CLASSES_PER_LOADER = 4096;
  private static final int MAX_FALLBACK_CLASSES_BOOTSTRAP = 1024;
  private static final int MAX_FALLBACK_CLASSES_PER_ANCHOR_CLASS = 64;

  private static final @Nullable ClassLoader COMPILER_CLASS_LOADER =
      FusedSegmentCompiler.class.getClassLoader();

  private static final ConcurrentMap<ClassLoader, LoaderNamespace> LOADER_NAMESPACES =
      new MapMaker().weakKeys().makeMap();

  private static final LoaderNamespace BOOTSTRAP_NAMESPACE = new LoaderNamespace(true);

  private static final ClassValue<Constructor<?>> GENERATED_CONSTRUCTOR =
      new ClassValue<Constructor<?>>() {
        @Override
        protected Constructor<?> computeValue(Class<?> type) {
          Constructor<?>[] ctors = type.getConstructors();
          if (ctors.length != 1) {
            throw new IllegalStateException(
                "Expected exactly one public constructor on generated class " + type.getName());
          }
          Constructor<?> ctor = ctors[0];
          Module targetModule = type.getModule();
          Module compilerModule = FusedSegmentCompiler.class.getModule();
          String pkg = getPackageName(type);
          if (targetModule.isNamed()
              && targetModule != compilerModule
              && !targetModule.isExported(pkg, compilerModule)) {
            ctor.setAccessible(true);
          }
          return ctor;
        }
      };

  private static final ClassValue<Cache<Object, Class<?>>> ANCHOR_FALLBACK_CACHE =
      new ClassValue<Cache<Object, Class<?>>>() {
        @Override
        protected Cache<Object, Class<?>> computeValue(Class<?> type) {
          return CacheBuilder.newBuilder()
              .maximumSize(MAX_FALLBACK_CLASSES_PER_ANCHOR_CLASS)
              .build();
        }
      };

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
    public final boolean wrapDownstreamInUserCode;
    protected final StepContextFallback<InputT, OutputT> fallback;
    private @Nullable MultiOutputReceiver cachedMultiReceiver;

    protected FusedDoFnNodeBase(
        DoFn<InputT, OutputT> fn,
        StepContextFallback<InputT, OutputT> fallback,
        boolean wrapDownstreamInUserCode) {
      fn.super();
      this.fallback = fallback;
      this.wrapDownstreamInUserCode = wrapDownstreamInUserCode;
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
      if (tmpl == null) {
        throw new IllegalStateException(
            "Attempting to emit an element outside of a @ProcessElement context.");
      }
      fallback.checkTimestamp(tmpl, windowedValue.getTimestamp());
      try {
        emitWindowed(windowedValue);
      } catch (Throwable t) {
        throw rethrowDownstreamOutputException(t, wrapDownstreamInUserCode);
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
      } catch (Throwable t) {
        throw rethrowDownstreamOutputException(t, wrapDownstreamInUserCode);
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
      throw new IllegalStateException("Cannot provide row output receiver without a schema coder.");
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      if (cachedMultiReceiver == null) {
        cachedMultiReceiver =
            new MultiOutputReceiver() {
              private final Map<TupleTag<?>, DoFn.OutputReceiver<?>> taggedReceivers =
                  new HashMap<>();

              @Override
              public <T> DoFn.OutputReceiver<T> get(TupleTag<T> tag) {
                if (tag == null || fallback.isMainOutputTag(tag)) {
                  return (DoFn.OutputReceiver<T>) FusedDoFnNodeBase.this;
                }
                return (DoFn.OutputReceiver<T>)
                    taggedReceivers.computeIfAbsent(
                        tag,
                        ignored ->
                            new DoFn.OutputReceiver<T>() {
                              @Override
                              public void output(T value) {
                                FusedDoFnNodeBase.this.output(tag, value);
                              }

                              @Override
                              public void outputWithTimestamp(T value, Instant timestamp) {
                                FusedDoFnNodeBase.this.outputWithTimestamp(tag, value, timestamp);
                              }

                              @Override
                              public OutputBuilder<T> builder(T value) {
                                WindowedValue<InputT> tmpl = FusedDoFnNodeBase.this.templateElem;
                                if (tmpl == null) {
                                  throw new IllegalStateException(
                                      "Attempting to emit an element outside of a @ProcessElement context.");
                                }
                                return WindowedValues.builder((WindowedValue<T>) tmpl)
                                    .withValue(value)
                                    .setReceiver(
                                        wv -> FusedDoFnNodeBase.this.outputWindowedValue(tag, wv));
                              }
                            });
              }

              @Override
              public <T> DoFn.OutputReceiver<Row> getRowReceiver(TupleTag<T> tag) {
                throw new IllegalStateException(
                    "Output with tag "
                        + tag
                        + " must have a schema in order to call getRowReceiver");
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

  public static boolean needsOtelScope(WindowedValue<?> elem) {
    Context otel = elem.getOpenTelemetryContext();
    return otel != null && otel != Context.current();
  }

  public static boolean hasChangedOtel(@Nullable WindowedValue<?> elem) {
    if (elem == null) {
      throw new IllegalStateException(
          "Attempting to emit an element outside of a @ProcessElement context.");
    }
    Context cur = Context.current();
    Context elemOtel = elem.getOpenTelemetryContext();
    return cur != elemOtel && !(cur == ROOT_OTEL_CONTEXT && elemOtel == null);
  }

  public static boolean hasChangedOtelFast(WindowedValue<?> elem) {
    Context cur = Context.current();
    Context elemOtel = elem.getOpenTelemetryContext();
    return cur != elemOtel && !(cur == ROOT_OTEL_CONTEXT && elemOtel == null);
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
      WindowedValue<?> templateElem, @Nullable Object value) {
    return WindowedValues.builder((WindowedValue<Object>) templateElem).withValue(value).build();
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
    if (t instanceof Error && (!wrapErrorsAsUserCode || isSystemDoFn)) {
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

  @VisibleForTesting
  static int bootstrapInjectedClassCount() {
    return BOOTSTRAP_NAMESPACE.injectedCount.get();
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
        currentNext = instantiateFunctionalSegment(chunk, currentNext);
        idx = startInclusive - 1;
      } else if (spec instanceof DoFnStepSpec) {
        currentNext = instantiateDoFnNode((DoFnStepSpec<?, ?>) spec, currentNext);
        idx--;
      } else {
        throw new IllegalArgumentException("Unsupported StepSpec: " + spec.getClass());
      }
    }
    return currentNext;
  }

  private static boolean canLoadBeam(@Nullable ClassLoader loader) {
    if (loader == null || loader instanceof SegmentClassLoader) {
      return false;
    }
    if (loader == COMPILER_CLASS_LOADER) {
      return true;
    }
    try {
      return Class.forName(FusedSegmentCompiler.class.getName(), false, loader)
          == FusedSegmentCompiler.class;
    } catch (Throwable t) {
      return false;
    }
  }

  private static boolean isBootstrapOrAncestorLoader(@Nullable ClassLoader loader) {
    return loader == null
        || loader == COMPILER_CLASS_LOADER
        || (COMPILER_CLASS_LOADER != null && isAncestor(loader, COMPILER_CLASS_LOADER));
  }

  private static boolean isStableInLoader(
      @Nullable Class<?> cls, @Nullable ClassLoader targetLoader) {
    if (cls == null) {
      return true;
    }
    ClassLoader cl = cls.getClassLoader();
    if (cl instanceof SegmentClassLoader) {
      return false;
    }
    return cl == null
        || cl == targetLoader
        || (targetLoader != null && isAncestor(cl, targetLoader));
  }

  private static @Nullable Class<?> stableKeyClass(
      @Nullable Class<?> runtimeCls,
      @Nullable Class<?> fieldCls,
      @Nullable ClassLoader targetLoader) {
    return isStableInLoader(runtimeCls, targetLoader) ? runtimeCls : fieldCls;
  }

  private static LoaderNamespace namespaceForLoader(@Nullable ClassLoader loader) {
    if (loader == null || loader == COMPILER_CLASS_LOADER) {
      return BOOTSTRAP_NAMESPACE;
    }
    return LOADER_NAMESPACES.computeIfAbsent(loader, ignored -> new LoaderNamespace(false));
  }

  private static final class HostTarget {
    final @Nullable ClassLoader hostLoader;
    final String hostPackage;
    final @Nullable Module hostModule;
    final MethodHandles.@Nullable Lookup packageLookup;

    HostTarget(
        @Nullable ClassLoader hostLoader,
        String hostPackage,
        @Nullable Module hostModule,
        MethodHandles.@Nullable Lookup packageLookup) {
      this.hostLoader = hostLoader;
      this.hostPackage = hostPackage;
      this.hostModule = hostModule;
      this.packageLookup = packageLookup;
    }
  }

  private static String getPackageName(Class<?> cls) {
    String name = cls.getName();
    int slashIdx = name.indexOf('/');
    if (slashIdx >= 0) {
      name = name.substring(0, slashIdx);
    }
    int dotIdx = name.lastIndexOf('.');
    return dotIdx > 0 ? name.substring(0, dotIdx) : "";
  }

  private static boolean isModuleAccessible(Class<?> target, @Nullable Module hostModule) {
    if (hostModule == null) {
      return true;
    }
    Module targetModule = target.getModule();
    if (targetModule == hostModule) {
      return true;
    }
    if (!hostModule.canRead(targetModule)) {
      return false;
    }
    String targetPkg = getPackageName(target);
    return targetPkg.isEmpty()
        ? !targetModule.isNamed()
        : targetModule.isExported(targetPkg, hostModule);
  }

  private static boolean canAnchorModuleReadBeam(Module anchorModule) {
    Module compilerModule = FusedSegmentCompiler.class.getModule();
    if (!anchorModule.canRead(compilerModule)
        || !compilerModule.isExported(getPackageName(FusedSegmentCompiler.class), anchorModule)
        || !compilerModule.isExported("org.apache.beam.sdk.transforms", anchorModule)
        || !compilerModule.isExported("org.apache.beam.sdk.transforms.windowing", anchorModule)
        || !compilerModule.isExported("org.apache.beam.sdk.values", anchorModule)
        || !compilerModule.isExported("org.apache.beam.sdk.options", anchorModule)) {
      return false;
    }
    Module instantModule = Instant.class.getModule();
    return anchorModule.canRead(instantModule)
        && instantModule.isExported(getPackageName(Instant.class), anchorModule);
  }

  private static HostTarget resolveHostTarget(
      @Nullable Class<?> anchorCandidate,
      @Nullable ClassLoader fallbackParentLoader,
      boolean allowLookupInjection) {
    if (allowLookupInjection
        && anchorCandidate != null
        && anchorCandidate.getName().indexOf('/') < 0
        && !anchorCandidate.isPrimitive()
        && !anchorCandidate.isArray()) {
      String pkg = getPackageName(anchorCandidate);
      ClassLoader anchorLoader = anchorCandidate.getClassLoader();
      Module anchorModule = anchorCandidate.getModule();
      Module compilerModule = FusedSegmentCompiler.class.getModule();
      boolean ctorAccessibleFromCompiler =
          !anchorModule.isNamed()
              || anchorModule == compilerModule
              || anchorModule.isExported(pkg, compilerModule)
              || anchorModule.isOpen(pkg, compilerModule);
      if (!pkg.isEmpty()
          && !pkg.startsWith("java.")
          && !pkg.startsWith("javax.")
          && !pkg.startsWith("jdk.")
          && !pkg.startsWith("sun.")
          && anchorLoader != null
          && !(anchorLoader instanceof SegmentClassLoader)
          && ctorAccessibleFromCompiler
          && canAnchorModuleReadBeam(anchorModule)) {
        try {
          if (canLoadBeam(anchorLoader) && ClassInjector.UsingLookup.isAvailable()) {
            MethodHandles.Lookup privateLookup =
                MethodHandles.privateLookupIn(anchorCandidate, MethodHandles.lookup());
            if ((privateLookup.lookupModes() & MethodHandles.Lookup.PACKAGE) != 0) {
              return new HostTarget(anchorLoader, pkg, anchorModule, privateLookup);
            }
          }
        } catch (Throwable ignored) {
          // Fall back to isolated SegmentClassLoader below
        }
      }
    }
    ClassLoader parent =
        fallbackParentLoader != null ? fallbackParentLoader : COMPILER_CLASS_LOADER;
    Module fallbackModule =
        parent != null ? parent.getUnnamedModule() : FusedSegmentCompiler.class.getModule();
    return new HostTarget(parent, GENERATED_PACKAGE, fallbackModule, null);
  }

  private static Class<?> defineGeneratedClass(
      HostTarget host, DynamicType.Unloaded<?> unloaded, String className) {
    if (host.packageLookup != null) {
      return unloaded
          .load(host.hostLoader, ClassLoadingStrategy.UsingLookup.of(host.packageLookup))
          .getLoaded();
    }
    SegmentClassLoader isolatedLoader = new SegmentClassLoader(host.hostLoader);
    return isolatedLoader.defineGeneratedClass(className, unloaded.getBytes());
  }

  private static boolean isSameRuntimePackage(Class<?> target, HostTarget host) {
    return host.packageLookup != null
        && target != null
        && target.getName().indexOf('/') < 0
        && target.getClassLoader() == host.hostLoader
        && getPackageName(target).equals(host.hostPackage);
  }

  private static boolean isPublicClassChain(Class<?> target) {
    if (target.getName().indexOf('/') >= 0 || !Modifier.isPublic(target.getModifiers())) {
      return false;
    }
    for (Class<?> enc = target.getEnclosingClass(); enc != null; enc = enc.getEnclosingClass()) {
      if (enc.getName().indexOf('/') >= 0 || !Modifier.isPublic(enc.getModifiers())) {
        return false;
      }
    }
    return true;
  }

  static boolean isAccessibleFromHost(Class<?> target, HostTarget host) {
    if (target == null || target.getName().indexOf('/') >= 0) {
      return false;
    }
    if (target.isPrimitive()) {
      return true;
    }
    if (target.isArray()) {
      return isAccessibleFromHost(target.getComponentType(), host);
    }
    if (!isModuleAccessible(target, host.hostModule)) {
      return false;
    }
    try {
      if (Class.forName(target.getName(), false, host.hostLoader) != target) {
        return false;
      }
    } catch (Throwable t) {
      return false;
    }
    return isSameRuntimePackage(target, host) || isPublicClassChain(target);
  }

  private static Class<?> resolveNextNodeFieldClass(
      Class<?> nextNodeRuntimeClass, HostTarget host) {
    if (isAccessibleFromHost(nextNodeRuntimeClass, host)) {
      return nextNodeRuntimeClass;
    }
    if (TailReceiver.class.isAssignableFrom(nextNodeRuntimeClass)) {
      return TailReceiver.class;
    }
    return CompiledNode.class;
  }

  private static @Nullable Class<?> resolveHookFieldClass(
      @Nullable StepHook hook, HostTarget host) {
    if (hook == null) {
      return null;
    }
    Class<?> hookCls = hook.getClass();
    return isAccessibleFromHost(hookCls, host) ? hookCls : StepHook.class;
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
      Class<?> fnClass, HostTarget host) {
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
      boolean allElementTypesAccessible = true;
      for (int i = 0; i < extraParams.size(); i++) {
        DoFnSignature.Parameter p = extraParams.get(i);
        Class<?> jvmParam = jvmParamTypes[i];
        if (jvmParam.isPrimitive()) {
          return null;
        }
        if (p instanceof DoFnSignature.Parameter.ElementParameter) {
          kinds[i] = DirectParamKind.ELEMENT;
          if (!isAccessibleFromHost(jvmParam, host)) {
            allElementTypesAccessible = false;
          }
        } else if (p instanceof DoFnSignature.Parameter.OutputReceiverParameter) {
          if (jvmParam != DoFn.OutputReceiver.class
              || ((DoFnSignature.Parameter.OutputReceiverParameter) p).isRowReceiver()) {
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
          needsCurrentVal = true;
        } else {
          return null;
        }
      }

      boolean methodAccessible =
          !Modifier.isPrivate(targetMethod.getModifiers())
              && (Modifier.isPublic(targetMethod.getModifiers())
                  || isSameRuntimePackage(targetMethod.getDeclaringClass(), host));
      if (methodAccessible
          && isAccessibleFromHost(fnClass, host)
          && isAccessibleFromHost(targetMethod.getDeclaringClass(), host)
          && allElementTypesAccessible) {
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

  private static CompiledNode instantiateDoFnNode(DoFnStepSpec<?, ?> spec, CompiledNode nextNode) {
    Class<?> fnClass = spec.fn.getClass();
    ClassLoader fnLoader = fnClass.getClassLoader();
    ClassLoader stepFallbackLoader = canLoadBeam(fnLoader) ? fnLoader : COMPILER_CLASS_LOADER;
    Class<?> stepCacheAnchor =
        (fnLoader instanceof SegmentClassLoader) ? FusedSegmentCompiler.class : fnClass;

    Class<?> invokerRuntimeClass = spec.invoker.getClass();
    Class<?> nextNodeRuntimeClass = nextNode.getClass();
    Class<?> hookRuntimeClass = spec.entryHook != null ? spec.entryHook.getClass() : null;

    HostTarget host = resolveHostTarget(fnClass, stepFallbackLoader, true);
    Class<?> generatedClass = null;
    try {
      if (host.packageLookup != null) {
        Class<?> invokerFieldClass =
            isAccessibleFromHost(invokerRuntimeClass, host)
                ? invokerRuntimeClass
                : DoFnInvoker.class;
        Class<?> nextNodeFieldClass = resolveNextNodeFieldClass(nextNodeRuntimeClass, host);
        Class<?> hookFieldClass = resolveHookFieldClass(spec.entryHook, host);
        DoFnNodeCacheKey key =
            new DoFnNodeCacheKey(
                host.hostLoader,
                host.hostPackage,
                true,
                fnClass,
                stableKeyClass(invokerRuntimeClass, invokerFieldClass, host.hostLoader),
                invokerFieldClass,
                stableKeyClass(nextNodeRuntimeClass, nextNodeFieldClass, host.hostLoader),
                nextNodeFieldClass,
                stableKeyClass(hookRuntimeClass, hookFieldClass, host.hostLoader),
                hookFieldClass,
                null);
        LoaderNamespace hostNs = namespaceForLoader(host.hostLoader);
        HostTarget lookupHost = host;
        generatedClass =
            hostNs.getOrInject(
                key,
                () ->
                    generateDoFnNodeClass(
                        lookupHost,
                        fnClass,
                        invokerFieldClass,
                        nextNodeFieldClass,
                        hookFieldClass));
      }

      if (generatedClass == null) {
        HostTarget fallbackHost =
            (host.packageLookup == null)
                ? host
                : resolveHostTarget(null, stepFallbackLoader, false);
        Class<?> invokerFieldClass =
            isAccessibleFromHost(invokerRuntimeClass, fallbackHost)
                ? invokerRuntimeClass
                : DoFnInvoker.class;
        Class<?> nextNodeFieldClass = resolveNextNodeFieldClass(nextNodeRuntimeClass, fallbackHost);
        Class<?> hookFieldClass = resolveHookFieldClass(spec.entryHook, fallbackHost);
        DoFnNodeCacheKey fallbackKey =
            new DoFnNodeCacheKey(
                fallbackHost.hostLoader,
                fallbackHost.hostPackage,
                false,
                fnClass,
                stableKeyClass(invokerRuntimeClass, invokerFieldClass, fallbackHost.hostLoader),
                invokerFieldClass,
                stableKeyClass(nextNodeRuntimeClass, nextNodeFieldClass, fallbackHost.hostLoader),
                nextNodeFieldClass,
                stableKeyClass(hookRuntimeClass, hookFieldClass, fallbackHost.hostLoader),
                hookFieldClass,
                null);
        Cache<Object, Class<?>> fallbackCache = fallbackCacheFor(stepCacheAnchor);
        generatedClass =
            fallbackCache.get(
                fallbackKey,
                () ->
                    generateDoFnNodeClass(
                        fallbackHost,
                        fnClass,
                        invokerFieldClass,
                        nextNodeFieldClass,
                        hookFieldClass));
      }

      Constructor<?> ctor = GENERATED_CONSTRUCTOR.get(generatedClass);
      return (CompiledNode)
          ctor.newInstance(
              spec.fn,
              spec.fallback,
              spec.invoker,
              spec.entryHook,
              spec.isSystemDoFn,
              spec.wrapDownstreamInUserCode,
              nextNode);
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException("Failed to instantiate compiled DoFn fusion node", e);
    }
  }

  private static Cache<Object, Class<?>> fallbackCacheFor(Class<?> cacheAnchorClass) {
    ClassLoader anchorLoader = cacheAnchorClass.getClassLoader();
    if (isBootstrapOrAncestorLoader(anchorLoader) || anchorLoader instanceof SegmentClassLoader) {
      return checkNotNull(BOOTSTRAP_NAMESPACE.bootstrapFallbackCache);
    }
    return ANCHOR_FALLBACK_CACHE.get(cacheAnchorClass);
  }

  private static CompiledNode instantiateFunctionalSegment(
      List<FunctionalStepSpec> chunk, CompiledNode nextNode) {
    int k = chunk.size();
    InlineableFunctionProvider.Kind[] kinds = new InlineableFunctionProvider.Kind[k];
    Class<?>[] runtimeFnClasses = new Class<?>[k];
    Class<?>[] runtimeHookClasses = new Class<?>[k];
    boolean[] isSystemFlags = new boolean[k];
    boolean[] wrapFlags = new boolean[k];
    ProcessFunction<?, ?>[] fns = new ProcessFunction<?, ?>[k];
    StepHook[] hooks = new StepHook[k];

    Class<?> anchorCandidate = null;
    Class<?> dynamicAnchorClass = null;
    ClassLoader stepFallbackLoader = COMPILER_CLASS_LOADER;
    for (int i = 0; i < k; i++) {
      FunctionalStepSpec s = chunk.get(i);
      kinds[i] = s.kind;
      Class<?> fnCls = s.fn.getClass();
      runtimeFnClasses[i] = fnCls;
      runtimeHookClasses[i] = s.entryHook != null ? s.entryHook.getClass() : null;
      isSystemFlags[i] = s.isSystemDoFn;
      wrapFlags[i] = s.wrapExceptionsAsUserCode;
      fns[i] = s.fn;
      hooks[i] = s.entryHook;
      if (anchorCandidate == null && fnCls.getName().indexOf('/') < 0) {
        anchorCandidate = fnCls;
      }
      ClassLoader fnLoader = fnCls.getClassLoader();
      if (dynamicAnchorClass == null
          && !isBootstrapOrAncestorLoader(fnLoader)
          && !(fnLoader instanceof SegmentClassLoader)) {
        dynamicAnchorClass = fnCls;
        if (canLoadBeam(fnLoader)) {
          stepFallbackLoader = fnLoader;
        }
      }
    }
    Class<?> stepCacheAnchor =
        dynamicAnchorClass != null ? dynamicAnchorClass : FusedSegmentCompiler.class;

    // Only allow UsingLookup into anchorCandidate's loader when every step function in this chunk
    // has lifetime >= anchorCandidate's loader (prevents ephemeral child-loader lambdas from
    // leaking immortal classes into AppClassLoader).
    boolean allowLookup = anchorCandidate != null;
    if (allowLookup) {
      ClassLoader candidateLoader = anchorCandidate.getClassLoader();
      for (int i = 0; i < k; i++) {
        if (!isStableInLoader(runtimeFnClasses[i], candidateLoader)) {
          allowLookup = false;
          break;
        }
      }
    }

    Class<?> nextNodeRuntimeClass = nextNode.getClass();
    HostTarget host = resolveHostTarget(anchorCandidate, stepFallbackLoader, allowLookup);
    Class<?> generatedClass = null;
    try {
      if (host.packageLookup != null) {
        Class<?>[] fieldFnClasses = new Class<?>[k];
        Class<?>[] fieldHookClasses = new Class<?>[k];
        Class<?>[] keyHookRuntimeClasses = new Class<?>[k];
        for (int i = 0; i < k; i++) {
          fieldFnClasses[i] =
              isAccessibleFromHost(runtimeFnClasses[i], host)
                  ? runtimeFnClasses[i]
                  : ProcessFunction.class;
          fieldHookClasses[i] = resolveHookFieldClass(hooks[i], host);
          keyHookRuntimeClasses[i] =
              stableKeyClass(runtimeHookClasses[i], fieldHookClasses[i], host.hostLoader);
        }
        Class<?> nextNodeFieldClass = resolveNextNodeFieldClass(nextNodeRuntimeClass, host);
        FuncSegmentCacheKey key =
            new FuncSegmentCacheKey(
                host.hostLoader,
                host.hostPackage,
                true,
                kinds,
                runtimeFnClasses,
                fieldFnClasses,
                keyHookRuntimeClasses,
                fieldHookClasses,
                isSystemFlags,
                wrapFlags,
                stableKeyClass(nextNodeRuntimeClass, nextNodeFieldClass, host.hostLoader),
                nextNodeFieldClass,
                null);
        LoaderNamespace hostNs = namespaceForLoader(host.hostLoader);
        HostTarget lookupHost = host;
        generatedClass =
            hostNs.getOrInject(
                key,
                () ->
                    generateFunctionalSegmentClass(
                        lookupHost,
                        kinds,
                        fieldFnClasses,
                        fieldHookClasses,
                        isSystemFlags,
                        wrapFlags,
                        nextNodeFieldClass));
      }

      if (generatedClass == null) {
        HostTarget fallbackHost =
            (host.packageLookup == null)
                ? host
                : resolveHostTarget(null, stepFallbackLoader, false);
        Class<?>[] fieldFnClasses = new Class<?>[k];
        Class<?>[] fieldHookClasses = new Class<?>[k];
        Class<?>[] keyFnRuntimeClasses = new Class<?>[k];
        Class<?>[] keyHookRuntimeClasses = new Class<?>[k];
        for (int i = 0; i < k; i++) {
          fieldFnClasses[i] =
              isAccessibleFromHost(runtimeFnClasses[i], fallbackHost)
                  ? runtimeFnClasses[i]
                  : ProcessFunction.class;
          fieldHookClasses[i] = resolveHookFieldClass(hooks[i], fallbackHost);
          keyFnRuntimeClasses[i] =
              stableKeyClass(runtimeFnClasses[i], fieldFnClasses[i], fallbackHost.hostLoader);
          keyHookRuntimeClasses[i] =
              stableKeyClass(runtimeHookClasses[i], fieldHookClasses[i], fallbackHost.hostLoader);
        }
        Class<?> nextNodeFieldClass = resolveNextNodeFieldClass(nextNodeRuntimeClass, fallbackHost);
        FuncSegmentCacheKey fallbackKey =
            new FuncSegmentCacheKey(
                fallbackHost.hostLoader,
                fallbackHost.hostPackage,
                false,
                kinds,
                keyFnRuntimeClasses,
                fieldFnClasses,
                keyHookRuntimeClasses,
                fieldHookClasses,
                isSystemFlags,
                wrapFlags,
                stableKeyClass(nextNodeRuntimeClass, nextNodeFieldClass, fallbackHost.hostLoader),
                nextNodeFieldClass,
                null);
        Cache<Object, Class<?>> fallbackCache = fallbackCacheFor(stepCacheAnchor);
        generatedClass =
            fallbackCache.get(
                fallbackKey,
                () ->
                    generateFunctionalSegmentClass(
                        fallbackHost,
                        kinds,
                        fieldFnClasses,
                        fieldHookClasses,
                        isSystemFlags,
                        wrapFlags,
                        nextNodeFieldClass));
      }

      Constructor<?> ctor = GENERATED_CONSTRUCTOR.get(generatedClass);
      return (CompiledNode) ctor.newInstance(fns, hooks, nextNode);
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException("Failed to instantiate compiled functional fusion segment", e);
    }
  }

  // ---- ByteBuddy Class Generators ----

  private static Class<?> generateDoFnNodeClass(
      HostTarget host,
      Class<?> fnClass,
      Class<?> invokerFieldClass,
      Class<?> nextNodeClass,
      @Nullable Class<?> hookFieldClass)
      throws ClassNotFoundException {
    long id = GENERATED_CLASS_COUNTER.incrementAndGet();
    String prefix = host.hostPackage.isEmpty() ? "" : host.hostPackage + ".";
    String className = prefix + "FusedDoFnNode$" + id;
    DirectProcessPlan directPlan = resolveDirectProcessPlan(fnClass, host);
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
            .defineField("nextNode", nextNodeClass, Visibility.PRIVATE, FieldManifestation.FINAL);

    if (hookFieldClass != null) {
      builder =
          builder.defineField(
              "entryHook", hookFieldClass, Visibility.PRIVATE, FieldManifestation.FINAL);
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
                        className, fnFieldClass, invokerFieldClass, nextNodeClass, hookFieldClass)))
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
                        className, invokerFieldClass, directPlan, hookFieldClass)))
            .visit(
                new AsmVisitorWrapper.ForDeclaredMethods()
                    .writerFlags(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS))
            .make();

    Class<?> generated;
    if (directPlan != null && !directPlan.isDirectVirtual) {
      PENDING_CLINIT_MH.set(directPlan.methodHandle);
      try {
        generated = defineGeneratedClass(host, unloaded, className);
        Class.forName(className, true, generated.getClassLoader());
      } finally {
        PENDING_CLINIT_MH.remove();
      }
    } else {
      generated = defineGeneratedClass(host, unloaded, className);
    }
    return generated;
  }

  private static Class<?> generateFunctionalSegmentClass(
      HostTarget host,
      InlineableFunctionProvider.Kind[] kinds,
      Class<?>[] fieldFnClasses,
      @Nullable Class<?>[] fieldHookClasses,
      boolean[] isSystemFlags,
      boolean[] wrapFlags,
      Class<?> nextNodeClass) {
    long id = GENERATED_CLASS_COUNTER.incrementAndGet();
    String prefix = host.hostPackage.isEmpty() ? "" : host.hostPackage + ".";
    String className = prefix + "FusedFuncSegment$" + id;

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
      if (fieldHookClasses[i] != null) {
        builder =
            builder.defineField(
                "hook_" + i, fieldHookClasses[i], Visibility.PRIVATE, FieldManifestation.FINAL);
      }
    }

    DynamicType.Unloaded<?> unloaded =
        builder
            .defineConstructor(Visibility.PUBLIC)
            .withParameters(ProcessFunction[].class, StepHook[].class, nextNodeClass)
            .intercept(
                new SimpleByteCodeImplementation(
                    new FuncSegmentConstructorAppender(
                        className, fieldFnClasses, fieldHookClasses, nextNodeClass)))
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
                        fieldHookClasses,
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
                        fieldHookClasses,
                        isSystemFlags,
                        wrapFlags,
                        nextNodeClass)))
            .visit(
                new AsmVisitorWrapper.ForDeclaredMethods()
                    .writerFlags(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS))
            .make();

    return defineGeneratedClass(host, unloaded, className);
  }

  // ---- ASM ByteCodeAppenders ----

  private static final String INTERNAL_COMPILER = Type.getInternalName(FusedSegmentCompiler.class);
  private static final String INTERNAL_DOFN_BASE = Type.getInternalName(FusedDoFnNodeBase.class);
  private static final String INTERNAL_FUNC_BODY = Type.getInternalName(FuncSegmentBody.class);
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
    private final @Nullable Class<?> hookFieldClass;

    DoFnNodeConstructorAppender(
        String className,
        Class<?> fnFieldClass,
        Class<?> invokerFieldClass,
        Class<?> nextNodeClass,
        @Nullable Class<?> hookFieldClass) {
      this.internalClassName = className.replace('.', '/');
      this.fnFieldClass = fnFieldClass;
      this.invokerFieldClass = invokerFieldClass;
      this.nextNodeClass = nextNodeClass;
      this.hookFieldClass = hookFieldClass;
    }

    @Override
    public Size apply(
        MethodVisitor mv, Implementation.Context ctx, MethodDescription instrumentedMethod) {
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ALOAD, 1);
      mv.visitVarInsn(Opcodes.ALOAD, 2);
      mv.visitVarInsn(Opcodes.ILOAD, 6);
      mv.visitMethodInsn(
          Opcodes.INVOKESPECIAL,
          INTERNAL_DOFN_BASE,
          "<init>",
          "(" + DESC_DOFN + DESC_FALLBACK + "Z)V",
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

      if (hookFieldClass != null) {
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitVarInsn(Opcodes.ALOAD, 4);
        if (hookFieldClass != StepHook.class) {
          mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(hookFieldClass));
        }
        mv.visitFieldInsn(
            Opcodes.PUTFIELD, internalClassName, "entryHook", Type.getDescriptor(hookFieldClass));
      }

      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ILOAD, 5);
      mv.visitFieldInsn(Opcodes.PUTFIELD, internalClassName, "isSystemDoFn", "Z");

      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ALOAD, 7);
      mv.visitFieldInsn(
          Opcodes.PUTFIELD, internalClassName, "nextNode", Type.getDescriptor(nextNodeClass));

      mv.visitInsn(Opcodes.RETURN);
      return new Size(4, 8);
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
          "hasChangedOtel",
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
      mv.visitFieldInsn(Opcodes.GETFIELD, INTERNAL_DOFN_BASE, "wrapDownstreamInUserCode", "Z");
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
    private final @Nullable Class<?> hookFieldClass;

    DoFnAcceptRawAppender(
        String className,
        Class<?> invokerFieldClass,
        @Nullable DirectProcessPlan directPlan,
        @Nullable Class<?> hookFieldClass) {
      this.internalClassName = className.replace('.', '/');
      this.invokerFieldClass = invokerFieldClass;
      this.directPlan = directPlan;
      this.hookFieldClass = hookFieldClass;
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
      Label fastInvoke = new Label();
      Label tryStart = new Label();
      Label tryEnd = new Label();
      Label catchHandler = new Label();

      mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, "java/lang/Throwable");

      if (hookFieldClass != null) {
        String hookInternal = Type.getInternalName(hookFieldClass);
        String hookDesc = Type.getDescriptor(hookFieldClass);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, internalClassName, "entryHook", hookDesc);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitVarInsn(Opcodes.ILOAD, 3);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL,
            hookInternal,
            "beforeFunc",
            "(" + DESC_OBJECT + DESC_WINDOWED_VALUE + "I)" + DESC_OBJECT,
            false);
        mv.visitVarInsn(Opcodes.ASTORE, 4);
      }

      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ALOAD, 2);
      mv.visitFieldInsn(Opcodes.PUTFIELD, INTERNAL_DOFN_BASE, "templateElem", DESC_WINDOWED_VALUE);
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitVarInsn(Opcodes.ILOAD, 3);
      mv.visitFieldInsn(Opcodes.PUTFIELD, INTERNAL_DOFN_BASE, "currentWinSize", "I");

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
          "needsOtelScope",
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
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitInsn(Opcodes.ACONST_NULL);
      mv.visitFieldInsn(Opcodes.PUTFIELD, INTERNAL_DOFN_BASE, "templateElem", DESC_WINDOWED_VALUE);
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitInsn(Opcodes.ACONST_NULL);
      mv.visitFieldInsn(Opcodes.PUTFIELD, INTERNAL_DOFN_BASE, "currentVal", DESC_OBJECT);
      if (hookFieldClass != null) {
        String hookInternal = Type.getInternalName(hookFieldClass);
        String hookDesc = Type.getDescriptor(hookFieldClass);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, internalClassName, "entryHook", hookDesc);
        mv.visitVarInsn(Opcodes.ALOAD, 4);
        mv.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL, hookInternal, "afterRawNormal", "(" + DESC_OBJECT + ")V", false);
      }
      mv.visitInsn(Opcodes.RETURN);

      mv.visitLabel(catchHandler);
      mv.visitVarInsn(Opcodes.ASTORE, 5);
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitInsn(Opcodes.ACONST_NULL);
      mv.visitFieldInsn(Opcodes.PUTFIELD, INTERNAL_DOFN_BASE, "templateElem", DESC_WINDOWED_VALUE);
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitInsn(Opcodes.ACONST_NULL);
      mv.visitFieldInsn(Opcodes.PUTFIELD, INTERNAL_DOFN_BASE, "currentVal", DESC_OBJECT);
      mv.visitVarInsn(Opcodes.ALOAD, 5);
      mv.visitVarInsn(Opcodes.ALOAD, 2);
      if (hookFieldClass != null) {
        mv.visitVarInsn(Opcodes.ALOAD, 4);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(
            Opcodes.GETFIELD, internalClassName, "entryHook", Type.getDescriptor(hookFieldClass));
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
    private final @Nullable Class<?>[] fieldHookClasses;
    private final Class<?> nextNodeClass;

    FuncSegmentConstructorAppender(
        String className,
        Class<?>[] fieldFnClasses,
        @Nullable Class<?>[] fieldHookClasses,
        Class<?> nextNodeClass) {
      this.internalClassName = className.replace('.', '/');
      this.fieldFnClasses = fieldFnClasses;
      this.fieldHookClasses = fieldHookClasses;
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

        Class<?> hookCls = fieldHookClasses[i];
        if (hookCls != null) {
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          pushInt(mv, i);
          mv.visitInsn(Opcodes.AALOAD);
          if (hookCls != StepHook.class) {
            mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(hookCls));
          }
          mv.visitFieldInsn(
              Opcodes.PUTFIELD, internalClassName, "hook_" + i, Type.getDescriptor(hookCls));
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
    private final @Nullable Class<?>[] fieldHookClasses;
    private final boolean[] isSystemFlags;
    private final boolean[] wrapFlags;
    private final Class<?> nextNodeClass;

    FuncSegmentBodyAppender(
        boolean includeOtelEntryCheck,
        String className,
        InlineableFunctionProvider.Kind[] kinds,
        Class<?>[] fieldFnClasses,
        @Nullable Class<?>[] fieldHookClasses,
        boolean[] isSystemFlags,
        boolean[] wrapFlags,
        Class<?> nextNodeClass) {
      this.includeOtelEntryCheck = includeOtelEntryCheck;
      this.internalClassName = className.replace('.', '/');
      this.kinds = kinds;
      this.fieldFnClasses = fieldFnClasses;
      this.fieldHookClasses = fieldHookClasses;
      this.isSystemFlags = isSystemFlags;
      this.wrapFlags = wrapFlags;
      this.nextNodeClass = nextNodeClass;
    }

    private void emitFinishHooksUpTo(MethodVisitor mv, int maxInclusive) {
      for (int j = maxInclusive; j >= 0; j--) {
        Class<?> hookCls = fieldHookClasses[j];
        if (hookCls != null) {
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitFieldInsn(
              Opcodes.GETFIELD, internalClassName, "hook_" + j, Type.getDescriptor(hookCls));
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL, Type.getInternalName(hookCls), "finishFuncLazy", "()V", false);
        }
      }
    }

    @Override
    public Size apply(
        MethodVisitor mv, Implementation.Context ctx, MethodDescription instrumentedMethod) {
      // Locals: 0=this, 1=v (rawValue), 2=templateElem, 3=winSize, 4=tok, 5=t, 6=keep
      int k = kinds.length;
      boolean nextIsInterface = nextNodeClass.isInterface();
      Label[] tryStarts = new Label[k];
      Label[] tryEnds = new Label[k];
      Label[] catchHandlers = new Label[k];
      for (int i = 0; i < k; i++) {
        tryStarts[i] = new Label();
        tryEnds[i] = new Label();
        catchHandlers[i] = new Label();
        mv.visitTryCatchBlock(tryStarts[i], tryEnds[i], catchHandlers[i], "java/lang/Throwable");
      }

      if (includeOtelEntryCheck) {
        Label fastLabel = new Label();
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        mv.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            INTERNAL_COMPILER,
            "needsOtelScope",
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
        Class<?> hookCls = fieldHookClasses[i];

        if (hookCls != null) {
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitFieldInsn(
              Opcodes.GETFIELD, internalClassName, "hook_" + i, Type.getDescriptor(hookCls));
          mv.visitVarInsn(Opcodes.ALOAD, 1);
          mv.visitVarInsn(Opcodes.ALOAD, 2);
          mv.visitVarInsn(Opcodes.ILOAD, 3);
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL,
              Type.getInternalName(hookCls),
              "beforeFunc",
              "(" + DESC_OBJECT + DESC_WINDOWED_VALUE + "I)" + DESC_OBJECT,
              false);
          mv.visitVarInsn(Opcodes.ASTORE, 4);
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

        if (kinds[i] == InlineableFunctionProvider.Kind.MAP) {
          mv.visitVarInsn(Opcodes.ASTORE, 1);
          mv.visitLabel(tryEnds[i]);
          if (hookCls != null) {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitFieldInsn(
                Opcodes.GETFIELD, internalClassName, "hook_" + i, Type.getDescriptor(hookCls));
            mv.visitVarInsn(Opcodes.ALOAD, 4);
            mv.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(hookCls),
                "exitFuncState",
                "(" + DESC_OBJECT + ")V",
                false);
          }
        } else {
          mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Boolean");
          mv.visitMethodInsn(
              Opcodes.INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue", "()Z", false);
          mv.visitVarInsn(Opcodes.ISTORE, 6);
          mv.visitLabel(tryEnds[i]);

          if (hookCls != null) {
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitFieldInsn(
                Opcodes.GETFIELD, internalClassName, "hook_" + i, Type.getDescriptor(hookCls));
            mv.visitVarInsn(Opcodes.ALOAD, 4);
            mv.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(hookCls),
                "exitFuncState",
                "(" + DESC_OBJECT + ")V",
                false);
          }

          mv.visitVarInsn(Opcodes.ILOAD, 6);
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
      mv.visitMethodInsn(
          Opcodes.INVOKESTATIC,
          INTERNAL_COMPILER,
          "buildChangedOtelOutput",
          "(" + DESC_WINDOWED_VALUE + DESC_OBJECT + ")" + DESC_WINDOWED_VALUE,
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
        mv.visitVarInsn(Opcodes.ASTORE, 5);
        mv.visitVarInsn(Opcodes.ALOAD, 5);
        mv.visitVarInsn(Opcodes.ALOAD, 2);
        Class<?> hookCls = fieldHookClasses[i];
        if (hookCls != null) {
          mv.visitVarInsn(Opcodes.ALOAD, 4);
          mv.visitVarInsn(Opcodes.ALOAD, 0);
          mv.visitFieldInsn(
              Opcodes.GETFIELD, internalClassName, "hook_" + i, Type.getDescriptor(hookCls));
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

      return new Size(6, 7);
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

  @FunctionalInterface
  private interface ClassGenerator {
    Class<?> generate() throws Exception;
  }

  private interface CleanupKey {
    CleanupKey copyForStorage(ReferenceQueue<Object> queue);
  }

  private static final class SegmentClassLoader extends ClassLoader {
    SegmentClassLoader(@Nullable ClassLoader parent) {
      super(parent);
    }

    Class<?> defineGeneratedClass(String name, byte[] bytes) {
      return defineClass(name, bytes, 0, bytes.length);
    }
  }

  /**
   * Per-{@link ClassLoader} cache metadata for {@code UsingLookup}-injected generated classes.
   *
   * <p>Holds <b>zero</b> strong references to the keyed {@link ClassLoader}, user {@link Class}es,
   * or generated {@link Class}es so {@link #LOADER_NAMESPACES}'s weak keys can be reclaimed as soon
   * as a dynamic {@link ClassLoader} becomes unreachable. The {@link #injectedCount} cap bounds
   * injection per loader; overflow falls back to individually unloadable {@link
   * SegmentClassLoader}s.
   */
  private static final class LoaderNamespace {
    final AtomicInteger injectedCount = new AtomicInteger();
    final ReferenceQueue<Object> refQueue = new ReferenceQueue<>();
    final ConcurrentHashMap<CleanupKey, WeakReference<Class<?>>> injectedClasses =
        new ConcurrentHashMap<>();
    final @Nullable Cache<Object, Class<?>> bootstrapFallbackCache;

    LoaderNamespace(boolean isBootstrap) {
      this.bootstrapFallbackCache =
          isBootstrap
              ? CacheBuilder.newBuilder().maximumSize(MAX_FALLBACK_CLASSES_BOOTSTRAP).build()
              : null;
    }

    void expungeStaleEntries() {
      Reference<?> ref;
      while ((ref = refQueue.poll()) != null) {
        if (ref instanceof KeyMemberRef) {
          CleanupKey owner = ((KeyMemberRef<?>) ref).ownerKey;
          if (owner != null) {
            injectedClasses.remove(owner);
          }
        }
      }
    }

    private boolean tryReserveInjectedSlot() {
      while (true) {
        int cur = injectedCount.get();
        if (cur >= MAX_INJECTED_CLASSES_PER_LOADER) {
          return false;
        }
        if (injectedCount.compareAndSet(cur, cur + 1)) {
          return true;
        }
      }
    }

    @Nullable Class<?> getOrInject(CleanupKey lookupKey, ClassGenerator generator)
        throws Exception {
      expungeStaleEntries();
      WeakReference<Class<?>> existingRef = injectedClasses.get(lookupKey);
      if (existingRef != null) {
        Class<?> existing = existingRef.get();
        if (existing != null) {
          return existing;
        }
      }
      if (injectedCount.get() >= MAX_INJECTED_CLASSES_PER_LOADER) {
        return null;
      }
      CleanupKey storedKey = lookupKey.copyForStorage(refQueue);
      Class<?>[] result = new Class<?>[1];
      Throwable[] failure = new Throwable[1];
      injectedClasses.compute(
          storedKey,
          (k, curRef) -> {
            if (curRef != null) {
              Class<?> existing = curRef.get();
              if (existing != null) {
                result[0] = existing;
                return curRef;
              }
            }
            if (!tryReserveInjectedSlot()) {
              return null;
            }
            try {
              Class<?> gen = generator.generate();
              result[0] = gen;
              return new WeakReference<>(gen);
            } catch (Throwable t) {
              injectedCount.decrementAndGet();
              failure[0] = t;
              return null;
            }
          });
      if (failure[0] != null) {
        if (failure[0] instanceof Exception) {
          throw (Exception) failure[0];
        }
        throw (Error) failure[0];
      }
      return result[0];
    }
  }

  private static final class KeyMemberRef<T> extends WeakReference<T> {
    final int hash;
    final @Nullable CleanupKey ownerKey;

    KeyMemberRef(
        T referent, @Nullable ReferenceQueue<Object> queue, @Nullable CleanupKey ownerKey) {
      super(referent, queue);
      this.hash = System.identityHashCode(referent);
      this.ownerKey = ownerKey;
    }

    static <T> @Nullable KeyMemberRef<T> ofNullable(
        @Nullable T referent,
        @Nullable ReferenceQueue<Object> queue,
        @Nullable CleanupKey ownerKey) {
      return referent == null ? null : new KeyMemberRef<>(referent, queue, ownerKey);
    }

    static <T> @Nullable KeyMemberRef<T> copyOf(
        @Nullable KeyMemberRef<T> src, ReferenceQueue<Object> queue, CleanupKey ownerKey) {
      if (src == null) {
        return null;
      }
      T ref = src.get();
      return ref == null ? null : new KeyMemberRef<>(ref, queue, ownerKey);
    }

    static int hashOf(@Nullable KeyMemberRef<?> ref) {
      return ref == null ? 0 : ref.hash;
    }

    static int hashArray(@Nullable KeyMemberRef<?>[] refs) {
      int h = 1;
      for (KeyMemberRef<?> r : refs) {
        h = 31 * h + hashOf(r);
      }
      return h;
    }

    static boolean sameReferent(@Nullable KeyMemberRef<?> a, @Nullable KeyMemberRef<?> b) {
      if (a == b) {
        return true;
      }
      if (a == null || b == null) {
        return false;
      }
      Object ra = a.get();
      Object rb = b.get();
      return ra != null && ra == rb;
    }

    static boolean sameReferentArray(@Nullable KeyMemberRef<?>[] a, @Nullable KeyMemberRef<?>[] b) {
      if (a.length != b.length) {
        return false;
      }
      for (int i = 0; i < a.length; i++) {
        if (!sameReferent(a[i], b[i])) {
          return false;
        }
      }
      return true;
    }
  }

  private static final class DoFnNodeCacheKey implements CleanupKey {
    private final @Nullable KeyMemberRef<ClassLoader> hostLoader;
    private final String hostPackage;
    private final boolean usesLookup;
    private final KeyMemberRef<Class<?>> doFnClass;
    private final KeyMemberRef<Class<?>> invokerRuntimeClass;
    private final KeyMemberRef<Class<?>> invokerFieldClass;
    private final KeyMemberRef<Class<?>> nextNodeRuntimeClass;
    private final KeyMemberRef<Class<?>> nextNodeFieldClass;
    private final @Nullable KeyMemberRef<Class<?>> hookRuntimeClass;
    private final @Nullable KeyMemberRef<Class<?>> hookFieldClass;
    private final int hash;

    DoFnNodeCacheKey(
        @Nullable ClassLoader hostLoader,
        String hostPackage,
        boolean usesLookup,
        Class<?> doFnClass,
        Class<?> invokerRuntimeClass,
        Class<?> invokerFieldClass,
        Class<?> nextNodeRuntimeClass,
        Class<?> nextNodeFieldClass,
        @Nullable Class<?> hookRuntimeClass,
        @Nullable Class<?> hookFieldClass,
        @Nullable ReferenceQueue<Object> queue) {
      CleanupKey owner = queue != null ? this : null;
      this.hostLoader = KeyMemberRef.ofNullable(hostLoader, queue, owner);
      this.hostPackage = hostPackage;
      this.usesLookup = usesLookup;
      this.doFnClass = new KeyMemberRef<>(doFnClass, queue, owner);
      this.invokerRuntimeClass = new KeyMemberRef<>(invokerRuntimeClass, queue, owner);
      this.invokerFieldClass = new KeyMemberRef<>(invokerFieldClass, queue, owner);
      this.nextNodeRuntimeClass = new KeyMemberRef<>(nextNodeRuntimeClass, queue, owner);
      this.nextNodeFieldClass = new KeyMemberRef<>(nextNodeFieldClass, queue, owner);
      this.hookRuntimeClass = KeyMemberRef.ofNullable(hookRuntimeClass, queue, owner);
      this.hookFieldClass = KeyMemberRef.ofNullable(hookFieldClass, queue, owner);
      this.hash = computeHash();
    }

    private DoFnNodeCacheKey(DoFnNodeCacheKey src, ReferenceQueue<Object> queue) {
      this.hostLoader = KeyMemberRef.copyOf(src.hostLoader, queue, this);
      this.hostPackage = src.hostPackage;
      this.usesLookup = src.usesLookup;
      this.doFnClass = KeyMemberRef.copyOf(src.doFnClass, queue, this);
      this.invokerRuntimeClass = KeyMemberRef.copyOf(src.invokerRuntimeClass, queue, this);
      this.invokerFieldClass = KeyMemberRef.copyOf(src.invokerFieldClass, queue, this);
      this.nextNodeRuntimeClass = KeyMemberRef.copyOf(src.nextNodeRuntimeClass, queue, this);
      this.nextNodeFieldClass = KeyMemberRef.copyOf(src.nextNodeFieldClass, queue, this);
      this.hookRuntimeClass = KeyMemberRef.copyOf(src.hookRuntimeClass, queue, this);
      this.hookFieldClass = KeyMemberRef.copyOf(src.hookFieldClass, queue, this);
      this.hash = src.hash;
    }

    @Override
    public CleanupKey copyForStorage(ReferenceQueue<Object> queue) {
      return new DoFnNodeCacheKey(this, queue);
    }

    private int computeHash() {
      return Objects.hash(
          KeyMemberRef.hashOf(hostLoader),
          hostPackage,
          usesLookup,
          KeyMemberRef.hashOf(doFnClass),
          KeyMemberRef.hashOf(invokerRuntimeClass),
          KeyMemberRef.hashOf(invokerFieldClass),
          KeyMemberRef.hashOf(nextNodeRuntimeClass),
          KeyMemberRef.hashOf(nextNodeFieldClass),
          KeyMemberRef.hashOf(hookRuntimeClass),
          KeyMemberRef.hashOf(hookFieldClass));
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
      return hash == other.hash
          && usesLookup == other.usesLookup
          && hostPackage.equals(other.hostPackage)
          && KeyMemberRef.sameReferent(hostLoader, other.hostLoader)
          && KeyMemberRef.sameReferent(doFnClass, other.doFnClass)
          && KeyMemberRef.sameReferent(invokerRuntimeClass, other.invokerRuntimeClass)
          && KeyMemberRef.sameReferent(invokerFieldClass, other.invokerFieldClass)
          && KeyMemberRef.sameReferent(nextNodeRuntimeClass, other.nextNodeRuntimeClass)
          && KeyMemberRef.sameReferent(nextNodeFieldClass, other.nextNodeFieldClass)
          && KeyMemberRef.sameReferent(hookRuntimeClass, other.hookRuntimeClass)
          && KeyMemberRef.sameReferent(hookFieldClass, other.hookFieldClass);
    }
  }

  private static final class FuncSegmentCacheKey implements CleanupKey {
    private final @Nullable KeyMemberRef<ClassLoader> hostLoader;
    private final String hostPackage;
    private final boolean usesLookup;
    private final InlineableFunctionProvider.Kind[] kinds;
    private final KeyMemberRef<Class<?>>[] runtimeFnClasses;
    private final KeyMemberRef<Class<?>>[] fieldFnClasses;
    private final @Nullable KeyMemberRef<Class<?>>[] runtimeHookClasses;
    private final @Nullable KeyMemberRef<Class<?>>[] fieldHookClasses;
    private final boolean[] isSystemFlags;
    private final boolean[] wrapFlags;
    private final KeyMemberRef<Class<?>> nextNodeRuntimeClass;
    private final KeyMemberRef<Class<?>> nextNodeFieldClass;
    private final int hash;

    FuncSegmentCacheKey(
        @Nullable ClassLoader hostLoader,
        String hostPackage,
        boolean usesLookup,
        InlineableFunctionProvider.Kind[] kinds,
        Class<?>[] runtimeFnClasses,
        Class<?>[] fieldFnClasses,
        @Nullable Class<?>[] runtimeHookClasses,
        @Nullable Class<?>[] fieldHookClasses,
        boolean[] isSystemFlags,
        boolean[] wrapFlags,
        Class<?> nextNodeRuntimeClass,
        Class<?> nextNodeFieldClass,
        @Nullable ReferenceQueue<Object> queue) {
      CleanupKey owner = queue != null ? this : null;
      this.hostLoader = KeyMemberRef.ofNullable(hostLoader, queue, owner);
      this.hostPackage = hostPackage;
      this.usesLookup = usesLookup;
      this.kinds = kinds;
      this.runtimeFnClasses = wrapArray(runtimeFnClasses, queue, owner);
      this.fieldFnClasses = wrapArray(fieldFnClasses, queue, owner);
      this.runtimeHookClasses = wrapArray(runtimeHookClasses, queue, owner);
      this.fieldHookClasses = wrapArray(fieldHookClasses, queue, owner);
      this.isSystemFlags = isSystemFlags;
      this.wrapFlags = wrapFlags;
      this.nextNodeRuntimeClass = new KeyMemberRef<>(nextNodeRuntimeClass, queue, owner);
      this.nextNodeFieldClass = new KeyMemberRef<>(nextNodeFieldClass, queue, owner);
      this.hash = computeHash();
    }

    private FuncSegmentCacheKey(FuncSegmentCacheKey src, ReferenceQueue<Object> queue) {
      this.hostLoader = KeyMemberRef.copyOf(src.hostLoader, queue, this);
      this.hostPackage = src.hostPackage;
      this.usesLookup = src.usesLookup;
      this.kinds = src.kinds;
      this.runtimeFnClasses = copyArray(src.runtimeFnClasses, queue, this);
      this.fieldFnClasses = copyArray(src.fieldFnClasses, queue, this);
      this.runtimeHookClasses = copyArray(src.runtimeHookClasses, queue, this);
      this.fieldHookClasses = copyArray(src.fieldHookClasses, queue, this);
      this.isSystemFlags = src.isSystemFlags;
      this.wrapFlags = src.wrapFlags;
      this.nextNodeRuntimeClass = KeyMemberRef.copyOf(src.nextNodeRuntimeClass, queue, this);
      this.nextNodeFieldClass = KeyMemberRef.copyOf(src.nextNodeFieldClass, queue, this);
      this.hash = src.hash;
    }

    private static KeyMemberRef<Class<?>>[] wrapArray(
        @Nullable Class<?>[] classes,
        @Nullable ReferenceQueue<Object> queue,
        @Nullable CleanupKey owner) {
      KeyMemberRef<Class<?>>[] refs = new KeyMemberRef[classes.length];
      for (int i = 0; i < classes.length; i++) {
        refs[i] = KeyMemberRef.ofNullable(classes[i], queue, owner);
      }
      return refs;
    }

    private static KeyMemberRef<Class<?>>[] copyArray(
        @Nullable KeyMemberRef<Class<?>>[] src, ReferenceQueue<Object> queue, CleanupKey owner) {
      KeyMemberRef<Class<?>>[] refs = new KeyMemberRef[src.length];
      for (int i = 0; i < src.length; i++) {
        refs[i] = KeyMemberRef.copyOf(src[i], queue, owner);
      }
      return refs;
    }

    @Override
    public CleanupKey copyForStorage(ReferenceQueue<Object> queue) {
      return new FuncSegmentCacheKey(this, queue);
    }

    private int computeHash() {
      int h =
          Objects.hash(
              KeyMemberRef.hashOf(hostLoader),
              hostPackage,
              usesLookup,
              KeyMemberRef.hashOf(nextNodeRuntimeClass),
              KeyMemberRef.hashOf(nextNodeFieldClass));
      h = 31 * h + Arrays.hashCode(kinds);
      h = 31 * h + KeyMemberRef.hashArray(runtimeFnClasses);
      h = 31 * h + KeyMemberRef.hashArray(fieldFnClasses);
      h = 31 * h + KeyMemberRef.hashArray(runtimeHookClasses);
      h = 31 * h + KeyMemberRef.hashArray(fieldHookClasses);
      h = 31 * h + Arrays.hashCode(isSystemFlags);
      h = 31 * h + Arrays.hashCode(wrapFlags);
      return h;
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
      return hash == other.hash
          && usesLookup == other.usesLookup
          && hostPackage.equals(other.hostPackage)
          && KeyMemberRef.sameReferent(hostLoader, other.hostLoader)
          && KeyMemberRef.sameReferent(nextNodeRuntimeClass, other.nextNodeRuntimeClass)
          && KeyMemberRef.sameReferent(nextNodeFieldClass, other.nextNodeFieldClass)
          && Arrays.equals(kinds, other.kinds)
          && KeyMemberRef.sameReferentArray(runtimeFnClasses, other.runtimeFnClasses)
          && KeyMemberRef.sameReferentArray(fieldFnClasses, other.fieldFnClasses)
          && KeyMemberRef.sameReferentArray(runtimeHookClasses, other.runtimeHookClasses)
          && KeyMemberRef.sameReferentArray(fieldHookClasses, other.fieldHookClasses)
          && Arrays.equals(isSystemFlags, other.isSystemFlags)
          && Arrays.equals(wrapFlags, other.wrapFlags);
    }
  }
}
