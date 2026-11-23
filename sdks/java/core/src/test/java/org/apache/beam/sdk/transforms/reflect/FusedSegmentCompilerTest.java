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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.InlineableFunctionProvider;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.CompiledNode;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.DoFnStepSpec;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.FunctionalStepSpec;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.FusedDoFnNodeBase;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.StepContextFallback;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.StepHook;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.StepSpec;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.TailReceiver;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link FusedSegmentCompiler}. */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class FusedSegmentCompilerTest {

  private static final TupleTag<String> MAIN_TAG = new TupleTag<>("main");
  private static final ContextKey<String> OTEL_KEY = ContextKey.named("compiler-test-otel");

  private static StepContextFallback<String, String> testFallback() {
    final PipelineOptions opts = PipelineOptionsFactory.create();
    return new StepContextFallback<String, String>() {
      @Override
      public PipelineOptions pipelineOptions() {
        return opts;
      }

      @Override
      public BundleFinalizer bundleFinalizer() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void checkTimestamp(WindowedValue<String> elem, Instant timestamp) {
        if (timestamp.isBefore(elem.getTimestamp())) {
          throw new IllegalArgumentException("Timestamp skew");
        }
      }

      @Override
      public boolean isMainOutputTag(TupleTag<?> tag) {
        return MAIN_TAG.equals(tag);
      }

      @Override
      public <T> void outputTagged(WindowedValue<String> elem, TupleTag<T> tag, T output) {
        throw new IllegalArgumentException("Unknown tag " + tag);
      }

      @Override
      public <T> void outputTaggedWithTimestamp(
          WindowedValue<String> elem, TupleTag<T> tag, T output, Instant timestamp) {
        throw new IllegalArgumentException("Unknown tag " + tag);
      }

      @Override
      public <T> void outputTaggedWindowedValue(
          WindowedValue<String> elem,
          TupleTag<T> tag,
          T output,
          Instant timestamp,
          Collection<? extends BoundedWindow> windows,
          PaneInfo paneInfo) {
        throw new IllegalArgumentException("Unknown tag " + tag);
      }

      @Override
      public <T> void outputTaggedWindowedValue(
          WindowedValue<String> elem, TupleTag<T> tag, WindowedValue<T> windowedValue) {
        throw new IllegalArgumentException("Unknown tag " + tag);
      }

      @Override
      public Object schemaElement(WindowedValue<String> elem, int index) {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static final class RecordingHook extends StepHook {
    final String name;
    final List<String> events;
    int count;

    RecordingHook(String name, List<String> events) {
      this.name = name;
      this.events = events;
    }

    @Override
    public @Nullable Object beforeStep(WindowedValue<?> elem) {
      count += elem.getWindows().size();
      events.add("enter:" + name);
      return "tok:" + name;
    }

    @Override
    public void afterStepNormal(WindowedValue<?> elem, @Nullable Object stateToken) {
      assertEquals("tok:" + name, stateToken);
      events.add("exit:" + name);
      events.add("lazy:" + name);
    }

    @Override
    public void afterStepException(
        Throwable t, WindowedValue<?> elem, @Nullable Object stateToken) {
      assertEquals("tok:" + name, stateToken);
      events.add("errExit:" + name);
    }

    @Override
    public @Nullable Object beforeFunc(
        @Nullable Object rawValue, WindowedValue<?> templateElem, int windowsSize) {
      count += windowsSize;
      events.add("enterFunc:" + name + ":" + rawValue);
      return "ftok:" + name;
    }

    @Override
    public void exitFuncState(@Nullable Object stateToken) {
      assertEquals("ftok:" + name, stateToken);
      events.add("exitFunc:" + name);
    }

    @Override
    public void afterFuncException(
        Throwable t, WindowedValue<?> templateElem, @Nullable Object stateToken) {
      assertEquals("ftok:" + name, stateToken);
      events.add("errFunc:" + name);
    }

    @Override
    public void finishFuncLazy() {
      events.add("lazyFunc:" + name);
    }
  }

  // Private static DoFn to verify non-public DoFn class handling without IllegalAccessError
  private static final class PrivateMultiEmitDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      out.output(e + "a");
      out.output(e + "b");
    }
  }

  // Package-private ProcessContext DoFn opening a child OTel span and shifting timestamp
  static final class PackagePrivateOtelTimestampDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {
      String parentSpan = Context.current().get(OTEL_KEY);
      Context child = Context.current().with(OTEL_KEY, parentSpan + "/step");
      try (Scope ignored = child.makeCurrent()) {
        c.outputWithTimestamp(
            c.element() + ":" + c.currentRecordId(), c.timestamp().plus(Duration.millis(100L)));
      }
    }
  }

  @Test
  public void testMixedPrivateDoFnsHiddenLambdasMetadataOtelAndMultiEmit() throws Exception {
    List<String> events = new ArrayList<>();
    RecordingHook h1 = new RecordingHook("s1", events);
    RecordingHook h2 = new RecordingHook("s2", events);
    RecordingHook h3 = new RecordingHook("s3", events);

    PrivateMultiEmitDoFn fn0 = new PrivateMultiEmitDoFn();
    ProcessFunction<String, Boolean> filterLambda = s -> !s.endsWith("a");
    ProcessFunction<String, String> mapLambda = s -> s.toUpperCase();
    PackagePrivateOtelTimestampDoFn fn3 = new PackagePrivateOtelTimestampDoFn();

    List<StepSpec> steps =
        Arrays.asList(
            new DoFnStepSpec<>(
                fn0, DoFnInvokers.invokerFor(fn0), null, testFallback(), false, false),
            new FunctionalStepSpec(
                InlineableFunctionProvider.Kind.FILTER, filterLambda, h1, false, false),
            new FunctionalStepSpec(
                InlineableFunctionProvider.Kind.MAP, mapLambda, h2, false, false),
            new DoFnStepSpec<>(
                fn3, DoFnInvokers.invokerFor(fn3), h3, testFallback(), false, false));

    List<WindowedValue<String>> results = new ArrayList<>();
    CompiledNode head =
        FusedSegmentCompiler.compileChain(
            steps,
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {
                results.add((WindowedValue<String>) elem);
              }
            });

    Context rootOtel = Context.root().with(OTEL_KEY, "trace0");
    Instant baseTs = new Instant(5_000L);
    PaneInfo pane = PaneInfo.createPane(false, true, PaneInfo.Timing.LATE, 2, 1);
    WindowedValue<String> input =
        WindowedValues.of(
            "x",
            baseTs,
            GlobalWindow.INSTANCE,
            pane,
            "id-7",
            77L,
            CausedByDrain.CAUSED_BY_DRAIN,
            rootOtel,
            ValueKind.DELETE);

    head.acceptWindowed(input);

    // "xa" filtered out at s1; "xb" mapped to "XB" at s2, then emitted as "XB:id-7" at ts+100
    assertEquals(1, results.size());
    WindowedValue<String> out = results.get(0);
    assertEquals("XB:id-7", out.getValue());
    assertEquals(baseTs.plus(Duration.millis(100L)), out.getTimestamp());
    assertEquals(pane, out.getPaneInfo());
    assertEquals("id-7", out.getRecordId());
    assertEquals(Long.valueOf(77L), out.getRecordOffset());
    assertEquals(CausedByDrain.CAUSED_BY_DRAIN, out.causedByDrain());
    assertEquals(ValueKind.DELETE, out.getValueKind());
    assertEquals("trace0/step", out.getOpenTelemetryContext().get(OTEL_KEY));

    // Verify exact per-edge counts: s1 saw 2 ("xa", "xb"), s2 saw 1 ("xb"), s3 saw 1 ("XB")
    assertEquals(2, h1.count);
    assertEquals(1, h2.count);
    assertEquals(1, h3.count);
    assertTrue(events.contains("lazyFunc:s1"));
    assertTrue(events.contains("lazyFunc:s2"));
    assertTrue(events.contains("lazyFunc:s3"));
  }

  @Test
  public void testDeepFunctionalSegmentCollapsesFramesAndCachesGeneratedClasses() throws Exception {
    // 25 consecutive MAP/FILTER hidden lambdas (exceeding MAX_FUNCTIONS_PER_CHUNK=16 -> 2 chunks)
    ProcessFunction<String, String> mapFn = s -> s + "!";
    ProcessFunction<String, Boolean> filterFn = s -> !s.isEmpty();

    List<StepSpec> steps = new ArrayList<>();
    for (int i = 0; i < 25; i++) {
      if ((i & 1) == 0) {
        steps.add(
            new FunctionalStepSpec(InlineableFunctionProvider.Kind.MAP, mapFn, null, false, false));
      } else {
        steps.add(
            new FunctionalStepSpec(
                InlineableFunctionProvider.Kind.FILTER, filterFn, null, false, false));
      }
    }

    final AtomicInteger framesAtSink = new AtomicInteger();
    TailReceiver sink =
        new TailReceiver() {
          @Override
          public void accept(WindowedValue<?> elem) {
            int depth =
                StackWalker.getInstance()
                    .walk(
                        s ->
                            (int)
                                s.takeWhile(
                                        f ->
                                            !f.getClassName()
                                                .equals(FusedSegmentCompilerTest.class.getName()))
                                    .count());
            framesAtSink.set(depth);
          }
        };

    CompiledNode firstInstance = FusedSegmentCompiler.compileChain(steps, sink);
    long classCountAfterFirst = FusedSegmentCompiler.generatedClassCount();

    // Compiling a second bundle with the same step classes must reuse cached generated classes!
    CompiledNode secondInstance = FusedSegmentCompiler.compileChain(steps, sink);
    long classCountAfterSecond = FusedSegmentCompiler.generatedClassCount();

    assertSame(firstInstance.getClass(), secondInstance.getClass());
    assertEquals(classCountAfterFirst, classCountAfterSecond);

    secondInstance.acceptWindowed(WindowedValues.valueInGlobalWindow("go"));
    // 25 steps execute in just 2 chunks (~6 total frames instead of 100+ frames!)
    assertThat(framesAtSink.get(), lessThanOrEqualTo(8));
  }

  @Test
  public void testExceptionWrappingAndStateCleanup() {
    List<String> events = new ArrayList<>();
    RecordingHook h1 = new RecordingHook("boomStep", events);
    ProcessFunction<String, String> throwingFn =
        s -> {
          throw new IllegalStateException("user-failure");
        };

    CompiledNode node =
        FusedSegmentCompiler.compileChain(
            Arrays.asList(
                new FunctionalStepSpec(
                    InlineableFunctionProvider.Kind.MAP, s -> s, null, false, true),
                new FunctionalStepSpec(
                    InlineableFunctionProvider.Kind.MAP, throwingFn, h1, false, true)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {}
            });

    UserCodeException ex =
        assertThrows(
            UserCodeException.class,
            () -> node.acceptWindowed(WindowedValues.valueInGlobalWindow("in")));
    assertEquals("user-failure", ex.getCause().getMessage());
    assertTrue("Expected error state exit hook", events.contains("errFunc:boomStep"));
  }

  public static final class PublicDirectDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, @Timestamp Instant ts, OutputReceiver<String> out) {
      if ("assert-fail".equals(e)) {
        throw new AssertionError("direct-assertion-boom");
      }
      out.output(e + "@" + ts.getMillis());
    }
  }

  @Test
  public void testPublicDirectInvokeAndAssertionErrorWrapping() throws Exception {
    List<String> events = new ArrayList<>();
    RecordingHook hook = new RecordingHook("pubStep", events);
    PublicDirectDoFn fn0 = new PublicDirectDoFn();
    PublicDirectDoFn fn1 = new PublicDirectDoFn();
    List<WindowedValue<String>> outList = new ArrayList<>();

    CompiledNode node =
        FusedSegmentCompiler.compileChain(
            Arrays.asList(
                new DoFnStepSpec<>(
                    fn0, DoFnInvokers.invokerFor(fn0), null, testFallback(), false, false),
                new DoFnStepSpec<>(
                    fn1, DoFnInvokers.invokerFor(fn1), hook, testFallback(), false, false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {
                outList.add((WindowedValue<String>) elem);
              }
            });

    node.acceptWindowed(WindowedValues.timestampedValueInGlobalWindow("ok", new Instant(42L)));
    assertEquals(1, outList.size());
    assertEquals("ok@42@42", outList.get(0).getValue());
    assertEquals(1, hook.count);

    UserCodeException err =
        assertThrows(
            UserCodeException.class,
            () ->
                node.acceptWindowed(
                    WindowedValues.timestampedValueInGlobalWindow(
                        "assert-fail", new Instant(42L))));
    assertTrue(err.getCause() instanceof AssertionError);
    assertEquals("direct-assertion-boom", err.getCause().getMessage());
  }

  @Test
  public void testAnonymousAndPrivateDoFnsUseDirectVirtualWithoutLambdaFormsAndConcreteHooks()
      throws Exception {
    List<String> events = new ArrayList<>();
    RecordingHook hook = new RecordingHook("anonStep", events);
    AtomicInteger lambdaFormFramesSeen = new AtomicInteger(-1);

    PrivateMultiEmitDoFn privateFn = new PrivateMultiEmitDoFn();
    DoFn<String, String> anonymousFn =
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            long lfCount =
                StackWalker.getInstance()
                    .walk(
                        s ->
                            s.takeWhile(
                                    f ->
                                        !f.getClassName()
                                            .equals(FusedSegmentCompilerTest.class.getName()))
                                .filter(
                                    f ->
                                        f.getClassName().contains("LambdaForm")
                                            || f.getClassName()
                                                .startsWith("java.lang.invoke.MethodHandle"))
                                .count());
            lambdaFormFramesSeen.set((int) lfCount);
            c.output(c.element() + "!");
          }
        };

    List<WindowedValue<String>> outList = new ArrayList<>();
    CompiledNode head =
        FusedSegmentCompiler.compileChain(
            Arrays.asList(
                new DoFnStepSpec<>(
                    privateFn,
                    DoFnInvokers.invokerFor(privateFn),
                    null,
                    testFallback(),
                    false,
                    false),
                new DoFnStepSpec<>(
                    anonymousFn,
                    DoFnInvokers.invokerFor(anonymousFn),
                    hook,
                    testFallback(),
                    false,
                    false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {
                outList.add((WindowedValue<String>) elem);
              }
            });

    // Both private static and anonymous DoFn nodes are package-injected via UsingLookup ->
    // direct INVOKEVIRTUAL with zero MH_PROCESS / LambdaForm adapter frames and exact hook field!
    org.junit.Assert.assertFalse(FusedSegmentCompiler.hasGeneratedMethodHandleField(head));
    java.lang.reflect.Field nextField = head.getClass().getDeclaredField("nextNode");
    nextField.setAccessible(true);
    Object nextNode = nextField.get(head);
    org.junit.Assert.assertFalse(
        FusedSegmentCompiler.hasGeneratedMethodHandleField((CompiledNode) nextNode));
    assertEquals(RecordingHook.class, nextNode.getClass().getDeclaredField("entryHook").getType());

    head.acceptWindowed(WindowedValues.valueInGlobalWindow("x"));
    assertEquals(0, lambdaFormFramesSeen.get());
    assertEquals(2, outList.size());
    assertEquals("xa!", outList.get(0).getValue());
    assertEquals("xb!", outList.get(1).getValue());
    assertEquals(2, hook.count);
  }

  public static final class RowOutputDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<Row> out) {
      out.output(null);
    }
  }

  public static final class LazyMultiOutputDoFn extends DoFn<String, String> {
    static final TupleTag<String> DLQ_TAG = new TupleTag<>("dlq");

    @ProcessElement
    public void process(@Element String e, MultiOutputReceiver out) {
      // Eagerly fetching an unused non-main tag must NOT throw; get(null) routes to main output.
      DoFn.OutputReceiver<String> dlq = out.get(DLQ_TAG);
      if ("send-dlq".equals(e)) {
        dlq.output("err:" + e);
      } else {
        out.get((TupleTag<String>) null).output("ok:" + e);
      }
    }
  }

  public static final class RetainingOutputReceiverDoFn extends DoFn<String, String> {
    final AtomicReference<DoFn.OutputReceiver<String>> saved = new AtomicReference<>();

    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      saved.set(out);
      out.output(e + "-in");
    }
  }

  public static final class SystemErrorDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if ("boom".equals(e)) {
        throw new AssertionError("system-error:" + e);
      }
      out.output(e);
    }
  }

  public static final class EmitWithTimestampDoFn extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, @Timestamp Instant ts, OutputReceiver<String> out) {
      out.outputWithTimestamp(e, ts);
    }
  }

  @Test
  public void testFilterNullReturnExitsHookAndWrapsInUserCodeException() {
    List<String> events = new ArrayList<>();
    RecordingHook hook = new RecordingHook("nullFilter", events);
    ProcessFunction<String, Boolean> nullReturningPred = s -> null;
    CompiledNode node =
        FusedSegmentCompiler.compileChain(
            Collections.singletonList(
                new FunctionalStepSpec(
                    InlineableFunctionProvider.Kind.FILTER, nullReturningPred, hook, false, false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {}
            });

    UserCodeException ex =
        assertThrows(
            UserCodeException.class,
            () -> node.acceptWindowed(WindowedValues.valueInGlobalWindow("x")));
    assertTrue(ex.getCause() instanceof NullPointerException);
    assertEquals(Arrays.asList("enterFunc:nullFilter:x", "errFunc:nullFilter"), events);
  }

  @Test
  public void testAllFilterSegmentAfterTransformPreservesTransformedValueWhenOtelChanges()
      throws Exception {
    DoFn<String, Integer> lenFn =
        new DoFn<String, Integer>() {
          @ProcessElement
          public void process(@Element String s, OutputReceiver<Integer> out) {
            out.output(s.length());
          }
        };
    ProcessFunction<Integer, Boolean> filterPred = n -> n > 0;
    List<WindowedValue<?>> outList = new ArrayList<>();

    CompiledNode head =
        FusedSegmentCompiler.compileChain(
            Arrays.asList(
                new DoFnStepSpec<>(
                    lenFn,
                    DoFnInvokers.invokerFor(lenFn),
                    null,
                    (StepContextFallback) testFallback(),
                    false,
                    false),
                new FunctionalStepSpec(
                    InlineableFunctionProvider.Kind.FILTER, filterPred, null, false, false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {
                outList.add(elem);
              }
            });

    Context activeSpan = Context.root().with(OTEL_KEY, "ambient-span");
    try (Scope ignored = activeSpan.makeCurrent()) {
      head.acceptWindowed(WindowedValues.valueInGlobalWindow("hello"));
    }
    assertEquals(1, outList.size());
    assertEquals(Integer.valueOf(5), outList.get(0).getValue());
    assertEquals("ambient-span", outList.get(0).getOpenTelemetryContext().get(OTEL_KEY));
  }

  @Test
  public void testOtelDetachToRootAndIncomingRootMaskingAmbientSpan() throws Exception {
    AtomicReference<String> seenInStep2 = new AtomicReference<>("unset");
    DoFn<String, String> detachDoFn =
        new DoFn<String, String>() {
          @ProcessElement
          public void process(@Element String s, OutputReceiver<String> out) {
            assertEquals("trace0", Context.current().get(OTEL_KEY));
            try (Scope ignored = Context.root().makeCurrent()) {
              out.output(s + "-detached");
            }
          }
        };
    DoFn<String, String> observerDoFn =
        new DoFn<String, String>() {
          @ProcessElement
          public void process(@Element String s, OutputReceiver<String> out) {
            seenInStep2.set(Context.current().get(OTEL_KEY));
            out.output(s);
          }
        };
    List<WindowedValue<?>> outList = new ArrayList<>();
    CompiledNode head =
        FusedSegmentCompiler.compileChain(
            Arrays.asList(
                new DoFnStepSpec<>(
                    detachDoFn,
                    DoFnInvokers.invokerFor(detachDoFn),
                    null,
                    testFallback(),
                    false,
                    false),
                new DoFnStepSpec<>(
                    observerDoFn,
                    DoFnInvokers.invokerFor(observerDoFn),
                    null,
                    testFallback(),
                    false,
                    false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {
                outList.add(elem);
              }
            });

    Context incoming = Context.root().with(OTEL_KEY, "trace0");
    WindowedValue<String> wv =
        WindowedValues.of(
            "v",
            Instant.EPOCH,
            GlobalWindow.INSTANCE,
            PaneInfo.NO_FIRING,
            null,
            null,
            CausedByDrain.NORMAL,
            incoming,
            ValueKind.INSERT);
    head.acceptWindowed(wv);
    assertNull(seenInStep2.get());
    assertEquals(1, outList.size());
    assertNull(outList.get(0).getOpenTelemetryContext().get(OTEL_KEY));

    // Also verify an incoming WindowedValue explicitly carrying Context.root() masks an ambient
    // span
    seenInStep2.set("unset");
    CompiledNode singleObserver =
        FusedSegmentCompiler.compileChain(
            Collections.singletonList(
                new DoFnStepSpec<>(
                    observerDoFn,
                    DoFnInvokers.invokerFor(observerDoFn),
                    null,
                    testFallback(),
                    false,
                    false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {}
            });
    WindowedValue<String> explicitRoot =
        WindowedValues.of(
            "v2",
            Instant.EPOCH,
            GlobalWindow.INSTANCE,
            PaneInfo.NO_FIRING,
            null,
            null,
            CausedByDrain.NORMAL,
            Context.root(),
            ValueKind.INSERT);
    try (Scope ignored = Context.root().with(OTEL_KEY, "ambient-leak").makeCurrent()) {
      singleObserver.acceptWindowed(explicitRoot);
    }
    assertNull(seenInStep2.get());
  }

  @Test
  public void testRowReceiverIsNotBypassedAndMultiOutputReceiverIsLazy() throws Exception {
    RowOutputDoFn rowFn = new RowOutputDoFn();
    CompiledNode rowNode =
        FusedSegmentCompiler.compileChain(
            Collections.singletonList(
                new DoFnStepSpec<>(
                    rowFn, DoFnInvokers.invokerFor(rowFn), null, testFallback(), false, false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {}
            });
    UserCodeException rowEx =
        assertThrows(
            UserCodeException.class,
            () -> rowNode.acceptWindowed(WindowedValues.valueInGlobalWindow("x")));
    assertTrue(rowEx.getCause() instanceof IllegalStateException);

    LazyMultiOutputDoFn multiFn = new LazyMultiOutputDoFn();
    List<WindowedValue<?>> outList = new ArrayList<>();
    CompiledNode multiNode =
        FusedSegmentCompiler.compileChain(
            Collections.singletonList(
                new DoFnStepSpec<>(
                    multiFn, DoFnInvokers.invokerFor(multiFn), null, testFallback(), false, false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {
                outList.add(elem);
              }
            });
    multiNode.acceptWindowed(WindowedValues.valueInGlobalWindow("hello"));
    assertEquals(1, outList.size());
    assertEquals("ok:hello", outList.get(0).getValue());
  }

  @Test
  public void testDynamicProxyFunctionCompilesWithoutModuleIllegalAccessError() throws Exception {
    ProcessFunction<String, String> proxyFn =
        (ProcessFunction<String, String>)
            Proxy.newProxyInstance(
                ProcessFunction.class.getClassLoader(),
                new Class<?>[] {ProcessFunction.class},
                (proxy, method, args) -> {
                  if ("apply".equals(method.getName())) {
                    return args[0] + "-proxied";
                  }
                  return method.invoke(this, args);
                });
    List<WindowedValue<?>> outList = new ArrayList<>();
    CompiledNode node =
        FusedSegmentCompiler.compileChain(
            Collections.singletonList(
                new FunctionalStepSpec(
                    InlineableFunctionProvider.Kind.MAP, proxyFn, null, false, false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {
                outList.add(elem);
              }
            });
    node.acceptWindowed(WindowedValues.valueInGlobalWindow("abc"));
    assertEquals(1, outList.size());
    assertEquals("abc-proxied", outList.get(0).getValue());
  }

  @Test
  public void testSystemErrorNotDemotedAndDownstreamCheckedExceptionNotDoubleWrapped() {
    SystemErrorDoFn sysFn = new SystemErrorDoFn();
    CompiledNode sysNode =
        FusedSegmentCompiler.compileChain(
            Collections.singletonList(
                new DoFnStepSpec<>(
                    sysFn, DoFnInvokers.invokerFor(sysFn), null, testFallback(), true, false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {}
            });
    AssertionError sysErr =
        assertThrows(
            AssertionError.class,
            () -> sysNode.acceptWindowed(WindowedValues.valueInGlobalWindow("boom")));
    assertEquals("system-error:boom", sysErr.getMessage());

    EmitWithTimestampDoFn tsFn = new EmitWithTimestampDoFn();
    CompiledNode tsNode =
        FusedSegmentCompiler.compileChain(
            Collections.singletonList(
                new DoFnStepSpec<>(
                    tsFn, DoFnInvokers.invokerFor(tsFn), null, testFallback(), false, true)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) throws Exception {
                throw new IOException("downstream-io");
              }
            });
    UserCodeException ex =
        assertThrows(
            UserCodeException.class,
            () -> tsNode.acceptWindowed(WindowedValues.valueInGlobalWindow("v")));
    assertTrue(
        "Expected direct IOException cause, got " + ex.getCause(),
        ex.getCause() instanceof IOException);
  }

  @Test
  public void testDoFnNodeClearsTemplateElemAfterAcceptRawAndRejectsOutOfContextOutput()
      throws Exception {
    RetainingOutputReceiverDoFn retainFn = new RetainingOutputReceiverDoFn();
    List<WindowedValue<?>> outList = new ArrayList<>();
    CompiledNode node =
        FusedSegmentCompiler.compileChain(
            Collections.singletonList(
                new DoFnStepSpec<>(
                    retainFn,
                    DoFnInvokers.invokerFor(retainFn),
                    null,
                    testFallback(),
                    false,
                    false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {
                outList.add(elem);
              }
            });
    FusedDoFnNodeBase<?, ?> base = (FusedDoFnNodeBase<?, ?>) node;
    assertThrows(
        IllegalStateException.class, () -> ((DoFn.OutputReceiver<String>) base).output("x"));

    node.acceptWindowed(WindowedValues.valueInGlobalWindow("payload"));
    assertEquals(1, outList.size());
    assertNull(base.templateElem);
    assertNull(base.currentVal);
    assertThrows(IllegalStateException.class, () -> retainFn.saved.get().output("stale"));
    assertThrows(
        IllegalStateException.class,
        () ->
            ((FusedDoFnNodeBase<String, String>) base)
                .outputWindowedValue(WindowedValues.valueInGlobalWindow("stale-wv")));
    assertThrows(
        IllegalStateException.class,
        () ->
            ((FusedDoFnNodeBase<String, String>) base)
                .output(WindowedValues.valueInGlobalWindow("stale-wv2")));
    assertFalse(FusedSegmentCompiler.hasGeneratedMethodHandleField(node));
  }

  public static class BaseChildLoaderFunc implements ProcessFunction<String, String> {
    @Override
    public String apply(String input) {
      return input + "-child";
    }
  }

  private static java.lang.ref.WeakReference<ClassLoader> compileFuncInEphemeralClassLoader(
      String generatedClassName) throws Exception {
    ClassLoader childLoader = new ClassLoader(FusedSegmentCompilerTest.class.getClassLoader()) {};
    Class<? extends BaseChildLoaderFunc> dynamicFuncClass =
        new net.bytebuddy.ByteBuddy()
            .subclass(BaseChildLoaderFunc.class)
            .name(generatedClassName)
            .make()
            .load(childLoader, net.bytebuddy.dynamic.loading.ClassLoadingStrategy.Default.INJECTION)
            .getLoaded();
    ProcessFunction<String, String> dynamicFn =
        dynamicFuncClass.getDeclaredConstructor().newInstance();
    List<WindowedValue<?>> outList = new ArrayList<>();
    CompiledNode node =
        FusedSegmentCompiler.compileChain(
            Collections.singletonList(
                new FunctionalStepSpec(
                    InlineableFunctionProvider.Kind.MAP, dynamicFn, null, false, false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {
                outList.add(elem);
              }
            });
    node.acceptWindowed(WindowedValues.valueInGlobalWindow("v"));
    assertEquals(1, outList.size());
    assertEquals("v-child", outList.get(0).getValue());
    return new java.lang.ref.WeakReference<>(childLoader);
  }

  @Test
  public void testDynamicClassLoaderNotPinnedBySegmentCompilerCaches() throws Exception {
    // 1. Named package class -> UsingLookup injection path (LOADER_NAMESPACES + injectedClasses)
    java.lang.ref.WeakReference<ClassLoader> lookupLoaderRef =
        compileFuncInEphemeralClassLoader(
            "org.apache.beam.sdk.transforms.reflect.EphemeralFunc$GcLookupTest");
    // 2. Default package class -> SegmentClassLoader + ANCHOR_FALLBACK_CACHE path
    java.lang.ref.WeakReference<ClassLoader> fallbackLoaderRef =
        compileFuncInEphemeralClassLoader("EphemeralDefaultPkgFunc$GcFallbackTest");

    for (int i = 0;
        i < 40 && (lookupLoaderRef.get() != null || fallbackLoaderRef.get() != null);
        i++) {
      System.gc();
      Thread.sleep(25);
    }
    assertNull(
        "Dynamic child ClassLoader (UsingLookup path) must be GC'd once unreferenced",
        lookupLoaderRef.get());
    assertNull(
        "Dynamic child ClassLoader (SegmentClassLoader fallback path) must be GC'd once unreferenced",
        fallbackLoaderRef.get());
  }

  public static final class MultiOutElementValueCaptureDoFn extends DoFn<String, String> {
    static final TupleTag<String> SIDE_TAG = new TupleTag<>("side-capture");

    @ProcessElement
    public void process(@Element String e, MultiOutputReceiver out) {
      out.get(SIDE_TAG).output("tagged:" + e);
    }
  }

  @Test
  public void testMultiOutputDirectPlanPopulatesCurrentValForStepContextFallback()
      throws Exception {
    AtomicReference<String> seenFallbackInputVal = new AtomicReference<>();
    AtomicReference<String> seenTaggedOutput = new AtomicReference<>();
    StepContextFallback<String, String> capturingFallback =
        new StepContextFallback<String, String>() {
          @Override
          public PipelineOptions pipelineOptions() {
            return PipelineOptionsFactory.create();
          }

          @Override
          public BundleFinalizer bundleFinalizer() {
            throw new UnsupportedOperationException();
          }

          @Override
          public void checkTimestamp(WindowedValue<String> elem, Instant timestamp) {}

          @Override
          public boolean isMainOutputTag(TupleTag<?> tag) {
            return MAIN_TAG.equals(tag);
          }

          @Override
          public <T> void outputTagged(WindowedValue<String> elem, TupleTag<T> tag, T output) {
            seenFallbackInputVal.set(elem.getValue());
            seenTaggedOutput.set((String) output);
          }

          @Override
          public <T> void outputTaggedWithTimestamp(
              WindowedValue<String> elem, TupleTag<T> tag, T output, Instant timestamp) {}

          @Override
          public <T> void outputTaggedWindowedValue(
              WindowedValue<String> elem,
              TupleTag<T> tag,
              T output,
              Instant timestamp,
              Collection<? extends BoundedWindow> windows,
              PaneInfo paneInfo) {}

          @Override
          public <T> void outputTaggedWindowedValue(
              WindowedValue<String> elem, TupleTag<T> tag, WindowedValue<T> windowedValue) {}

          @Override
          public Object schemaElement(WindowedValue<String> elem, int index) {
            throw new UnsupportedOperationException();
          }
        };

    MultiOutElementValueCaptureDoFn fn = new MultiOutElementValueCaptureDoFn();
    CompiledNode node =
        FusedSegmentCompiler.compileChain(
            Collections.singletonList(
                new DoFnStepSpec<>(
                    fn, DoFnInvokers.invokerFor(fn), null, capturingFallback, false, false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {}
            });
    node.acceptWindowed(WindowedValues.valueInGlobalWindow("hello"));
    assertEquals("hello", seenFallbackInputVal.get());
    assertEquals("tagged:hello", seenTaggedOutput.get());
  }

  private static java.lang.ref.WeakReference<ClassLoader> compileMixedAppDoFnIntoChildLoaderAndRun(
      int iter) throws Exception {
    ClassLoader childLoader = new ClassLoader(FusedSegmentCompilerTest.class.getClassLoader()) {};
    Class<? extends BaseChildLoaderFunc> dynamicFuncClass =
        new net.bytebuddy.ByteBuddy()
            .subclass(BaseChildLoaderFunc.class)
            .name("org.apache.beam.sdk.transforms.reflect.EphemeralChainFunc$" + iter)
            .make()
            .load(childLoader, net.bytebuddy.dynamic.loading.ClassLoadingStrategy.Default.INJECTION)
            .getLoaded();
    ProcessFunction<String, String> dynamicFn =
        dynamicFuncClass.getDeclaredConstructor().newInstance();
    PublicDirectDoFn appDoFn = new PublicDirectDoFn();
    List<WindowedValue<?>> outList = new ArrayList<>();
    CompiledNode node =
        FusedSegmentCompiler.compileChain(
            Arrays.asList(
                new DoFnStepSpec<>(
                    appDoFn, DoFnInvokers.invokerFor(appDoFn), null, testFallback(), false, false),
                new FunctionalStepSpec(
                    InlineableFunctionProvider.Kind.MAP, dynamicFn, null, false, false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {
                outList.add(elem);
              }
            });
    node.acceptWindowed(WindowedValues.timestampedValueInGlobalWindow("x", Instant.EPOCH));
    assertEquals(1, outList.size());
    assertEquals("x@0-child", outList.get(0).getValue());
    return new java.lang.ref.WeakReference<>(childLoader);
  }

  @Test
  public void testEphemeralChildDownstreamDoesNotLeakImmortalClassesIntoBootstrapLoader()
      throws Exception {
    // First iteration may inject the single AppClassLoader wrapper for (PublicDirectDoFn ->
    // CompiledNode)
    java.lang.ref.WeakReference<ClassLoader> ref0 = compileMixedAppDoFnIntoChildLoaderAndRun(0);
    int bootstrapCountAfterFirst = FusedSegmentCompiler.bootstrapInjectedClassCount();
    List<java.lang.ref.WeakReference<ClassLoader>> refs = new ArrayList<>();
    refs.add(ref0);
    for (int i = 1; i <= 8; i++) {
      refs.add(compileMixedAppDoFnIntoChildLoaderAndRun(i));
    }
    assertEquals(
        "Repeated ephemeral child-loader downstream steps must NOT inject new immortal classes into AppClassLoader",
        bootstrapCountAfterFirst,
        FusedSegmentCompiler.bootstrapInjectedClassCount());
    for (int attempt = 0; attempt < 40; attempt++) {
      boolean allCleared = true;
      for (java.lang.ref.WeakReference<ClassLoader> r : refs) {
        if (r.get() != null) {
          allCleared = false;
          break;
        }
      }
      if (allCleared) {
        break;
      }
      System.gc();
      Thread.sleep(25);
    }
    for (java.lang.ref.WeakReference<ClassLoader> r : refs) {
      assertNull("Ephemeral child ClassLoader must be GC'd", r.get());
    }
  }

  private static java.lang.ref.WeakReference<ClassLoader> compileInNonDescendantPluginLoaderAndRun()
      throws Exception {
    ClassLoader appLoader = FusedSegmentCompilerTest.class.getClassLoader();
    // Non-descendant ClassLoader whose parent is PlatformClassLoader (NOT appLoader),
    // delegating Beam/Joda/test-base classes explicitly (simulating OSGi / isolated plugin loaders)
    ClassLoader pluginLoader =
        new ClassLoader(ClassLoader.getPlatformClassLoader()) {
          @Override
          protected Class<?> findClass(String name) throws ClassNotFoundException {
            if (name.startsWith("org.apache.beam.") || name.startsWith("org.joda.")) {
              return appLoader.loadClass(name);
            }
            throw new ClassNotFoundException(name);
          }
        };
    Class<? extends BaseChildLoaderFunc> pluginFuncClass =
        new net.bytebuddy.ByteBuddy()
            .subclass(BaseChildLoaderFunc.class)
            .name("PluginDefaultPkgFunc$NonDescendantGcTest")
            .make()
            .load(
                pluginLoader, net.bytebuddy.dynamic.loading.ClassLoadingStrategy.Default.INJECTION)
            .getLoaded();
    ProcessFunction<String, String> pluginFn =
        pluginFuncClass.getDeclaredConstructor().newInstance();
    List<WindowedValue<?>> outList = new ArrayList<>();
    CompiledNode node =
        FusedSegmentCompiler.compileChain(
            Collections.singletonList(
                new FunctionalStepSpec(
                    InlineableFunctionProvider.Kind.MAP, pluginFn, null, false, false)),
            new TailReceiver() {
              @Override
              public void accept(WindowedValue<?> elem) {
                outList.add(elem);
              }
            });
    node.acceptWindowed(WindowedValues.valueInGlobalWindow("plug"));
    assertEquals(1, outList.size());
    assertEquals("plug-child", outList.get(0).getValue());
    return new java.lang.ref.WeakReference<>(pluginLoader);
  }

  @Test
  public void testNonDescendantDynamicClassLoaderNotPinnedByBootstrapFallbackCache()
      throws Exception {
    java.lang.ref.WeakReference<ClassLoader> pluginRef = compileInNonDescendantPluginLoaderAndRun();
    for (int i = 0; i < 40 && pluginRef.get() != null; i++) {
      System.gc();
      Thread.sleep(25);
    }
    assertNull(
        "Non-descendant dynamic ClassLoader must not be pinned in bootstrapFallbackCache",
        pluginRef.get());
  }
}
