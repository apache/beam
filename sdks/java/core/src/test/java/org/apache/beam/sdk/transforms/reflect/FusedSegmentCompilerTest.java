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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.InlineableFunctionProvider;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.CompiledNode;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.DoFnStepSpec;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.FunctionalStepSpec;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.StepContextFallback;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.StepHook;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.StepSpec;
import org.apache.beam.sdk.transforms.reflect.FusedSegmentCompiler.TailReceiver;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.CausedByDrain;
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
}
