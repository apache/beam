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

import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.resume;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.stop;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker.FakeArgumentProvider;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerDeclaration;
import org.apache.beam.sdk.transforms.reflect.testhelper.DoFnInvokersTestHelper;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.Progress;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.TruncateResult;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.UserCodeException;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.AdditionalAnswers;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link DoFnInvokers}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "SameNameButDifferent",
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
public class DoFnInvokersTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private DoFn<String, String>.StartBundleContext mockStartBundleContext;
  @Mock private DoFn<String, String>.FinishBundleContext mockFinishBundleContext;
  @Mock private DoFn<String, String>.ProcessContext mockProcessContext;
  private String mockElement;
  private Instant mockTimestamp;
  @Mock private OutputReceiver<String> mockOutputReceiver;
  @Mock private MultiOutputReceiver mockMultiOutputReceiver;
  @Mock private IntervalWindow mockWindow;
  // @Mock private PaneInfo mockPaneInfo;
  @Mock private DoFnInvoker.ArgumentProvider<String, String> mockArgumentProvider;
  @Mock private PipelineOptions mockOptions;

  @Before
  public void setUp() {
    mockElement = "element";
    mockTimestamp = new Instant(0);
    MockitoAnnotations.initMocks(this);
    when(mockArgumentProvider.window()).thenReturn(mockWindow);
    // when(mockArgumentProvider.paneInfo(Matchers.<DoFn>any()))
    //     .thenReturn(mockPaneInfo);
    when(mockArgumentProvider.element(Matchers.<DoFn>any())).thenReturn(mockElement);
    when(mockArgumentProvider.timestamp(Matchers.<DoFn>any())).thenReturn(mockTimestamp);
    when(mockArgumentProvider.outputReceiver(Matchers.<DoFn>any())).thenReturn(mockOutputReceiver);
    when(mockArgumentProvider.taggedOutputReceiver(Matchers.<DoFn>any()))
        .thenReturn(mockMultiOutputReceiver);
    when(mockArgumentProvider.pipelineOptions()).thenReturn(mockOptions);
    when(mockArgumentProvider.startBundleContext(Matchers.<DoFn>any()))
        .thenReturn(mockStartBundleContext);
    when(mockArgumentProvider.finishBundleContext(Matchers.<DoFn>any()))
        .thenReturn(mockFinishBundleContext);
    when(mockArgumentProvider.processContext(Matchers.<DoFn>any())).thenReturn(mockProcessContext);
  }

  private DoFn.ProcessContinuation invokeProcessElement(DoFn<String, String> fn) {
    return DoFnInvokers.invokerFor(fn).invokeProcessElement(mockArgumentProvider);
  }

  private void invokeOnTimer(String timerId, DoFn<String, String> fn) {
    DoFnInvokers.invokerFor(fn)
        .invokeOnTimer(TimerDeclaration.PREFIX + timerId, "", mockArgumentProvider);
  }

  @Test
  public void testDoFnInvokersReused() throws Exception {
    // Ensures that we don't create a new Invoker class for every instance of the DoFn.
    IdentityParent fn1 = new IdentityParent();
    IdentityParent fn2 = new IdentityParent();
    assertSame(
        "Invoker classes should only be generated once for each type",
        DoFnInvokers.invokerFor(fn1).getClass(),
        DoFnInvokers.invokerFor(fn2).getClass());
  }

  // ---------------------------------------------------------------------------------------
  // Tests for general invocations of DoFn methods.
  // ---------------------------------------------------------------------------------------

  @Test
  public void testDoFnWithNoExtraContext() throws Exception {
    class MockFn extends DoFn<String, String> {
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {}
    }

    MockFn mockFn = mock(MockFn.class);
    assertEquals(stop(), invokeProcessElement(mockFn));
    verify(mockFn).processElement(mockProcessContext);
  }

  interface InterfaceWithProcessElement {
    @DoFn.ProcessElement
    void processElement(DoFn<String, String>.ProcessContext c);
  }

  interface LayersOfInterfaces extends InterfaceWithProcessElement {}

  private static class IdentityUsingInterfaceWithProcessElement extends DoFn<String, String>
      implements LayersOfInterfaces {
    @Override
    public void processElement(DoFn<String, String>.ProcessContext c) {}
  }

  @Test
  public void testDoFnWithProcessElementInterface() throws Exception {
    IdentityUsingInterfaceWithProcessElement fn =
        mock(IdentityUsingInterfaceWithProcessElement.class);
    assertEquals(stop(), invokeProcessElement(fn));
    verify(fn).processElement(mockProcessContext);
  }

  private static class IdentityParent extends DoFn<String, String> {
    @ProcessElement
    public void process(ProcessContext c) {}
  }

  @SuppressWarnings("ClassCanBeStatic")
  private class IdentityChildWithoutOverride extends IdentityParent {}

  @SuppressWarnings("ClassCanBeStatic")
  private class IdentityChildWithOverride extends IdentityParent {
    @Override
    public void process(DoFn<String, String>.ProcessContext c) {
      super.process(c);
    }
  }

  @Test
  public void testDoFnWithMethodInSuperclass() throws Exception {
    IdentityChildWithoutOverride fn = mock(IdentityChildWithoutOverride.class);
    assertEquals(stop(), invokeProcessElement(fn));
    verify(fn).process(mockProcessContext);
  }

  @Test
  public void testDoFnWithMethodInSubclass() throws Exception {
    IdentityChildWithOverride fn = mock(IdentityChildWithOverride.class);
    assertEquals(stop(), invokeProcessElement(fn));
    verify(fn).process(mockProcessContext);
  }

  @Test
  public void testDoFnWithWindow() throws Exception {
    class MockFn extends DoFn<String, String> {
      @DoFn.ProcessElement
      public void processElement(ProcessContext c, IntervalWindow w) throws Exception {}
    }

    MockFn fn = mock(MockFn.class);
    assertEquals(stop(), invokeProcessElement(fn));
    verify(fn).processElement(mockProcessContext, mockWindow);
  }

  @Test
  public void testDoFnWithAllParameters() throws Exception {
    class MockFn extends DoFn<String, String> {
      @DoFn.ProcessElement
      public void processElement(
          ProcessContext c,
          @Element String element,
          @Timestamp Instant timestamp,
          IntervalWindow w,
          //        PaneInfo p,
          OutputReceiver<String> receiver,
          MultiOutputReceiver multiReceiver)
          throws Exception {}
    }

    MockFn fn = mock(MockFn.class);
    assertEquals(stop(), invokeProcessElement(fn));
    verify(fn)
        .processElement(
            mockProcessContext,
            mockElement,
            mockTimestamp,
            mockWindow,
            mockOutputReceiver,
            mockMultiOutputReceiver);
  }

  /** Tests that the generated {@link DoFnInvoker} passes the state parameter that it should. */
  @Test
  public void testDoFnWithState() throws Exception {
    ValueState<Integer> mockState = mock(ValueState.class);
    final String stateId = "my-state-id-here";
    when(mockArgumentProvider.state(stateId, false)).thenReturn(mockState);

    class MockFn extends DoFn<String, String> {

      @StateId(stateId)
      private final StateSpec<ValueState<Integer>> spec = StateSpecs.value(VarIntCoder.of());

      @ProcessElement
      public void processElement(ProcessContext c, @StateId(stateId) ValueState<Integer> valueState)
          throws Exception {}
    }

    MockFn fn = mock(MockFn.class);
    assertEquals(stop(), invokeProcessElement(fn));
    verify(fn).processElement(mockProcessContext, mockState);
  }

  /** Tests that the generated {@link DoFnInvoker} passes the timer parameter that it should. */
  @Test
  public void testDoFnWithTimer() throws Exception {
    Timer mockTimer = mock(Timer.class);
    final String timerId = "my-timer-id-here";
    when(mockArgumentProvider.timer(TimerDeclaration.PREFIX + timerId)).thenReturn(mockTimer);

    class MockFn extends DoFn<String, String> {

      @TimerId(timerId)
      private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

      @ProcessElement
      public void processElement(ProcessContext c, @TimerId(timerId) Timer timer)
          throws Exception {}

      @OnTimer(timerId)
      public void onTimer() {}
    }

    MockFn fn = mock(MockFn.class);
    assertEquals(stop(), invokeProcessElement(fn));
    verify(fn).processElement(mockProcessContext, mockTimer);
  }

  @Test
  public void testOnWindowExpirationWithNoParam() throws Exception {
    class MockFn extends DoFn<String, String> {

      @ProcessElement
      public void process(ProcessContext c) {}

      @OnWindowExpiration
      public void onWindowExpiration() {}
    }

    MockFn fn = mock(MockFn.class);
    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(fn);

    invoker.invokeOnWindowExpiration(mockArgumentProvider);
    verify(fn).onWindowExpiration();
  }

  @Test
  public void testOnWindowExpirationWithParam() {
    class MockFn extends DoFn<String, String> {

      @ProcessElement
      public void process(ProcessContext c) {}

      @OnWindowExpiration
      public void onWindowExpiration(BoundedWindow window) {}
    }

    MockFn fn = mock(MockFn.class);
    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(fn);

    invoker.invokeOnWindowExpiration(mockArgumentProvider);
    verify(fn).onWindowExpiration(mockWindow);
  }

  @Test
  public void testDoFnWithReturn() throws Exception {
    class MockFn extends DoFn<String, String> {
      @DoFn.ProcessElement
      public ProcessContinuation processElement(
          ProcessContext c, RestrictionTracker<SomeRestriction, Void> tracker) throws Exception {
        return null;
      }

      @GetInitialRestriction
      public SomeRestriction getInitialRestriction(@Element String element) {
        return null;
      }

      @NewTracker
      public SomeRestrictionTracker newTracker(@Restriction SomeRestriction restriction) {
        return null;
      }
    }

    MockFn fn = mock(MockFn.class);
    when(fn.processElement(mockProcessContext, null)).thenReturn(resume());
    assertEquals(resume(), invokeProcessElement(fn));
  }

  @Test
  public void testDoFnWithStartBundleSetupTeardown() throws Exception {
    when(mockArgumentProvider.pipelineOptions()).thenReturn(mockOptions);
    when(mockArgumentProvider.startBundleContext(any(DoFn.class)))
        .thenReturn(mockStartBundleContext);
    when(mockArgumentProvider.finishBundleContext(any(DoFn.class)))
        .thenReturn(mockFinishBundleContext);
    class MockFn extends DoFn<String, String> {
      @ProcessElement
      public void processElement(ProcessContext c) {}

      @StartBundle
      public void startBundle(StartBundleContext c) {}

      @FinishBundle
      public void finishBundle(FinishBundleContext c) {}

      @Setup
      public void before(PipelineOptions options) {}

      @Teardown
      public void after() {}
    }

    MockFn fn = mock(MockFn.class);

    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(fn);
    invoker.invokeSetup(mockArgumentProvider);
    invoker.invokeStartBundle(mockArgumentProvider);
    invoker.invokeFinishBundle(mockArgumentProvider);
    invoker.invokeTeardown();
    verify(fn).before(mockOptions);
    verify(fn).startBundle(mockStartBundleContext);
    verify(fn).finishBundle(mockFinishBundleContext);
    verify(fn).after();
  }

  // ---------------------------------------------------------------------------------------
  // Tests for invoking Splittable DoFn methods
  // ---------------------------------------------------------------------------------------
  private static class SomeRestriction {}

  private abstract static class SomeRestrictionTracker
      extends RestrictionTracker<SomeRestriction, Void> {}

  private static class SomeRestrictionCoder extends AtomicCoder<SomeRestriction> {
    public static SomeRestrictionCoder of() {
      return new SomeRestrictionCoder();
    }

    @Override
    public void encode(SomeRestriction value, OutputStream outStream) {}

    @Override
    public SomeRestriction decode(InputStream inStream) {
      return null;
    }
  }

  /** Public so Mockito can do "delegatesTo()" in the test below. */
  public static class MockFn extends DoFn<String, String> {
    @ProcessElement
    public ProcessContinuation processElement(
        ProcessContext c,
        RestrictionTracker<SomeRestriction, Void> tracker,
        WatermarkEstimator<Instant> watermarkEstimator) {
      return null;
    }

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(@Element String element) {
      return null;
    }

    @SplitRestriction
    public void splitRestriction(
        @Element String element,
        @Restriction SomeRestriction restriction,
        OutputReceiver<SomeRestriction> receiver) {}

    @NewTracker
    public SomeRestrictionTracker newTracker(@Restriction SomeRestriction restriction) {
      return null;
    }

    @GetSize
    public double getSize() {
      return 2.0;
    }

    @GetRestrictionCoder
    public SomeRestrictionCoder getRestrictionCoder() {
      return null;
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState() {
      return null;
    }

    @GetWatermarkEstimatorStateCoder
    public InstantCoder getWatermarkEstimatorStateCoder() {
      return null;
    }

    @NewWatermarkEstimator
    public WatermarkEstimator<Instant> newWatermarkEstimator(
        @WatermarkEstimatorState Instant watermarkEstimatorState) {
      return null;
    }
  }

  @Test
  public void testSplittableDoFnWithAllMethods() throws Exception {
    MockFn fn = mock(MockFn.class);
    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(fn);
    final SomeRestrictionTracker tracker = mock(SomeRestrictionTracker.class);
    final SomeRestrictionCoder coder = mock(SomeRestrictionCoder.class);
    final InstantCoder watermarkEstimatorStateCoder = InstantCoder.of();
    final Instant watermarkEstimatorState = Instant.now();
    final WatermarkEstimator<Instant> watermarkEstimator =
        new WatermarkEstimators.Manual(watermarkEstimatorState);
    SomeRestriction restriction = new SomeRestriction();
    final SomeRestriction part1 = new SomeRestriction();
    final SomeRestriction part2 = new SomeRestriction();
    final SomeRestriction part3 = new SomeRestriction();
    when(fn.getRestrictionCoder()).thenReturn(coder);
    when(fn.getWatermarkEstimatorStateCoder()).thenReturn(watermarkEstimatorStateCoder);
    when(fn.getInitialRestriction(mockElement)).thenReturn(restriction);
    doAnswer(
            AdditionalAnswers.delegatesTo(
                new MockFn() {
                  @DoFn.SplitRestriction
                  @Override
                  public void splitRestriction(
                      @Element String element,
                      @Restriction SomeRestriction restriction,
                      DoFn.OutputReceiver<SomeRestriction> receiver) {
                    receiver.output(part1);
                    receiver.output(part2);
                    receiver.output(part3);
                  }
                }))
        .when(fn)
        .splitRestriction(eq(mockElement), same(restriction), any());
    when(fn.getInitialWatermarkEstimatorState()).thenReturn(watermarkEstimatorState);
    when(fn.newTracker(restriction)).thenReturn(tracker);
    when(fn.newWatermarkEstimator(watermarkEstimatorState)).thenReturn(watermarkEstimator);
    when(fn.processElement(mockProcessContext, tracker, watermarkEstimator)).thenReturn(resume());
    when(fn.getSize()).thenReturn(2.0);

    assertEquals(coder, invoker.invokeGetRestrictionCoder(CoderRegistry.createDefault()));
    assertEquals(
        watermarkEstimatorStateCoder,
        invoker.invokeGetWatermarkEstimatorStateCoder(CoderRegistry.createDefault()));
    assertEquals(
        restriction,
        invoker.invokeGetInitialRestriction(
            new FakeArgumentProvider<String, String>() {
              @Override
              public String element(DoFn<String, String> doFn) {
                return mockElement;
              }
            }));
    List<SomeRestriction> outputs = new ArrayList<>();
    invoker.invokeSplitRestriction(
        new FakeArgumentProvider<String, String>() {
          @Override
          public String element(DoFn<String, String> doFn) {
            return mockElement;
          }

          @Override
          public Object restriction() {
            return restriction;
          }

          @Override
          public OutputReceiver outputReceiver(DoFn doFn) {
            return new OutputReceiver<SomeRestriction>() {
              @Override
              public void output(SomeRestriction output) {
                outputs.add(output);
              }

              @Override
              public void outputWithTimestamp(SomeRestriction output, Instant timestamp) {
                fail("Unexpected output with timestamp");
              }

              @Override
              public void outputWindowedValue(
                  SomeRestriction output,
                  Instant timestamp,
                  Collection<? extends BoundedWindow> windows,
                  PaneInfo paneInfo) {
                fail("Unexpected outputWindowedValue");
              }
            };
          }
        });

    assertEquals(Arrays.asList(part1, part2, part3), outputs);
    assertEquals(
        watermarkEstimatorState,
        invoker.invokeGetInitialWatermarkEstimatorState(new FakeArgumentProvider<>()));
    assertEquals(
        tracker,
        invoker.invokeNewTracker(
            new FakeArgumentProvider<String, String>() {
              @Override
              public String element(DoFn<String, String> doFn) {
                return mockElement;
              }

              @Override
              public Object restriction() {
                return restriction;
              }
            }));
    assertEquals(
        watermarkEstimator,
        invoker.invokeNewWatermarkEstimator(
            new FakeArgumentProvider<String, String>() {
              @Override
              public Object watermarkEstimatorState() {
                return watermarkEstimatorState;
              }
            }));
    assertEquals(
        resume(),
        invoker.invokeProcessElement(
            new FakeArgumentProvider<String, String>() {
              @Override
              public DoFn<String, String>.ProcessContext processContext(DoFn<String, String> fn) {
                return mockProcessContext;
              }

              @Override
              public RestrictionTracker<?, ?> restrictionTracker() {
                return tracker;
              }

              @Override
              public WatermarkEstimator<?> watermarkEstimator() {
                return watermarkEstimator;
              }
            }));
    assertEquals(2.0, invoker.invokeGetSize(mockArgumentProvider), 0.0001);
  }

  private static class RestrictionWithBoundedDefaultTracker
      implements HasDefaultTracker<RestrictionWithBoundedDefaultTracker, BoundedDefaultTracker> {
    @Override
    public BoundedDefaultTracker newTracker() {
      return new BoundedDefaultTracker();
    }
  }

  private static class RestrictionWithUnboundedDefaultTracker
      implements HasDefaultTracker<
          RestrictionWithUnboundedDefaultTracker, UnboundedDefaultTracker> {
    @Override
    public UnboundedDefaultTracker newTracker() {
      return new UnboundedDefaultTracker();
    }
  }

  private static class BoundedDefaultTracker
      extends RestrictionTracker<RestrictionWithBoundedDefaultTracker, Void> {
    @Override
    public boolean tryClaim(Void position) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RestrictionWithBoundedDefaultTracker currentRestriction() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SplitResult<RestrictionWithBoundedDefaultTracker> trySplit(double fractionOfRemainder) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void checkDone() throws IllegalStateException {}

    @Override
    public IsBounded isBounded() {
      return IsBounded.BOUNDED;
    }
  }

  private static class UnboundedDefaultTracker
      extends RestrictionTracker<RestrictionWithUnboundedDefaultTracker, Void> {
    @Override
    public boolean tryClaim(Void position) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RestrictionWithUnboundedDefaultTracker currentRestriction() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SplitResult<RestrictionWithUnboundedDefaultTracker> trySplit(
        double fractionOfRemainder) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void checkDone() throws IllegalStateException {}

    @Override
    public IsBounded isBounded() {
      return IsBounded.UNBOUNDED;
    }
  }

  private static class CoderForDefaultTracker
      extends AtomicCoder<RestrictionWithBoundedDefaultTracker> {
    public static CoderForDefaultTracker of() {
      return new CoderForDefaultTracker();
    }

    @Override
    public void encode(RestrictionWithBoundedDefaultTracker value, OutputStream outStream) {}

    @Override
    public RestrictionWithBoundedDefaultTracker decode(InputStream inStream) {
      return null;
    }
  }

  private static class WatermarkEstimatorStateWithDefaultWatermarkEstimator
      implements HasDefaultWatermarkEstimator<
          WatermarkEstimatorStateWithDefaultWatermarkEstimator, DefaultWatermarkEstimator> {

    @Override
    public DefaultWatermarkEstimator newWatermarkEstimator() {
      return new DefaultWatermarkEstimator();
    }
  }

  private static class DefaultWatermarkEstimator
      implements WatermarkEstimator<WatermarkEstimatorStateWithDefaultWatermarkEstimator> {
    @Override
    public Instant currentWatermark() {
      return null;
    }

    @Override
    public WatermarkEstimatorStateWithDefaultWatermarkEstimator getState() {
      return null;
    }
  }

  private static class CoderForWatermarkEstimatorStateWithDefaultWatermarkEstimator
      extends AtomicCoder<WatermarkEstimatorStateWithDefaultWatermarkEstimator> {

    @Override
    public void encode(
        WatermarkEstimatorStateWithDefaultWatermarkEstimator value, OutputStream outStream)
        throws CoderException, IOException {}

    @Override
    public WatermarkEstimatorStateWithDefaultWatermarkEstimator decode(InputStream inStream)
        throws CoderException, IOException {
      return null;
    }
  }

  @Test
  public void testSplittableDoFnWithHasDefaultMethods() throws Exception {
    class MockFn extends DoFn<String, String> {
      @ProcessElement
      public void processElement(
          ProcessContext c,
          RestrictionTracker<RestrictionWithBoundedDefaultTracker, Void> tracker,
          WatermarkEstimator<WatermarkEstimatorStateWithDefaultWatermarkEstimator>
              watermarkEstimator) {}

      @GetInitialRestriction
      public RestrictionWithBoundedDefaultTracker getInitialRestriction(@Element String element) {
        return null;
      }

      @GetInitialWatermarkEstimatorState
      public WatermarkEstimatorStateWithDefaultWatermarkEstimator
          getInitialWatermarkEstimatorState() {
        return null;
      }
    }

    MockFn fn = mock(MockFn.class);
    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(fn);

    CoderRegistry coderRegistry = CoderRegistry.createDefault();
    coderRegistry.registerCoderProvider(
        CoderProviders.fromStaticMethods(
            RestrictionWithBoundedDefaultTracker.class, CoderForDefaultTracker.class));
    coderRegistry.registerCoderForClass(
        WatermarkEstimatorStateWithDefaultWatermarkEstimator.class,
        new CoderForWatermarkEstimatorStateWithDefaultWatermarkEstimator());
    assertThat(
        invoker.<RestrictionWithBoundedDefaultTracker>invokeGetRestrictionCoder(coderRegistry),
        instanceOf(CoderForDefaultTracker.class));
    assertThat(
        invoker.invokeGetWatermarkEstimatorStateCoder(coderRegistry),
        instanceOf(CoderForWatermarkEstimatorStateWithDefaultWatermarkEstimator.class));
    invoker.invokeSplitRestriction(
        new FakeArgumentProvider<String, String>() {
          @Override
          public String element(DoFn<String, String> doFn) {
            return "blah";
          }

          @Override
          public Object restriction() {
            return "foo";
          }

          @Override
          public OutputReceiver<String> outputReceiver(DoFn<String, String> doFn) {
            return new DoFn.OutputReceiver<String>() {
              private boolean invoked;

              @Override
              public void output(String output) {
                assertFalse(invoked);
                invoked = true;
                assertEquals("foo", output);
              }

              @Override
              public void outputWithTimestamp(String output, Instant instant) {
                assertFalse(invoked);
                invoked = true;
                assertEquals("foo", output);
              }

              @Override
              public void outputWindowedValue(
                  String output,
                  Instant timestamp,
                  Collection<? extends BoundedWindow> windows,
                  PaneInfo paneInfo) {
                assertFalse(invoked);
                invoked = true;
                assertEquals("foo", output);
              }
            };
          }
        });
    assertEquals(stop(), invoker.invokeProcessElement(mockArgumentProvider));
    assertThat(
        invoker.invokeNewTracker(
            new FakeArgumentProvider<String, String>() {
              @Override
              public Object restriction() {
                return new RestrictionWithBoundedDefaultTracker();
              }
            }),
        instanceOf(BoundedDefaultTracker.class));
    assertThat(
        invoker.invokeNewWatermarkEstimator(
            new FakeArgumentProvider<String, String>() {
              @Override
              public Object watermarkEstimatorState() {
                return new WatermarkEstimatorStateWithDefaultWatermarkEstimator();
              }
            }),
        instanceOf(DefaultWatermarkEstimator.class));
  }

  @Test
  public void testTruncateFnWithHasDefaultMethodsWhenBounded() throws Exception {
    class BoundedMockFn extends DoFn<String, String> {
      @ProcessElement
      public void processElement(
          ProcessContext c,
          RestrictionTracker<RestrictionWithBoundedDefaultTracker, Void> tracker,
          WatermarkEstimator<WatermarkEstimatorStateWithDefaultWatermarkEstimator>
              watermarkEstimator) {}

      @GetInitialRestriction
      public RestrictionWithBoundedDefaultTracker getInitialRestriction(@Element String element) {
        return null;
      }

      @GetInitialWatermarkEstimatorState
      public WatermarkEstimatorStateWithDefaultWatermarkEstimator
          getInitialWatermarkEstimatorState() {
        return null;
      }
    }

    BoundedMockFn fn = mock(BoundedMockFn.class);
    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(fn);

    CoderRegistry coderRegistry = CoderRegistry.createDefault();
    coderRegistry.registerCoderProvider(
        CoderProviders.fromStaticMethods(
            RestrictionWithBoundedDefaultTracker.class, CoderForDefaultTracker.class));
    coderRegistry.registerCoderForClass(
        WatermarkEstimatorStateWithDefaultWatermarkEstimator.class,
        new CoderForWatermarkEstimatorStateWithDefaultWatermarkEstimator());
    assertThat(
        invoker.<RestrictionWithBoundedDefaultTracker>invokeGetRestrictionCoder(coderRegistry),
        instanceOf(CoderForDefaultTracker.class));
    assertThat(
        invoker.invokeGetWatermarkEstimatorStateCoder(coderRegistry),
        instanceOf(CoderForWatermarkEstimatorStateWithDefaultWatermarkEstimator.class));
    RestrictionTracker tracker =
        invoker.invokeNewTracker(
            new FakeArgumentProvider<String, String>() {
              @Override
              public Object restriction() {
                return new RestrictionWithBoundedDefaultTracker();
              }
            });
    assertThat(tracker, instanceOf(BoundedDefaultTracker.class));
    TruncateResult<?> result =
        invoker.invokeTruncateRestriction(
            new FakeArgumentProvider<String, String>() {
              @Override
              public RestrictionTracker restrictionTracker() {
                return tracker;
              }

              @Override
              public String element(DoFn<String, String> doFn) {
                return "blah";
              }

              @Override
              public Object restriction() {
                return "foo";
              }
            });
    assertEquals("foo", result.getTruncatedRestriction());
    assertEquals(stop(), invoker.invokeProcessElement(mockArgumentProvider));
    assertThat(
        invoker.invokeNewWatermarkEstimator(
            new FakeArgumentProvider<String, String>() {
              @Override
              public Object watermarkEstimatorState() {
                return new WatermarkEstimatorStateWithDefaultWatermarkEstimator();
              }
            }),
        instanceOf(DefaultWatermarkEstimator.class));
  }

  @Test
  public void testTruncateFnWithHasDefaultMethodsWhenUnbounded() throws Exception {
    class UnboundedMockFn extends DoFn<String, String> {
      @ProcessElement
      public void processElement(
          ProcessContext c,
          RestrictionTracker<RestrictionWithUnboundedDefaultTracker, Void> tracker,
          WatermarkEstimator<WatermarkEstimatorStateWithDefaultWatermarkEstimator>
              watermarkEstimator) {}

      @GetInitialRestriction
      public RestrictionWithUnboundedDefaultTracker getInitialRestriction(@Element String element) {
        return null;
      }

      @GetInitialWatermarkEstimatorState
      public WatermarkEstimatorStateWithDefaultWatermarkEstimator
          getInitialWatermarkEstimatorState() {
        return null;
      }
    }

    UnboundedMockFn fn = mock(UnboundedMockFn.class);
    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(fn);

    CoderRegistry coderRegistry = CoderRegistry.createDefault();
    coderRegistry.registerCoderProvider(
        CoderProviders.fromStaticMethods(
            RestrictionWithUnboundedDefaultTracker.class, CoderForDefaultTracker.class));
    coderRegistry.registerCoderForClass(
        WatermarkEstimatorStateWithDefaultWatermarkEstimator.class,
        new CoderForWatermarkEstimatorStateWithDefaultWatermarkEstimator());
    assertThat(
        invoker.<RestrictionWithBoundedDefaultTracker>invokeGetRestrictionCoder(coderRegistry),
        instanceOf(CoderForDefaultTracker.class));
    assertThat(
        invoker.invokeGetWatermarkEstimatorStateCoder(coderRegistry),
        instanceOf(CoderForWatermarkEstimatorStateWithDefaultWatermarkEstimator.class));
    RestrictionTracker tracker =
        invoker.invokeNewTracker(
            new FakeArgumentProvider<String, String>() {
              @Override
              public Object restriction() {
                return new RestrictionWithUnboundedDefaultTracker();
              }
            });
    assertThat(tracker, instanceOf(UnboundedDefaultTracker.class));
    TruncateResult<?> result =
        invoker.invokeTruncateRestriction(
            new FakeArgumentProvider<String, String>() {
              @Override
              public RestrictionTracker restrictionTracker() {
                return tracker;
              }

              @Override
              public String element(DoFn<String, String> doFn) {
                return "blah";
              }

              @Override
              public Object restriction() {
                return "foo";
              }
            });
    assertNull(result);
    assertEquals(stop(), invoker.invokeProcessElement(mockArgumentProvider));
    assertThat(
        invoker.invokeNewWatermarkEstimator(
            new FakeArgumentProvider<String, String>() {
              @Override
              public Object watermarkEstimatorState() {
                return new WatermarkEstimatorStateWithDefaultWatermarkEstimator();
              }
            }),
        instanceOf(DefaultWatermarkEstimator.class));
  }

  @Test
  public void testDefaultWatermarkEstimatorStateAndCoder() throws Exception {
    class MockFn extends DoFn<String, String> {
      @ProcessElement
      public void processElement(
          ProcessContext c,
          RestrictionTracker<RestrictionWithBoundedDefaultTracker, Void> tracker) {}

      @GetInitialRestriction
      public RestrictionWithBoundedDefaultTracker getInitialRestriction(@Element String element) {
        return null;
      }
    }

    MockFn fn = mock(MockFn.class);
    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(fn);

    CoderRegistry coderRegistry = CoderRegistry.createDefault();
    coderRegistry.registerCoderProvider(
        CoderProviders.fromStaticMethods(
            RestrictionWithBoundedDefaultTracker.class, CoderForDefaultTracker.class));
    assertEquals(VoidCoder.of(), invoker.invokeGetWatermarkEstimatorStateCoder(coderRegistry));
    assertNull(invoker.invokeGetInitialWatermarkEstimatorState(new FakeArgumentProvider<>()));
  }

  @Test
  public void testDefaultGetSizeWithoutHasProgress() throws Exception {
    class MockFn extends DoFn<String, String> {
      @ProcessElement
      public void processElement(
          ProcessContext c,
          RestrictionTracker<RestrictionWithBoundedDefaultTracker, Void> tracker) {}

      @GetInitialRestriction
      public RestrictionWithBoundedDefaultTracker getInitialRestriction(@Element String element) {
        return null;
      }
    }

    MockFn fn = mock(MockFn.class);
    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(fn);
    assertEquals(0.0, invoker.invokeGetSize(mockArgumentProvider), 0.0001);
  }

  @Test
  public void testDefaultGetSizeWithHasProgress() throws Exception {
    class MockFn extends DoFn<String, String> {
      @ProcessElement
      public void processElement(
          ProcessContext c,
          RestrictionTracker<RestrictionWithBoundedDefaultTracker, Void> tracker) {}

      @GetInitialRestriction
      public RestrictionWithBoundedDefaultTracker getInitialRestriction(@Element String element) {
        return null;
      }
    }

    abstract class HasProgressRestrictionTracker extends SomeRestrictionTracker
        implements HasProgress {}
    HasProgressRestrictionTracker tracker = mock(HasProgressRestrictionTracker.class);
    when(tracker.getProgress()).thenReturn(Progress.from(3.0, 4.0));

    when(mockArgumentProvider.restrictionTracker()).thenReturn((RestrictionTracker) tracker);

    MockFn fn = mock(MockFn.class);
    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(fn);
    assertEquals(4.0, invoker.invokeGetSize(mockArgumentProvider), 0.0001);
  }

  @Test
  public void testGetSize() throws Exception {
    abstract class MockFn extends DoFn<String, String> {
      @ProcessElement
      public abstract void processElement(
          ProcessContext c, RestrictionTracker<RestrictionWithBoundedDefaultTracker, Void> tracker);

      @GetInitialRestriction
      public abstract RestrictionWithBoundedDefaultTracker getInitialRestriction(
          @Element String element);

      @GetSize
      public abstract double getSize();
    }

    MockFn fn = mock(MockFn.class);
    when(fn.getSize()).thenReturn(5.0, -3.0);

    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(fn);
    assertEquals(5.0, invoker.invokeGetSize(mockArgumentProvider), 0.0001);
    assertThrows(
        "Expected size >= 0 but received",
        IllegalArgumentException.class,
        () -> invoker.invokeGetSize(mockArgumentProvider));
  }

  // ---------------------------------------------------------------------------------------
  // Tests for ability to invoke @OnTimer for private, inner and anonymous classes.
  // ---------------------------------------------------------------------------------------

  private static final String TIMER_ID = "test-timer-id";

  private static class PrivateDoFnWithTimers extends DoFn<String, String> {
    @ProcessElement
    public void processThis(ProcessContext c) {}

    @TimerId(TIMER_ID)
    private final TimerSpec myTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @OnTimer(TIMER_ID)
    public void onTimer(BoundedWindow w) {}
  }

  @Test
  public void testLocalPrivateDoFnWithTimers() throws Exception {
    PrivateDoFnWithTimers fn = mock(PrivateDoFnWithTimers.class);
    invokeOnTimer(TIMER_ID, fn);
    verify(fn).onTimer(mockWindow);
  }

  @Test
  public void testStaticPackagePrivateDoFnWithTimers() throws Exception {
    DoFn<String, String> fn =
        mock(DoFnInvokersTestHelper.newStaticPackagePrivateDoFnWithTimers().getClass());
    invokeOnTimer(TIMER_ID, fn);
    DoFnInvokersTestHelper.verifyStaticPackagePrivateDoFnWithTimers(fn, mockWindow);
  }

  @Test
  public void testInnerPackagePrivateDoFnWithTimers() throws Exception {
    DoFn<String, String> fn =
        mock(new DoFnInvokersTestHelper().newInnerPackagePrivateDoFnWithTimers().getClass());
    invokeOnTimer(TIMER_ID, fn);
    DoFnInvokersTestHelper.verifyInnerPackagePrivateDoFnWithTimers(fn, mockWindow);
  }

  @Test
  public void testStaticPrivateDoFnWithTimers() throws Exception {
    DoFn<String, String> fn =
        mock(DoFnInvokersTestHelper.newStaticPrivateDoFnWithTimers().getClass());
    invokeOnTimer(TIMER_ID, fn);
    DoFnInvokersTestHelper.verifyStaticPrivateDoFnWithTimers(fn, mockWindow);
  }

  @Test
  public void testInnerPrivateDoFnWithTimers() throws Exception {
    DoFn<String, String> fn =
        mock(new DoFnInvokersTestHelper().newInnerPrivateDoFnWithTimers().getClass());
    invokeOnTimer(TIMER_ID, fn);
    DoFnInvokersTestHelper.verifyInnerPrivateDoFnWithTimers(fn, mockWindow);
  }

  @Test
  public void testAnonymousInnerDoFnWithTimers() throws Exception {
    DoFn<String, String> fn =
        mock(new DoFnInvokersTestHelper().newInnerAnonymousDoFnWithTimers().getClass());
    invokeOnTimer(TIMER_ID, fn);
    DoFnInvokersTestHelper.verifyInnerAnonymousDoFnWithTimers(fn, mockWindow);
  }

  @Test
  public void testStaticAnonymousDoFnWithTimersInOtherPackage() throws Exception {
    // Can't use mockito for this one - the anonymous class is final and can't be mocked.
    DoFn<String, String> fn = DoFnInvokersTestHelper.newStaticAnonymousDoFnWithTimers();
    invokeOnTimer(TIMER_ID, fn);
    DoFnInvokersTestHelper.verifyStaticAnonymousDoFnWithTimersInvoked(fn, mockWindow);
  }

  // ---------------------------------------------------------------------------------------
  // Tests for ability to invoke @ProcessElement for private, inner and anonymous classes.
  // ---------------------------------------------------------------------------------------

  private static class PrivateDoFnClass extends DoFn<String, String> {
    @ProcessElement
    public void processThis(ProcessContext c) {}
  }

  @Test
  public void testLocalPrivateDoFnClass() throws Exception {
    PrivateDoFnClass fn = mock(PrivateDoFnClass.class);
    assertEquals(stop(), invokeProcessElement(fn));
    verify(fn).processThis(mockProcessContext);
  }

  @Test
  public void testStaticPackagePrivateDoFnClass() throws Exception {
    DoFn<String, String> fn = mock(DoFnInvokersTestHelper.newStaticPackagePrivateDoFn().getClass());
    assertEquals(stop(), invokeProcessElement(fn));
    DoFnInvokersTestHelper.verifyStaticPackagePrivateDoFn(fn, mockProcessContext);
  }

  @Test
  public void testInnerPackagePrivateDoFnClass() throws Exception {
    DoFn<String, String> fn =
        mock(new DoFnInvokersTestHelper().newInnerPackagePrivateDoFn().getClass());
    assertEquals(stop(), invokeProcessElement(fn));
    DoFnInvokersTestHelper.verifyInnerPackagePrivateDoFn(fn, mockProcessContext);
  }

  @Test
  public void testStaticPrivateDoFnClass() throws Exception {
    DoFn<String, String> fn = mock(DoFnInvokersTestHelper.newStaticPrivateDoFn().getClass());
    assertEquals(stop(), invokeProcessElement(fn));
    DoFnInvokersTestHelper.verifyStaticPrivateDoFn(fn, mockProcessContext);
  }

  @Test
  public void testInnerPrivateDoFnClass() throws Exception {
    DoFn<String, String> fn = mock(new DoFnInvokersTestHelper().newInnerPrivateDoFn().getClass());
    assertEquals(stop(), invokeProcessElement(fn));
    DoFnInvokersTestHelper.verifyInnerPrivateDoFn(fn, mockProcessContext);
  }

  @Test
  public void testAnonymousInnerDoFn() throws Exception {
    DoFn<String, String> fn = mock(new DoFnInvokersTestHelper().newInnerAnonymousDoFn().getClass());
    assertEquals(stop(), invokeProcessElement(fn));
    DoFnInvokersTestHelper.verifyInnerAnonymousDoFn(fn, mockProcessContext);
  }

  @Test
  public void testStaticAnonymousDoFnInOtherPackage() throws Exception {
    // Can't use mockito for this one - the anonymous class is final and can't be mocked.
    DoFn<String, String> fn = DoFnInvokersTestHelper.newStaticAnonymousDoFn();
    invokeProcessElement(fn);
    DoFnInvokersTestHelper.verifyStaticAnonymousDoFnInvoked(fn, mockProcessContext);
  }

  // ---------------------------------------------------------------------------------------
  // Tests for wrapping exceptions.
  // ---------------------------------------------------------------------------------------

  @Test
  public void testProcessElementException() throws Exception {
    DoFnInvoker<Integer, Integer> invoker =
        DoFnInvokers.invokerFor(
            new DoFn<Integer, Integer>() {
              @ProcessElement
              public void processElement(@SuppressWarnings("unused") ProcessContext c) {
                throw new IllegalArgumentException("bogus");
              }
            });
    thrown.expect(UserCodeException.class);
    thrown.expectMessage("bogus");
    invoker.invokeProcessElement(
        new FakeArgumentProvider<Integer, Integer>() {
          @Override
          public DoFn<Integer, Integer>.ProcessContext processContext(DoFn<Integer, Integer> fn) {
            return null;
          }
        });
  }

  @Test
  public void testProcessElementExceptionWithReturn() throws Exception {
    thrown.expect(UserCodeException.class);
    thrown.expectMessage("bogus");
    DoFnInvokers.invokerFor(
            new DoFn<Integer, Integer>() {
              @ProcessElement
              public ProcessContinuation processElement(
                  @SuppressWarnings("unused") ProcessContext c,
                  RestrictionTracker<SomeRestriction, Void> tracker) {
                throw new IllegalArgumentException("bogus");
              }

              @GetInitialRestriction
              public SomeRestriction getInitialRestriction(@Element Integer element) {
                return null;
              }

              @NewTracker
              public SomeRestrictionTracker newTracker(@Restriction SomeRestriction restriction) {
                return null;
              }
            })
        .invokeProcessElement(
            new FakeArgumentProvider<Integer, Integer>() {
              @Override
              public DoFn.ProcessContext processContext(DoFn<Integer, Integer> doFn) {
                return null; // will not be touched
              }

              @Override
              public RestrictionTracker<?, ?> restrictionTracker() {
                return null; // will not be touched
              }
            });
  }

  @Test
  public void testStartBundleException() throws Exception {
    DoFnInvoker.ArgumentProvider<Integer, Integer> mockArguments =
        mock(DoFnInvoker.ArgumentProvider.class);
    when(mockArguments.startBundleContext(any(DoFn.class))).thenReturn(null);
    DoFnInvoker<Integer, Integer> invoker =
        DoFnInvokers.invokerFor(
            new DoFn<Integer, Integer>() {
              @StartBundle
              public void startBundle(@SuppressWarnings("unused") StartBundleContext c) {
                throw new IllegalArgumentException("bogus");
              }

              @ProcessElement
              public void processElement(@SuppressWarnings("unused") ProcessContext c) {}
            });
    thrown.expect(UserCodeException.class);
    thrown.expectMessage("bogus");
    invoker.invokeStartBundle(mockArguments);
  }

  @Test
  public void testFinishBundleException() throws Exception {
    DoFnInvoker.ArgumentProvider<Integer, Integer> mockArguments =
        mock(DoFnInvoker.ArgumentProvider.class);
    when(mockArguments.finishBundleContext(any(DoFn.class))).thenReturn(null);
    DoFnInvoker<Integer, Integer> invoker =
        DoFnInvokers.invokerFor(
            new DoFn<Integer, Integer>() {
              @FinishBundle
              public void finishBundle(@SuppressWarnings("unused") FinishBundleContext c) {
                throw new IllegalArgumentException("bogus");
              }

              @ProcessElement
              public void processElement(@SuppressWarnings("unused") ProcessContext c) {}
            });
    thrown.expect(UserCodeException.class);
    thrown.expectMessage("bogus");
    invoker.invokeFinishBundle(mockArguments);
  }

  @Test
  public void testOnTimerHelloWord() throws Exception {
    final String timerId = "my-timer-id";

    class SimpleTimerDoFn extends DoFn<String, String> {

      public String status = "not yet";

      @TimerId(timerId)
      private final TimerSpec myTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

      @ProcessElement
      public void process(ProcessContext c) {}

      @OnTimer(timerId)
      public void onMyTimer() {
        status = "OK now";
      }
    }

    SimpleTimerDoFn fn = new SimpleTimerDoFn();

    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(fn);
    invoker.invokeOnTimer(TimerDeclaration.PREFIX + timerId, "", mockArgumentProvider);
    assertThat(fn.status, equalTo("OK now"));
  }

  @Test
  public void testOnTimerWithWindow() throws Exception {
    final String timerId = "my-timer-id";
    final IntervalWindow testWindow = new IntervalWindow(new Instant(0), new Instant(15));
    when(mockArgumentProvider.window()).thenReturn(testWindow);

    class SimpleTimerDoFn extends DoFn<String, String> {

      public IntervalWindow window = null;

      @TimerId(timerId)
      private final TimerSpec myTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

      @ProcessElement
      public void process(ProcessContext c) {}

      @OnTimer(timerId)
      public void onMyTimer(IntervalWindow w) {
        window = w;
      }
    }

    SimpleTimerDoFn fn = new SimpleTimerDoFn();

    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(fn);
    invoker.invokeOnTimer(TimerDeclaration.PREFIX + timerId, "", mockArgumentProvider);
    assertThat(fn.window, equalTo(testWindow));
  }

  static class StableNameTestDoFn extends DoFn<Void, Void> {
    @ProcessElement
    public void process() {}
  }

  /** This is a change-detector test that the generated name is stable across runs. */
  @Test
  public void testStableName() {
    DoFnInvoker<Void, Void> invoker = DoFnInvokers.invokerFor(new StableNameTestDoFn());
    assertThat(
        invoker.getClass().getName(),
        equalTo(
            String.format(
                "%s$%s", StableNameTestDoFn.class.getName(), DoFnInvoker.class.getSimpleName())));
  }

  @Test
  public void testBundleFinalizer() {
    class BundleFinalizerDoFn extends DoFn<String, String> {
      @ProcessElement
      public void processElement(BundleFinalizer bundleFinalizer) {
        bundleFinalizer.afterBundleCommit(Instant.ofEpochSecond(42L), null);
      }
    }

    BundleFinalizer mockBundleFinalizer = mock(BundleFinalizer.class);
    when(mockArgumentProvider.bundleFinalizer()).thenReturn(mockBundleFinalizer);

    DoFnInvoker<String, String> invoker = DoFnInvokers.invokerFor(new BundleFinalizerDoFn());
    invoker.invokeProcessElement(mockArgumentProvider);

    verify(mockBundleFinalizer).afterBundleCommit(eq(Instant.ofEpochSecond(42L)), eq(null));
  }
}
