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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static org.apache.beam.runners.core.construction.PTransformTranslation.PAR_DO_TRANSFORM_URN;
import static org.apache.beam.runners.flink.translation.wrappers.streaming.StreamRecordStripper.stripStreamRecordFromWindowedValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.mutable.MutableObject;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.metrics.DoFnRunnerWithMetricsUpdate;
import org.apache.beam.runners.flink.streaming.FlinkStateInternalsTest;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageContextFactory;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.ExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.control.TimerReceiverFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.NoopLock;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.sdk.v2.sdk.extensions.protobuf.ByteStringCoder;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.reflect.Whitebox;

/** Tests for {@link ExecutableStageDoFnOperator}. */
@RunWith(JUnit4.class)
public class ExecutableStageDoFnOperatorTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private RuntimeContext runtimeContext;
  @Mock private DistributedCache distributedCache;
  @Mock private ExecutableStageContext stageContext;
  @Mock private StageBundleFactory stageBundleFactory;
  @Mock private StateRequestHandler stateRequestHandler;
  @Mock private ProcessBundleDescriptors.ExecutableProcessBundleDescriptor processBundleDescriptor;

  // NOTE: ExecutableStage.fromPayload expects exactly one input, so we provide one here. These unit
  // tests in general ignore the executable stage itself and mock around it.
  private final ExecutableStagePayload stagePayload =
      ExecutableStagePayload.newBuilder()
          .setInput("input")
          .setComponents(
              Components.newBuilder()
                  .putPcollections("input", PCollection.getDefaultInstance())
                  .build())
          .build();

  private final String stateId = "userState";
  private final ExecutableStagePayload stagePayloadWithUserState =
      stagePayload
          .toBuilder()
          .setComponents(
              stagePayload
                  .getComponents()
                  .toBuilder()
                  .putTransforms(
                      "transform",
                      RunnerApi.PTransform.newBuilder()
                          .setSpec(
                              RunnerApi.FunctionSpec.newBuilder()
                                  .setUrn(PAR_DO_TRANSFORM_URN)
                                  .build())
                          .putInputs("input", "input")
                          .build())
                  .build())
          .addUserStates(
              ExecutableStagePayload.UserStateId.newBuilder()
                  .setLocalName(stateId)
                  .setTransformId("transform")
                  .build())
          .build();

  private final JobInfo jobInfo =
      JobInfo.create("job-id", "job-name", "retrieval-token", Struct.getDefaultInstance());

  @Before
  public void setUpMocks() {
    MockitoAnnotations.initMocks(this);
    when(runtimeContext.getDistributedCache()).thenReturn(distributedCache);
    when(stageContext.getStageBundleFactory(any())).thenReturn(stageBundleFactory);
    when(processBundleDescriptor.getTimerSpecs()).thenReturn(Collections.emptyMap());
    when(processBundleDescriptor.getBagUserStateSpecs()).thenReturn(Collections.emptyMap());
    when(stageBundleFactory.getProcessBundleDescriptor()).thenReturn(processBundleDescriptor);
  }

  @Test
  public void sdkErrorsSurfaceOnClose() throws Exception {
    TupleTag<Integer> mainOutput = new TupleTag<>("main-output");
    DoFnOperator.MultiOutputOutputManagerFactory<Integer> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(mainOutput, VoidCoder.of());
    ExecutableStageDoFnOperator<Integer, Integer> operator =
        getOperator(mainOutput, Collections.emptyList(), outputManagerFactory);

    OneInputStreamOperatorTestHarness<WindowedValue<Integer>, WindowedValue<Integer>> testHarness =
        new OneInputStreamOperatorTestHarness<>(operator);

    testHarness.open();

    @SuppressWarnings("unchecked")
    RemoteBundle bundle = Mockito.mock(RemoteBundle.class);
    when(stageBundleFactory.getBundle(any(), any(), any(), any())).thenReturn(bundle);

    @SuppressWarnings("unchecked")
    FnDataReceiver<WindowedValue<?>> receiver = Mockito.mock(FnDataReceiver.class);
    when(bundle.getInputReceivers()).thenReturn(ImmutableMap.of("input", receiver));

    Exception expected = new RuntimeException(new Exception());
    doThrow(expected).when(bundle).close();
    thrown.expectCause(is(expected));

    operator.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow(0)));
    testHarness.close();
  }

  @Test
  public void expectedInputsAreSent() throws Exception {
    TupleTag<Integer> mainOutput = new TupleTag<>("main-output");
    DoFnOperator.MultiOutputOutputManagerFactory<Integer> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(mainOutput, VoidCoder.of());
    ExecutableStageDoFnOperator<Integer, Integer> operator =
        getOperator(mainOutput, Collections.emptyList(), outputManagerFactory);

    @SuppressWarnings("unchecked")
    RemoteBundle bundle = Mockito.mock(RemoteBundle.class);
    when(stageBundleFactory.getBundle(any(), any(), any(), any())).thenReturn(bundle);

    @SuppressWarnings("unchecked")
    FnDataReceiver<WindowedValue<?>> receiver = Mockito.mock(FnDataReceiver.class);
    when(bundle.getInputReceivers()).thenReturn(ImmutableMap.of("input", receiver));

    WindowedValue<Integer> one = WindowedValue.valueInGlobalWindow(1);
    WindowedValue<Integer> two = WindowedValue.valueInGlobalWindow(2);
    WindowedValue<Integer> three = WindowedValue.valueInGlobalWindow(3);

    OneInputStreamOperatorTestHarness<WindowedValue<Integer>, WindowedValue<Integer>> testHarness =
        new OneInputStreamOperatorTestHarness<>(operator);

    testHarness.open();

    testHarness.processElement(new StreamRecord<>(one));
    testHarness.processElement(new StreamRecord<>(two));
    testHarness.processElement(new StreamRecord<>(three));

    verify(receiver).accept(one);
    verify(receiver).accept(two);
    verify(receiver).accept(three);
    verifyNoMoreInteractions(receiver);

    testHarness.close();
  }

  @Test
  public void outputsAreTaggedCorrectly() throws Exception {

    WindowedValue.ValueOnlyWindowedValueCoder<Integer> coder =
        WindowedValue.getValueOnlyCoder(VarIntCoder.of());

    TupleTag<Integer> mainOutput = new TupleTag<>("main-output");
    TupleTag<Integer> additionalOutput1 = new TupleTag<>("output-1");
    TupleTag<Integer> additionalOutput2 = new TupleTag<>("output-2");
    ImmutableMap<TupleTag<?>, OutputTag<?>> tagsToOutputTags =
        ImmutableMap.<TupleTag<?>, OutputTag<?>>builder()
            .put(
                additionalOutput1,
                new OutputTag<WindowedValue<String>>(additionalOutput1.getId()) {})
            .put(
                additionalOutput2,
                new OutputTag<WindowedValue<String>>(additionalOutput2.getId()) {})
            .build();
    ImmutableMap<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders =
        ImmutableMap.<TupleTag<?>, Coder<WindowedValue<?>>>builder()
            .put(mainOutput, (Coder) coder)
            .put(additionalOutput1, coder)
            .put(additionalOutput2, coder)
            .build();
    ImmutableMap<TupleTag<?>, Integer> tagsToIds =
        ImmutableMap.<TupleTag<?>, Integer>builder()
            .put(mainOutput, 0)
            .put(additionalOutput1, 1)
            .put(additionalOutput2, 2)
            .build();

    DoFnOperator.MultiOutputOutputManagerFactory<Integer> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(
            mainOutput, tagsToOutputTags, tagsToCoders, tagsToIds);

    WindowedValue<Integer> zero = WindowedValue.valueInGlobalWindow(0);
    WindowedValue<Integer> three = WindowedValue.valueInGlobalWindow(3);
    WindowedValue<Integer> four = WindowedValue.valueInGlobalWindow(4);
    WindowedValue<Integer> five = WindowedValue.valueInGlobalWindow(5);

    // We use a real StageBundleFactory here in order to exercise the output receiver factory.
    StageBundleFactory stageBundleFactory =
        new StageBundleFactory() {

          private boolean onceEmitted;

          @Override
          public RemoteBundle getBundle(
              OutputReceiverFactory receiverFactory,
              TimerReceiverFactory timerReceiverFactory,
              StateRequestHandler stateRequestHandler,
              BundleProgressHandler progressHandler) {
            return new RemoteBundle() {
              @Override
              public String getId() {
                return "bundle-id";
              }

              @Override
              public Map<String, FnDataReceiver> getInputReceivers() {
                return ImmutableMap.of(
                    "input",
                    input -> {
                      /* Ignore input*/
                    });
              }

              @Override
              public Map<KV<String, String>, FnDataReceiver<Timer>> getTimerReceivers() {
                return Collections.emptyMap();
              }

              @Override
              public void requestProgress() {
                throw new UnsupportedOperationException();
              }

              @Override
              public void split(double fractionOfRemainder) {
                throw new UnsupportedOperationException();
              }

              @Override
              public void close() throws Exception {
                if (onceEmitted) {
                  return;
                }
                // Emit all values to the runner when the bundle is closed.
                receiverFactory.create(mainOutput.getId()).accept(three);
                receiverFactory.create(additionalOutput1.getId()).accept(four);
                receiverFactory.create(additionalOutput2.getId()).accept(five);
                onceEmitted = true;
              }
            };
          }

          @Override
          public ProcessBundleDescriptors.ExecutableProcessBundleDescriptor
              getProcessBundleDescriptor() {
            return processBundleDescriptor;
          }

          @Override
          public void close() {}
        };
    // Wire the stage bundle factory into our context.
    when(stageContext.getStageBundleFactory(any())).thenReturn(stageBundleFactory);

    ExecutableStageDoFnOperator<Integer, Integer> operator =
        getOperator(
            mainOutput,
            ImmutableList.of(additionalOutput1, additionalOutput2),
            outputManagerFactory);

    OneInputStreamOperatorTestHarness<WindowedValue<Integer>, WindowedValue<Integer>> testHarness =
        new OneInputStreamOperatorTestHarness<>(operator);

    long watermark = testHarness.getCurrentWatermark() + 1;
    testHarness.open();

    testHarness.processElement(new StreamRecord<>(zero));

    testHarness.processWatermark(watermark);
    watermark++;
    testHarness.processWatermark(watermark);

    assertEquals(watermark, testHarness.getCurrentWatermark());
    // watermark hold until bundle complete
    assertEquals(0, testHarness.getOutput().size());

    testHarness.close(); // triggers finish bundle

    assertThat(stripStreamRecordFromWindowedValue(testHarness.getOutput()), contains(three));

    assertThat(
        testHarness.getSideOutput(tagsToOutputTags.get(additionalOutput1)),
        contains(new StreamRecord<>(four)));

    assertThat(
        testHarness.getSideOutput(tagsToOutputTags.get(additionalOutput2)),
        contains(new StreamRecord<>(five)));
  }

  @Test
  public void testWatermarkHandling() throws Exception {
    TupleTag<Integer> mainOutput = new TupleTag<>("main-output");
    DoFnOperator.MultiOutputOutputManagerFactory<Integer> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(mainOutput, VoidCoder.of());
    ExecutableStageDoFnOperator<KV<String, Integer>, Integer> operator =
        getOperator(
            mainOutput,
            Collections.emptyList(),
            outputManagerFactory,
            WindowingStrategy.of(FixedWindows.of(Duration.millis(10))),
            StringUtf8Coder.of(),
            WindowedValue.getFullCoder(
                KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()), IntervalWindow.getCoder()));

    KeyedOneInputStreamOperatorTestHarness<
            String, WindowedValue<KV<String, Integer>>, WindowedValue<Integer>>
        testHarness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                operator,
                val -> val.getValue().getKey(),
                new CoderTypeInformation<>(StringUtf8Coder.of()));
    RemoteBundle bundle = Mockito.mock(RemoteBundle.class);
    when(bundle.getInputReceivers())
        .thenReturn(
            ImmutableMap.<String, FnDataReceiver<WindowedValue>>builder()
                .put("input", Mockito.mock(FnDataReceiver.class))
                .build());
    when(bundle.getTimerReceivers())
        .thenReturn(
            ImmutableMap.<KV<String, String>, FnDataReceiver<WindowedValue>>builder()
                .put(KV.of("transform", "timer"), Mockito.mock(FnDataReceiver.class))
                .put(KV.of("transform", "timer2"), Mockito.mock(FnDataReceiver.class))
                .put(KV.of("transform", "timer3"), Mockito.mock(FnDataReceiver.class))
                .build());
    when(stageBundleFactory.getBundle(any(), any(), any(), any())).thenReturn(bundle);

    testHarness.open();
    assertThat(
        operator.getCurrentOutputWatermark(), is(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis()));

    // No bundle has been started, watermark can be freely advanced
    testHarness.processWatermark(0);
    assertThat(operator.getCurrentOutputWatermark(), is(0L));

    // Trigger a new bundle
    IntervalWindow intervalWindow = new IntervalWindow(new Instant(0), new Instant(9));
    WindowedValue<KV<String, Integer>> windowedValue =
        WindowedValue.of(KV.of("one", 1), Instant.now(), intervalWindow, PaneInfo.NO_FIRING);
    testHarness.processElement(new StreamRecord<>(windowedValue));

    // The output watermark should be held back during the bundle
    testHarness.processWatermark(1);
    assertThat(operator.getEffectiveInputWatermark(), is(1L));
    assertThat(operator.getCurrentOutputWatermark(), is(0L));

    // After the bundle has been finished, the watermark should be advanced
    operator.invokeFinishBundle();
    assertThat(operator.getCurrentOutputWatermark(), is(1L));

    // Bundle finished, watermark can be freely advanced
    testHarness.processWatermark(2);
    assertThat(operator.getEffectiveInputWatermark(), is(2L));
    assertThat(operator.getCurrentOutputWatermark(), is(2L));

    // Trigger a new bundle
    testHarness.processElement(new StreamRecord<>(windowedValue));
    assertThat(testHarness.numEventTimeTimers(), is(1)); // cleanup timer

    // Set at timer
    Instant timerTarget = new Instant(5);
    Instant timerTarget2 = new Instant(6);
    operator.getLockToAcquireForStateAccessDuringBundles().lock();

    BiConsumer<String, Instant> timerConsumer =
        (timerId, timestamp) ->
            operator.setTimer(
                Timer.of(
                    windowedValue.getValue().getKey(),
                    "",
                    windowedValue.getWindows(),
                    timestamp,
                    timestamp,
                    PaneInfo.NO_FIRING),
                TimerInternals.TimerData.of(
                    TimerReceiverFactory.encodeToTimerDataTimerId("transform", timerId),
                    "",
                    StateNamespaces.window(IntervalWindow.getCoder(), intervalWindow),
                    timestamp,
                    timestamp,
                    TimeDomain.EVENT_TIME));

    timerConsumer.accept("timer", timerTarget);
    timerConsumer.accept("timer2", timerTarget2);
    assertThat(testHarness.numEventTimeTimers(), is(3));

    // Advance input watermark past the timer
    // Check the output watermark is held back
    long targetWatermark = timerTarget.getMillis() + 100;
    testHarness.processWatermark(targetWatermark);
    // Do not yet advance the output watermark because we are still processing a bundle
    assertThat(testHarness.numEventTimeTimers(), is(3));
    assertThat(operator.getCurrentOutputWatermark(), is(2L));

    // Check that the timers are fired but the output watermark is advanced no further than
    // the minimum timer timestamp of the previous bundle because we are still processing a
    // bundle which might contain more timers.
    // Timers can create loops if they keep rescheduling themselves when firing
    // Thus, we advance the watermark asynchronously to allow for checkpointing to run
    operator.invokeFinishBundle();
    assertThat(testHarness.numEventTimeTimers(), is(3));
    testHarness.setProcessingTime(testHarness.getProcessingTime() + 1);
    assertThat(testHarness.numEventTimeTimers(), is(0));
    assertThat(operator.getCurrentOutputWatermark(), is(5L));

    // Output watermark is advanced synchronously when the bundle finishes,
    // no more timers are scheduled
    operator.invokeFinishBundle();
    assertThat(operator.getCurrentOutputWatermark(), is(targetWatermark));
    assertThat(testHarness.numEventTimeTimers(), is(0));

    // Watermark is advanced in a blocking fashion on close, not via a timers
    // Create a bundle with a pending timer to simulate that
    testHarness.processElement(new StreamRecord<>(windowedValue));
    timerConsumer.accept("timer3", new Instant(targetWatermark));
    assertThat(testHarness.numEventTimeTimers(), is(1));

    // This should be blocking until the watermark reaches Long.MAX_VALUE.
    testHarness.close();
    assertThat(testHarness.numEventTimeTimers(), is(0));
    assertThat(operator.getCurrentOutputWatermark(), is(Long.MAX_VALUE));
  }

  @Test
  public void testStageBundleClosed() throws Exception {
    TupleTag<Integer> mainOutput = new TupleTag<>("main-output");
    DoFnOperator.MultiOutputOutputManagerFactory<Integer> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(mainOutput, VoidCoder.of());
    ExecutableStageDoFnOperator<Integer, Integer> operator =
        getOperator(mainOutput, Collections.emptyList(), outputManagerFactory);

    OneInputStreamOperatorTestHarness<WindowedValue<Integer>, WindowedValue<Integer>> testHarness =
        new OneInputStreamOperatorTestHarness<>(operator);

    RemoteBundle bundle = Mockito.mock(RemoteBundle.class);
    when(bundle.getInputReceivers())
        .thenReturn(
            ImmutableMap.<String, FnDataReceiver<WindowedValue>>builder()
                .put("input", Mockito.mock(FnDataReceiver.class))
                .build());
    when(stageBundleFactory.getBundle(any(), any(), any(), any())).thenReturn(bundle);

    testHarness.open();
    testHarness.close();

    verify(stageBundleFactory).close();
    verify(stageContext).close();
    verifyNoMoreInteractions(stageBundleFactory);

    // close() will also call dispose(), but call again to verify no new bundle
    // is created afterwards
    operator.dispose();
    verifyNoMoreInteractions(bundle);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEnsureStateCleanupWithKeyedInput() throws Exception {
    TupleTag<Integer> mainOutput = new TupleTag<>("main-output");
    DoFnOperator.MultiOutputOutputManagerFactory<Integer> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(mainOutput, VarIntCoder.of());
    VarIntCoder keyCoder = VarIntCoder.of();
    ExecutableStageDoFnOperator<Integer, Integer> operator =
        getOperator(
            mainOutput,
            Collections.emptyList(),
            outputManagerFactory,
            WindowingStrategy.globalDefault(),
            keyCoder,
            WindowedValue.getFullCoder(keyCoder, GlobalWindow.Coder.INSTANCE));

    KeyedOneInputStreamOperatorTestHarness<Integer, WindowedValue<Integer>, WindowedValue<Integer>>
        testHarness =
            new KeyedOneInputStreamOperatorTestHarness(
                operator, val -> val, new CoderTypeInformation<>(keyCoder));

    RemoteBundle bundle = Mockito.mock(RemoteBundle.class);
    when(bundle.getInputReceivers())
        .thenReturn(
            ImmutableMap.<String, FnDataReceiver<WindowedValue>>builder()
                .put("input", Mockito.mock(FnDataReceiver.class))
                .build());
    when(stageBundleFactory.getBundle(any(), any(), any(), any())).thenReturn(bundle);

    testHarness.open();

    Object doFnRunner = Whitebox.getInternalState(operator, "doFnRunner");
    assertThat(doFnRunner, instanceOf(DoFnRunnerWithMetricsUpdate.class));

    // There should be a StatefulDoFnRunner installed which takes care of clearing state
    Object statefulDoFnRunner = Whitebox.getInternalState(doFnRunner, "delegate");
    assertThat(statefulDoFnRunner, instanceOf(StatefulDoFnRunner.class));
  }

  @Test
  public void testEnsureStateCleanupWithKeyedInputCleanupTimer() {
    InMemoryTimerInternals inMemoryTimerInternals = new InMemoryTimerInternals();
    KeyedStateBackend keyedStateBackend = Mockito.mock(KeyedStateBackend.class);
    Lock stateBackendLock = Mockito.mock(Lock.class);
    StringUtf8Coder keyCoder = StringUtf8Coder.of();
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();

    // Test that cleanup timer is set correctly
    ExecutableStageDoFnOperator.CleanupTimer cleanupTimer =
        new ExecutableStageDoFnOperator.CleanupTimer<>(
            inMemoryTimerInternals,
            stateBackendLock,
            WindowingStrategy.globalDefault(),
            keyCoder,
            windowCoder,
            keyedStateBackend);
    cleanupTimer.setForWindow(KV.of("key", "string"), window);

    Mockito.verify(stateBackendLock).lock();
    ByteBuffer key = FlinkKeyUtils.encodeKey("key", keyCoder);
    Mockito.verify(keyedStateBackend).setCurrentKey(key);
    assertThat(
        inMemoryTimerInternals.getNextTimer(TimeDomain.EVENT_TIME),
        is(window.maxTimestamp().plus(1)));
    Mockito.verify(stateBackendLock).unlock();
  }

  @Test
  public void testEnsureStateCleanupWithKeyedInputStateCleaner() throws Exception {
    GlobalWindow.Coder windowCoder = GlobalWindow.Coder.INSTANCE;
    InMemoryStateInternals<String> stateInternals = InMemoryStateInternals.forKey("key");
    List<String> userStateNames = ImmutableList.of("state1", "state2");
    ImmutableList.Builder<BagState<String>> bagStateBuilder = ImmutableList.builder();
    for (String userStateName : userStateNames) {
      BagState<String> state =
          stateInternals.state(
              StateNamespaces.window(windowCoder, GlobalWindow.INSTANCE),
              StateTags.bag(userStateName, StringUtf8Coder.of()));
      bagStateBuilder.add(state);
      state.add("this should be cleaned");
    }
    ImmutableList<BagState<String>> bagStates = bagStateBuilder.build();

    MutableObject<ByteBuffer> key =
        new MutableObject<>(
            ByteBuffer.wrap(stateInternals.getKey().getBytes(StandardCharsets.UTF_8)));

    // Test that state is cleaned up correctly
    ExecutableStageDoFnOperator.StateCleaner stateCleaner =
        new ExecutableStageDoFnOperator.StateCleaner(
            userStateNames, windowCoder, key::getValue, ts -> false, null);
    for (BagState<String> bagState : bagStates) {
      assertThat(Iterables.size(bagState.read()), is(1));
    }

    stateCleaner.clearForWindow(GlobalWindow.INSTANCE);
    stateCleaner.cleanupState(stateInternals, key::setValue);

    for (BagState<String> bagState : bagStates) {
      assertThat(Iterables.size(bagState.read()), is(0));
    }
  }

  @Test
  public void testEnsureDeferredStateCleanupTimerFiring() throws Exception {
    testEnsureDeferredStateCleanupTimerFiring(false);
  }

  @Test
  public void testEnsureDeferredStateCleanupTimerFiringWithCheckpointing() throws Exception {
    testEnsureDeferredStateCleanupTimerFiring(true);
  }

  private void testEnsureDeferredStateCleanupTimerFiring(boolean withCheckpointing)
      throws Exception {
    TupleTag<Integer> mainOutput = new TupleTag<>("main-output");
    DoFnOperator.MultiOutputOutputManagerFactory<Integer> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(mainOutput, VoidCoder.of());
    StringUtf8Coder keyCoder = StringUtf8Coder.of();

    WindowingStrategy windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(1000)));

    KvCoder<String, Integer> kvCoder = KvCoder.of(keyCoder, VarIntCoder.of());
    ExecutableStageDoFnOperator<Integer, Integer> operator =
        getOperator(
            mainOutput,
            Collections.emptyList(),
            outputManagerFactory,
            windowingStrategy,
            keyCoder,
            WindowedValue.getFullCoder(kvCoder, windowingStrategy.getWindowFn().windowCoder()));

    @SuppressWarnings("unchecked")
    RemoteBundle bundle = Mockito.mock(RemoteBundle.class);
    when(stageBundleFactory.getBundle(any(), any(), any(), any())).thenReturn(bundle);

    KV<String, String> timerInputKey = KV.of("transformId", "timerId");
    AtomicBoolean timerInputReceived = new AtomicBoolean();
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(1000));
    IntervalWindow.IntervalWindowCoder windowCoder = IntervalWindow.IntervalWindowCoder.of();
    WindowedValue<KV<String, Integer>> windowedValue =
        WindowedValue.of(
            KV.of("one", 1), window.maxTimestamp(), ImmutableList.of(window), PaneInfo.NO_FIRING);

    FnDataReceiver receiver = Mockito.mock(FnDataReceiver.class);
    FnDataReceiver<Timer> timerReceiver = Mockito.mock(FnDataReceiver.class);
    doAnswer(
            (invocation) -> {
              timerInputReceived.set(true);
              return null;
            })
        .when(timerReceiver)
        .accept(any());

    when(bundle.getInputReceivers()).thenReturn(ImmutableMap.of("input", receiver));
    when(bundle.getTimerReceivers()).thenReturn(ImmutableMap.of(timerInputKey, timerReceiver));

    KeyedOneInputStreamOperatorTestHarness<
            ByteBuffer, WindowedValue<KV<String, Integer>>, WindowedValue<Integer>>
        testHarness =
            new KeyedOneInputStreamOperatorTestHarness(
                operator,
                operator.keySelector,
                new CoderTypeInformation<>(FlinkKeyUtils.ByteBufferCoder.of()));

    testHarness.open();

    Lock stateBackendLock = Whitebox.getInternalState(operator, "stateBackendLock");
    stateBackendLock.lock();

    KeyedStateBackend<ByteBuffer> keyedStateBackend = operator.getKeyedStateBackend();
    ByteBuffer key = FlinkKeyUtils.encodeKey(windowedValue.getValue().getKey(), keyCoder);
    keyedStateBackend.setCurrentKey(key);

    DoFnOperator.FlinkTimerInternals timerInternals =
        Whitebox.getInternalState(operator, "timerInternals");

    Object doFnRunner = Whitebox.getInternalState(operator, "doFnRunner");
    Object delegate = Whitebox.getInternalState(doFnRunner, "delegate");
    Object stateCleaner = Whitebox.getInternalState(delegate, "stateCleaner");
    Collection<?> cleanupQueue = Whitebox.getInternalState(stateCleaner, "cleanupQueue");

    // create some state which can be cleaned up
    assertThat(testHarness.numKeyedStateEntries(), is(0));
    StateNamespace stateNamespace = StateNamespaces.window(windowCoder, window);
    BagState<ByteString> state = // State from the SDK Harness is stored as ByteStrings
        operator.keyedStateInternals.state(
            stateNamespace, StateTags.bag(stateId, ByteStringCoder.of()));
    state.add(ByteString.copyFrom("userstate".getBytes(Charsets.UTF_8)));
    assertThat(testHarness.numKeyedStateEntries(), is(1));

    // user timer that fires after the end of the window and after state cleanup
    TimerInternals.TimerData userTimer =
        TimerInternals.TimerData.of(
            TimerReceiverFactory.encodeToTimerDataTimerId(
                timerInputKey.getKey(), timerInputKey.getValue()),
            stateNamespace,
            window.maxTimestamp(),
            window.maxTimestamp(),
            TimeDomain.EVENT_TIME);
    timerInternals.setTimer(userTimer);

    // start of bundle
    testHarness.processElement(new StreamRecord<>(windowedValue));
    verify(receiver).accept(windowedValue);

    // move watermark past user timer while bundle is in progress
    testHarness.processWatermark(new Watermark(window.maxTimestamp().plus(1).getMillis()));

    // Output watermark is held back and timers do not yet fire (they can still be changed!)
    assertThat(timerInputReceived.get(), is(false));
    assertThat(
        operator.getCurrentOutputWatermark(), is(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis()));
    // The timer fires on bundle finish
    operator.invokeFinishBundle();
    assertThat(timerInputReceived.getAndSet(false), is(true));

    // Move watermark past the cleanup timer
    testHarness.processWatermark(new Watermark(window.maxTimestamp().plus(2).getMillis()));
    operator.invokeFinishBundle();

    // Cleanup timer has fired and cleanup queue is prepared for bundle finish
    assertThat(testHarness.numEventTimeTimers(), is(0));
    assertThat(testHarness.numKeyedStateEntries(), is(1));
    assertThat(cleanupQueue, hasSize(1));

    // Cleanup timer are rescheduled if a new timer is created during the bundle
    TimerInternals.TimerData userTimer2 =
        TimerInternals.TimerData.of(
            TimerReceiverFactory.encodeToTimerDataTimerId(
                timerInputKey.getKey(), timerInputKey.getValue()),
            stateNamespace,
            window.maxTimestamp(),
            window.maxTimestamp(),
            TimeDomain.EVENT_TIME);
    operator.setTimer(
        Timer.of(
            windowedValue.getValue().getKey(),
            "",
            windowedValue.getWindows(),
            window.maxTimestamp(),
            window.maxTimestamp(),
            PaneInfo.NO_FIRING),
        userTimer2);
    assertThat(testHarness.numEventTimeTimers(), is(1));

    if (withCheckpointing) {
      // Upon checkpointing, the bundle will be finished.
      testHarness.snapshot(0, 0);
    } else {
      operator.invokeFinishBundle();
    }

    // Cleanup queue has been processed and cleanup timer has been re-added due to pending timers
    // for the window.
    assertThat(cleanupQueue, hasSize(0));
    verifyNoMoreInteractions(receiver);
    assertThat(testHarness.numKeyedStateEntries(), is(2));
    assertThat(testHarness.numEventTimeTimers(), is(2));

    // No timer has been fired but bundle should be ended
    assertThat(timerInputReceived.get(), is(false));
    assertThat(Whitebox.getInternalState(operator, "bundleStarted"), is(false));

    // Allow user timer and cleanup timer to fire by triggering watermark advancement
    testHarness.setProcessingTime(testHarness.getProcessingTime() + 1);
    assertThat(timerInputReceived.getAndSet(false), is(true));
    assertThat(cleanupQueue, hasSize(1));

    // Cleanup will be executed after the bundle is complete because there are no more pending
    // timers for the window
    operator.invokeFinishBundle();
    assertThat(cleanupQueue, hasSize(0));
    assertThat(testHarness.numKeyedStateEntries(), is(0));

    testHarness.close();
    verifyNoMoreInteractions(receiver);
  }

  @Test
  public void testEnsureStateCleanupOnFinalWatermark() throws Exception {
    TupleTag<Integer> mainOutput = new TupleTag<>("main-output");
    DoFnOperator.MultiOutputOutputManagerFactory<Integer> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(mainOutput, VoidCoder.of());

    StringUtf8Coder keyCoder = StringUtf8Coder.of();

    WindowingStrategy windowingStrategy = WindowingStrategy.globalDefault();
    Coder<BoundedWindow> windowCoder = windowingStrategy.getWindowFn().windowCoder();

    KvCoder<String, Integer> kvCoder = KvCoder.of(keyCoder, VarIntCoder.of());
    ExecutableStageDoFnOperator<Integer, Integer> operator =
        getOperator(
            mainOutput,
            Collections.emptyList(),
            outputManagerFactory,
            windowingStrategy,
            keyCoder,
            WindowedValue.getFullCoder(kvCoder, windowCoder));

    KeyedOneInputStreamOperatorTestHarness<
            ByteBuffer, WindowedValue<KV<String, Integer>>, WindowedValue<Integer>>
        testHarness =
            new KeyedOneInputStreamOperatorTestHarness(
                operator,
                operator.keySelector,
                new CoderTypeInformation<>(FlinkKeyUtils.ByteBufferCoder.of()));

    RemoteBundle bundle = Mockito.mock(RemoteBundle.class);
    when(bundle.getInputReceivers())
        .thenReturn(
            ImmutableMap.<String, FnDataReceiver<WindowedValue>>builder()
                .put("input", Mockito.mock(FnDataReceiver.class))
                .build());
    when(stageBundleFactory.getBundle(any(), any(), any(), any())).thenReturn(bundle);

    testHarness.open();

    KeyedStateBackend<ByteBuffer> keyedStateBackend = operator.getKeyedStateBackend();
    ByteBuffer key = FlinkKeyUtils.encodeKey("key1", keyCoder);
    keyedStateBackend.setCurrentKey(key);

    // create some state which can be cleaned up
    assertThat(testHarness.numKeyedStateEntries(), is(0));
    StateNamespace stateNamespace = StateNamespaces.window(windowCoder, GlobalWindow.INSTANCE);
    BagState<ByteString> state = // State from the SDK Harness is stored as ByteStrings
        operator.keyedStateInternals.state(
            stateNamespace, StateTags.bag(stateId, ByteStringCoder.of()));
    state.add(ByteString.copyFrom("userstate".getBytes(Charsets.UTF_8)));
    // No timers have been set for cleanup
    assertThat(testHarness.numEventTimeTimers(), is(0));
    // State has been created
    assertThat(testHarness.numKeyedStateEntries(), is(1));

    // Generate final watermark to trigger state cleanup
    testHarness.processWatermark(
        new Watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.plus(1).getMillis()));

    assertThat(testHarness.numKeyedStateEntries(), is(0));
  }

  @Test
  public void testCacheTokenHandling() throws Exception {
    InMemoryStateInternals test = InMemoryStateInternals.forKey("test");
    KeyedStateBackend<ByteBuffer> stateBackend = FlinkStateInternalsTest.createStateBackend();

    ExecutableStageDoFnOperator.BagUserStateFactory<Integer, GlobalWindow> bagUserStateFactory =
        new ExecutableStageDoFnOperator.BagUserStateFactory<>(
            test, stateBackend, NoopLock.get(), null);

    ByteString key1 = ByteString.copyFrom("key1", Charsets.UTF_8);
    ByteString key2 = ByteString.copyFrom("key2", Charsets.UTF_8);

    Map<String, Map<String, ProcessBundleDescriptors.BagUserStateSpec>> userStateMapMock =
        Mockito.mock(Map.class);
    Map<String, ProcessBundleDescriptors.BagUserStateSpec> transformMap = Mockito.mock(Map.class);

    final String userState1 = "userstate1";
    ProcessBundleDescriptors.BagUserStateSpec bagUserStateSpec1 = mockBagUserState(userState1);
    when(transformMap.get(userState1)).thenReturn(bagUserStateSpec1);

    final String userState2 = "userstate2";
    ProcessBundleDescriptors.BagUserStateSpec bagUserStateSpec2 = mockBagUserState(userState2);
    when(transformMap.get(userState2)).thenReturn(bagUserStateSpec2);

    when(userStateMapMock.get(anyString())).thenReturn(transformMap);
    when(processBundleDescriptor.getBagUserStateSpecs()).thenReturn(userStateMapMock);
    StateRequestHandler stateRequestHandler =
        StateRequestHandlers.forBagUserStateHandlerFactory(
            processBundleDescriptor, bagUserStateFactory);

    // User state the cache token is valid for the lifetime of the operator
    final BeamFnApi.ProcessBundleRequest.CacheToken expectedCacheToken =
        Iterables.getOnlyElement(stateRequestHandler.getCacheTokens());

    // Make a request to generate initial cache token
    stateRequestHandler.handle(getRequest(key1, userState1));
    BeamFnApi.ProcessBundleRequest.CacheToken returnedCacheToken =
        Iterables.getOnlyElement(stateRequestHandler.getCacheTokens());
    assertThat(returnedCacheToken.hasUserState(), is(true));
    assertThat(returnedCacheToken, is(expectedCacheToken));

    List<RequestGenerator> generators =
        Arrays.asList(
            ExecutableStageDoFnOperatorTest::getRequest,
            ExecutableStageDoFnOperatorTest::getAppend,
            ExecutableStageDoFnOperatorTest::getClear);

    for (RequestGenerator req : generators) {
      // For every state read the tokens remains unchanged
      stateRequestHandler.handle(req.makeRequest(key1, userState1));
      assertThat(
          Iterables.getOnlyElement(stateRequestHandler.getCacheTokens()), is(expectedCacheToken));

      // The token is still valid for another key in the same key range
      stateRequestHandler.handle(req.makeRequest(key2, userState1));
      assertThat(
          Iterables.getOnlyElement(stateRequestHandler.getCacheTokens()), is(expectedCacheToken));

      // The token is still valid for another state cell in the same key range
      stateRequestHandler.handle(req.makeRequest(key2, userState2));
      assertThat(
          Iterables.getOnlyElement(stateRequestHandler.getCacheTokens()), is(expectedCacheToken));
    }
  }

  private interface RequestGenerator {
    BeamFnApi.StateRequest makeRequest(ByteString key, String userStateId) throws Exception;
  }

  private static BeamFnApi.StateRequest getRequest(ByteString key, String userStateId)
      throws Exception {
    BeamFnApi.StateRequest.Builder builder = stateRequest(key, userStateId);
    builder.setGet(BeamFnApi.StateGetRequest.newBuilder().build());
    return builder.build();
  }

  private static BeamFnApi.StateRequest getAppend(ByteString key, String userStateId)
      throws Exception {
    BeamFnApi.StateRequest.Builder builder = stateRequest(key, userStateId);
    builder.setAppend(BeamFnApi.StateAppendRequest.newBuilder().build());
    return builder.build();
  }

  private static BeamFnApi.StateRequest getClear(ByteString key, String userStateId)
      throws Exception {
    BeamFnApi.StateRequest.Builder builder = stateRequest(key, userStateId);
    builder.setClear(BeamFnApi.StateClearRequest.newBuilder().build());
    return builder.build();
  }

  private static BeamFnApi.StateRequest.Builder stateRequest(ByteString key, String userStateId)
      throws Exception {
    return BeamFnApi.StateRequest.newBuilder()
        .setStateKey(
            BeamFnApi.StateKey.newBuilder()
                .setBagUserState(
                    BeamFnApi.StateKey.BagUserState.newBuilder()
                        .setTransformId("transform")
                        .setKey(key)
                        .setUserStateId(userStateId)
                        .setWindow(
                            ByteString.copyFrom(
                                CoderUtils.encodeToByteArray(
                                    GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE)))
                        .build()));
  }

  private static ProcessBundleDescriptors.BagUserStateSpec mockBagUserState(String userStateId) {
    ProcessBundleDescriptors.BagUserStateSpec bagUserStateMock =
        Mockito.mock(ProcessBundleDescriptors.BagUserStateSpec.class);
    when(bagUserStateMock.keyCoder()).thenReturn(ByteStringCoder.of());
    when(bagUserStateMock.valueCoder()).thenReturn(ByteStringCoder.of());
    when(bagUserStateMock.transformId()).thenReturn("transformId");
    when(bagUserStateMock.userStateId()).thenReturn(userStateId);
    when(bagUserStateMock.windowCoder()).thenReturn(GlobalWindow.Coder.INSTANCE);
    return bagUserStateMock;
  }

  @Test
  public void testSerialization() {
    WindowedValue.ValueOnlyWindowedValueCoder<Integer> coder =
        WindowedValue.getValueOnlyCoder(VarIntCoder.of());

    TupleTag<Integer> mainOutput = new TupleTag<>("main-output");
    TupleTag<Integer> additionalOutput = new TupleTag<>("additional-output");
    ImmutableMap<TupleTag<?>, OutputTag<?>> tagsToOutputTags =
        ImmutableMap.<TupleTag<?>, OutputTag<?>>builder()
            .put(
                additionalOutput,
                new OutputTag<>(additionalOutput.getId(), TypeInformation.of(Integer.class)))
            .build();
    ImmutableMap<TupleTag<?>, Coder<WindowedValue<?>>> tagsToCoders =
        ImmutableMap.<TupleTag<?>, Coder<WindowedValue<?>>>builder()
            .put(mainOutput, (Coder) coder)
            .put(additionalOutput, coder)
            .build();
    ImmutableMap<TupleTag<?>, Integer> tagsToIds =
        ImmutableMap.<TupleTag<?>, Integer>builder()
            .put(mainOutput, 0)
            .put(additionalOutput, 1)
            .build();

    DoFnOperator.MultiOutputOutputManagerFactory<Integer> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory(
            mainOutput, tagsToOutputTags, tagsToCoders, tagsToIds);

    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);

    ExecutableStageDoFnOperator<Integer, Integer> operator =
        new ExecutableStageDoFnOperator<>(
            "transform",
            WindowedValue.getValueOnlyCoder(VarIntCoder.of()),
            Collections.emptyMap(),
            mainOutput,
            ImmutableList.of(additionalOutput),
            outputManagerFactory,
            Collections.emptyMap() /* sideInputTagMapping */,
            Collections.emptyList() /* sideInputs */,
            Collections.emptyMap() /* sideInputId mapping */,
            options,
            stagePayload,
            jobInfo,
            FlinkExecutableStageContextFactory.getInstance(),
            createOutputMap(mainOutput, ImmutableList.of(additionalOutput)),
            WindowingStrategy.globalDefault(),
            null,
            null);

    ExecutableStageDoFnOperator<Integer, Integer> clone = SerializationUtils.clone(operator);
    assertNotNull(clone);
    assertNotEquals(operator, clone);
  }

  /**
   * Creates a {@link ExecutableStageDoFnOperator}. Sets the runtime context to {@link
   * #runtimeContext}. The context factory is mocked to return {@link #stageContext} every time. The
   * behavior of the stage context itself is unchanged.
   */
  @SuppressWarnings("rawtypes")
  private ExecutableStageDoFnOperator getOperator(
      TupleTag<Integer> mainOutput,
      List<TupleTag<?>> additionalOutputs,
      DoFnOperator.MultiOutputOutputManagerFactory<Integer> outputManagerFactory) {
    return getOperator(
        mainOutput,
        additionalOutputs,
        outputManagerFactory,
        WindowingStrategy.globalDefault(),
        null,
        WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE));
  }

  @SuppressWarnings("rawtypes")
  private ExecutableStageDoFnOperator getOperator(
      TupleTag<Integer> mainOutput,
      List<TupleTag<?>> additionalOutputs,
      DoFnOperator.MultiOutputOutputManagerFactory<Integer> outputManagerFactory,
      WindowingStrategy windowingStrategy,
      @Nullable Coder keyCoder,
      Coder windowedInputCoder) {

    FlinkExecutableStageContextFactory contextFactory =
        Mockito.mock(FlinkExecutableStageContextFactory.class);
    when(contextFactory.get(any())).thenReturn(stageContext);

    final ExecutableStagePayload stagePayload;
    if (keyCoder != null) {
      stagePayload = this.stagePayloadWithUserState;
    } else {
      stagePayload = this.stagePayload;
    }

    ExecutableStageDoFnOperator<Integer, Integer> operator =
        new ExecutableStageDoFnOperator<>(
            "transform",
            windowedInputCoder,
            Collections.emptyMap(),
            mainOutput,
            additionalOutputs,
            outputManagerFactory,
            Collections.emptyMap() /* sideInputTagMapping */,
            Collections.emptyList() /* sideInputs */,
            Collections.emptyMap() /* sideInputId mapping */,
            PipelineOptionsFactory.as(FlinkPipelineOptions.class),
            stagePayload,
            jobInfo,
            contextFactory,
            createOutputMap(mainOutput, additionalOutputs),
            windowingStrategy,
            keyCoder,
            keyCoder != null ? new KvToByteBufferKeySelector<>(keyCoder) : null);

    Whitebox.setInternalState(operator, "stateRequestHandler", stateRequestHandler);
    return operator;
  }

  private static Map<String, TupleTag<?>> createOutputMap(
      TupleTag mainOutput, List<TupleTag<?>> additionalOutputs) {
    Map<String, TupleTag<?>> outputMap = new HashMap<>(additionalOutputs.size() + 1);
    if (mainOutput != null) {
      outputMap.put(mainOutput.getId(), mainOutput);
    }
    for (TupleTag<?> additionalTag : additionalOutputs) {
      outputMap.put(additionalTag.getId(), additionalTag);
    }
    return outputMap;
  }
}
