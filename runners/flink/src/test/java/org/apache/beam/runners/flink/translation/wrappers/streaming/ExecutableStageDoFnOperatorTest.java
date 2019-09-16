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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.metrics.DoFnRunnerWithMetricsUpdate;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageContext;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
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
  @Mock private FlinkExecutableStageContext stageContext;
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
    when(stageBundleFactory.getBundle(any(), any(), any())).thenReturn(bundle);

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
    when(stageBundleFactory.getBundle(any(), any(), any())).thenReturn(bundle);

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
            .put(additionalOutput1, new OutputTag<String>(additionalOutput1.getId()) {})
            .put(additionalOutput2, new OutputTag<String>(additionalOutput2.getId()) {})
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
              StateRequestHandler stateRequestHandler,
              BundleProgressHandler progressHandler) {
            return new RemoteBundle() {
              @Override
              public String getId() {
                return "bundle-id";
              }

              @Override
              public Map<String, FnDataReceiver<WindowedValue<?>>> getInputReceivers() {
                return ImmutableMap.of(
                    "input",
                    input -> {
                      /* Ignore input*/
                    });
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

    assertThat(
        testHarness.getOutput(),
        contains(
            new StreamRecord<>(three), new Watermark(watermark), new Watermark(Long.MAX_VALUE)));

    assertThat(
        testHarness.getSideOutput(tagsToOutputTags.get(additionalOutput1)),
        contains(new StreamRecord<>(four)));

    assertThat(
        testHarness.getSideOutput(tagsToOutputTags.get(additionalOutput2)),
        contains(new StreamRecord<>(five)));
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
    when(stageBundleFactory.getBundle(any(), any(), any())).thenReturn(bundle);

    testHarness.open();
    testHarness.close();

    verify(stageBundleFactory).getProcessBundleDescriptor();
    verify(stageBundleFactory).close();
    verify(stageContext).close();
    // DoFnOperator generates a final watermark, which triggers a new bundle..
    verify(stageBundleFactory).getBundle(any(), any(), any());
    verify(bundle).getInputReceivers();
    verify(bundle).close();
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
    when(stageBundleFactory.getBundle(any(), any(), any())).thenReturn(bundle);

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
    GlobalWindow window = GlobalWindow.INSTANCE;
    GlobalWindow.Coder windowCoder = GlobalWindow.Coder.INSTANCE;

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
  public void testEnsureStateCleanupWithKeyedInputStateCleaner() {
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
            userStateNames, windowCoder, () -> key.getValue());
    for (BagState<String> bagState : bagStates) {
      assertThat(Iterables.size(bagState.read()), is(1));
    }

    stateCleaner.clearForWindow(GlobalWindow.INSTANCE);
    stateCleaner.cleanupState(stateInternals, (k) -> key.setValue(k));

    for (BagState<String> bagState : bagStates) {
      assertThat(Iterables.size(bagState.read()), is(0));
    }
  }

  @Test
  public void testEnsureDeferredStateCleanupTimerFiring() throws Exception {
    testEnsureDeferredStateCleanupTimerFiring(false);
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
    when(stageBundleFactory.getBundle(any(), any(), any())).thenReturn(bundle);

    String timerInputId = "timerInput";
    AtomicBoolean timerInputReceived = new AtomicBoolean();
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(1000));
    IntervalWindow.IntervalWindowCoder windowCoder = IntervalWindow.IntervalWindowCoder.of();
    WindowedValue<KV<String, Integer>> one =
        WindowedValue.of(
            KV.of("one", 1), window.maxTimestamp(), ImmutableList.of(window), PaneInfo.NO_FIRING);

    FnDataReceiver<WindowedValue<?>> receiver = Mockito.mock(FnDataReceiver.class);
    FnDataReceiver<WindowedValue<?>> timerReceiver = Mockito.mock(FnDataReceiver.class);
    doAnswer(
            (invocation) -> {
              timerInputReceived.set(true);
              return null;
            })
        .when(timerReceiver)
        .accept(any());

    when(bundle.getInputReceivers())
        .thenReturn(ImmutableMap.of("input", receiver, timerInputId, timerReceiver));

    KeyedOneInputStreamOperatorTestHarness<
            String, WindowedValue<KV<String, Integer>>, WindowedValue<KV<String, Integer>>>
        testHarness =
            new KeyedOneInputStreamOperatorTestHarness(
                operator, val -> val, new CoderTypeInformation<>(keyCoder));

    testHarness.open();

    Lock stateBackendLock = (Lock) Whitebox.getInternalState(operator, "stateBackendLock");
    stateBackendLock.lock();

    KeyedStateBackend<ByteBuffer> keyedStateBackend = operator.getKeyedStateBackend();
    ByteBuffer key = FlinkKeyUtils.encodeKey(one.getValue().getKey(), keyCoder);
    keyedStateBackend.setCurrentKey(key);

    DoFnOperator.FlinkTimerInternals timerInternals =
        (DoFnOperator.FlinkTimerInternals) Whitebox.getInternalState(operator, "timerInternals");

    Object doFnRunner = Whitebox.getInternalState(operator, "doFnRunner");
    Object delegate = Whitebox.getInternalState(doFnRunner, "delegate");
    Object stateCleaner = Whitebox.getInternalState(delegate, "stateCleaner");
    Collection<?> cleanupTimers =
        (Collection) Whitebox.getInternalState(stateCleaner, "cleanupQueue");

    // create some state which can be cleaned up
    assertThat(testHarness.numKeyedStateEntries(), is(0));
    StateNamespace stateNamespace = StateNamespaces.window(windowCoder, window);
    BagState<String> state =
        operator.keyedStateInternals.state(
            stateNamespace, StateTags.bag(stateId, StringUtf8Coder.of()));
    state.add("testUserState");
    assertThat(testHarness.numKeyedStateEntries(), is(1));

    // user timer that fires after the end of the window and after state cleanup
    TimerInternals.TimerData userTimer =
        TimerInternals.TimerData.of(
            timerInputId, stateNamespace, window.maxTimestamp().plus(1), TimeDomain.EVENT_TIME);
    timerInternals.setTimer(userTimer);

    // start of bundle
    testHarness.processElement(new StreamRecord<>(one));
    verify(receiver).accept(one);

    // move watermark past cleanup and user timer while bundle in progress
    operator.processWatermark(new Watermark(window.maxTimestamp().plus(2).getMillis()));

    // due to watermark hold the timers won't fire at this point
    assertFalse("Watermark must be held back until bundle is complete.", timerInputReceived.get());
    assertThat(cleanupTimers, hasSize(0));

    if (withCheckpointing) {
      // Upon checkpointing, the bundle is finished and the watermark advances;
      // timers can fire. Note: The bundle is ensured to be finished.
      testHarness.snapshot(0, 0);

      // The user timer was scheduled to fire after cleanup, but executes first
      assertTrue("Timer should have been triggered.", timerInputReceived.get());
      // Cleanup will be executed after the bundle is complete
      assertThat(cleanupTimers, hasSize(0));
      verifyNoMoreInteractions(receiver);
    } else {
      // Upon finishing a bundle, the watermark advances; timers can fire.
      // Note that this will finish the current bundle, but will also start a new one
      // when timers fire as part of advancing the watermark
      operator.invokeFinishBundle();

      // The user timer was scheduled to fire after cleanup, but executes first
      assertTrue("Timer should have been triggered.", timerInputReceived.get());
      // Cleanup will be executed after the bundle is complete
      assertThat(cleanupTimers, hasSize(1));
      verifyNoMoreInteractions(receiver);

      // Finish bundle which has been started by finishing the bundle
      operator.invokeFinishBundle();
      assertThat(cleanupTimers, hasSize(0));
    }

    assertThat(testHarness.numKeyedStateEntries(), is(0));

    testHarness.close();
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
            null,
            null,
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
            FlinkExecutableStageContext.factory(options),
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
  private ExecutableStageDoFnOperator<Integer, Integer> getOperator(
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

  private ExecutableStageDoFnOperator<Integer, Integer> getOperator(
      TupleTag<Integer> mainOutput,
      List<TupleTag<?>> additionalOutputs,
      DoFnOperator.MultiOutputOutputManagerFactory<Integer> outputManagerFactory,
      WindowingStrategy windowingStrategy,
      @Nullable Coder keyCoder,
      @Nullable Coder windowedInputCoder) {

    FlinkExecutableStageContext.Factory contextFactory =
        Mockito.mock(FlinkExecutableStageContext.Factory.class);
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
            null,
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
