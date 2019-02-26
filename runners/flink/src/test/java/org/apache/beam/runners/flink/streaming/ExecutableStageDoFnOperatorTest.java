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
package org.apache.beam.runners.flink.streaming;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageContext;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.ExecutableStageDoFnOperator;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;

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

    FlinkExecutableStageContext.Factory contextFactory =
        Mockito.mock(FlinkExecutableStageContext.Factory.class);
    when(contextFactory.get(any())).thenReturn(stageContext);

    ExecutableStageDoFnOperator<Integer, Integer> operator =
        new ExecutableStageDoFnOperator<>(
            "transform",
            null,
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
            WindowingStrategy.globalDefault(),
            null,
            null);

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
