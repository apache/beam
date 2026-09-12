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
package org.apache.beam.runners.spark.translation;

import static org.apache.beam.sdk.util.construction.PTransformTranslation.PAR_DO_TRANSFORM_URN;
import static org.apache.beam.sdk.util.construction.PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.fnexecution.control.BundleCheckpointHandler;
import org.apache.beam.runners.fnexecution.control.BundleFinalizationHandler;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.ExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.control.TimerReceiverFactory;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.spark.metrics.MetricsContainerStepMapAccumulator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.construction.Timer;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link SparkExecutableStageFunction}. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class SparkExecutableStageFunctionTest {
  @Mock private SparkExecutableStageContextFactory contextFactory;
  @Mock private ExecutableStageContext stageContext;
  @Mock private StageBundleFactory stageBundleFactory;
  @Mock private RemoteBundle remoteBundle;
  @Mock private MetricsContainerStepMapAccumulator metricsAccumulator;
  @Mock private MetricsContainerStepMap stepMap;
  @Mock private MetricsContainerImpl container;

  private final SerializablePipelineOptions pipelineOptions =
      new SerializablePipelineOptions(PipelineOptionsFactory.create());
  private final String inputId = "input-id";
  private final ExecutableStagePayload stagePayload =
      ExecutableStagePayload.newBuilder()
          .setInput(inputId)
          .setComponents(
              Components.newBuilder()
                  .putTransforms(
                      "transform-id",
                      RunnerApi.PTransform.newBuilder()
                          .putInputs("input-name", inputId)
                          .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(PAR_DO_TRANSFORM_URN))
                          .build())
                  .putPcollections(inputId, PCollection.getDefaultInstance())
                  .build())
          .build();

  private final ExecutableStagePayload sdfStagePayload =
      ExecutableStagePayload.newBuilder()
          .setInput(inputId)
          .addTransforms("sdf-transform-id")
          .setComponents(
              Components.newBuilder()
                  .putTransforms(
                      "sdf-transform-id",
                      RunnerApi.PTransform.newBuilder()
                          .putInputs("input-name", inputId)
                          .setSpec(
                              RunnerApi.FunctionSpec.newBuilder()
                                  .setUrn(SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN))
                          .build())
                  .putPcollections(inputId, PCollection.getDefaultInstance())
                  .build())
          .build();

  @Before
  public void setUpMocks() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(contextFactory.get(any())).thenReturn(stageContext);
    when(stageContext.getStageBundleFactory(any())).thenReturn(stageBundleFactory);
    when(stageBundleFactory.getBundle(
            any(), any(), any(), any(BundleProgressHandler.class), any(), any()))
        .thenReturn(remoteBundle);
    @SuppressWarnings("unchecked")
    ImmutableMap<String, FnDataReceiver> inputReceiver =
        ImmutableMap.of("input", Mockito.mock(FnDataReceiver.class));
    when(remoteBundle.getInputReceivers()).thenReturn(inputReceiver);
    when(metricsAccumulator.value()).thenReturn(stepMap);
    when(stepMap.getContainer(any())).thenReturn(container);
  }

  @Test(expected = Exception.class)
  public void sdkErrorsSurfaceOnClose() throws Exception {
    SparkExecutableStageFunction<Integer, ?> function = getFunction(Collections.emptyMap());
    doThrow(new Exception()).when(remoteBundle).close();
    List<WindowedValue<Integer>> inputs = new ArrayList<>();
    inputs.add(WindowedValues.valueInGlobalWindow(0));
    function.call(inputs.iterator());
  }

  @Test
  public void expectedInputsAreSent() throws Exception {
    SparkExecutableStageFunction<Integer, ?> function = getFunction(Collections.emptyMap());

    RemoteBundle bundle = Mockito.mock(RemoteBundle.class);
    when(stageBundleFactory.getBundle(
            any(), any(), any(), any(BundleProgressHandler.class), any(), any()))
        .thenReturn(bundle);

    @SuppressWarnings("unchecked")
    FnDataReceiver<WindowedValue<?>> receiver = Mockito.mock(FnDataReceiver.class);
    when(bundle.getInputReceivers()).thenReturn(ImmutableMap.of(inputId, receiver));

    WindowedValue<Integer> one = WindowedValues.valueInGlobalWindow(1);
    WindowedValue<Integer> two = WindowedValues.valueInGlobalWindow(2);
    WindowedValue<Integer> three = WindowedValues.valueInGlobalWindow(3);
    function.call(Arrays.asList(one, two, three).iterator());

    verify(receiver).accept(one);
    verify(receiver).accept(two);
    verify(receiver).accept(three);
    verifyNoMoreInteractions(receiver);
  }

  @Test
  public void outputsAreTaggedCorrectly() throws Exception {
    WindowedValue<Integer> three = WindowedValues.valueInGlobalWindow(3);
    WindowedValue<Integer> four = WindowedValues.valueInGlobalWindow(4);
    WindowedValue<Integer> five = WindowedValues.valueInGlobalWindow(5);
    Map<String, Integer> outputTagMap =
        ImmutableMap.of(
            "one", 1,
            "two", 2,
            "three", 3);

    // We use a real StageBundleFactory here in order to exercise the output receiver factory.
    StageBundleFactory stageBundleFactory =
        new StageBundleFactory() {

          private boolean once;

          @Override
          public RemoteBundle getBundle(
              OutputReceiverFactory receiverFactory,
              TimerReceiverFactory timerReceiverFactory,
              StateRequestHandler stateRequestHandler,
              BundleProgressHandler progressHandler,
              BundleFinalizationHandler finalizationHandler,
              BundleCheckpointHandler checkpointHandler) {
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
                if (once) {
                  return;
                }
                // Emit all values to the runner when the bundle is closed.
                receiverFactory.create("one").accept(three);
                receiverFactory.create("two").accept(four);
                receiverFactory.create("three").accept(five);
                once = true;
              }
            };
          }

          @Override
          public ProcessBundleDescriptors.ExecutableProcessBundleDescriptor
              getProcessBundleDescriptor() {
            return Mockito.mock(ProcessBundleDescriptors.ExecutableProcessBundleDescriptor.class);
          }

          @Override
          public InstructionRequestHandler getInstructionRequestHandler() {
            return null;
          }

          @Override
          public void close() {}
        };
    when(stageContext.getStageBundleFactory(any())).thenReturn(stageBundleFactory);

    SparkExecutableStageFunction<Integer, ?> function = getFunction(outputTagMap);
    List<WindowedValue<Integer>> inputs = new ArrayList<>();
    inputs.add(WindowedValues.valueInGlobalWindow(0));
    Iterator<RawUnionValue> iterator = function.call(inputs.iterator());
    Iterable<RawUnionValue> iterable = () -> iterator;

    assertThat(
        iterable,
        contains(
            new RawUnionValue(1, three), new RawUnionValue(2, four), new RawUnionValue(3, five)));
  }

  @Test
  public void testStageBundleClosed() throws Exception {
    SparkExecutableStageFunction<Integer, ?> function = getFunction(Collections.emptyMap());
    List<WindowedValue<Integer>> inputs = new ArrayList<>();
    inputs.add(WindowedValues.valueInGlobalWindow(0));
    function.call(inputs.iterator());
    verify(stageBundleFactory)
        .getBundle(any(), any(), any(), any(BundleProgressHandler.class), any(), any());
    verify(stageBundleFactory).getProcessBundleDescriptor();
    verify(stageBundleFactory).close();
    verifyNoMoreInteractions(stageBundleFactory);
  }

  @Test
  public void testNoCallOnEmptyInputIterator() throws Exception {
    SparkExecutableStageFunction<Integer, ?> function = getFunction(Collections.emptyMap());
    function.call(Collections.emptyIterator());
    verifyNoInteractions(stageBundleFactory);
  }

  @Test
  public void sdfResidualsAreReplayedUntilDrained() throws Exception {
    // A stage whose bundle self-checkpoints once: the first bundle returns a residual, the replay
    // bundle returns none.
    List<WindowedValue<?>> received = new ArrayList<>();
    WindowedValue<Integer> residualValue = WindowedValues.valueInGlobalWindow(7);
    Coder<WindowedValue<Integer>> residualCoder =
        WindowedValues.getFullCoder(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE);
    ProcessBundleResponse withResidual =
        ProcessBundleResponse.newBuilder()
            .addResidualRoots(
                DelayedBundleApplication.newBuilder()
                    .setApplication(
                        BundleApplication.newBuilder()
                            .setElement(
                                ByteString.copyFrom(
                                    CoderUtils.encodeToByteArray(residualCoder, residualValue)))))
            .build();

    StageBundleFactory bundleFactory =
        new StageBundleFactory() {
          private int bundles;

          @Override
          public RemoteBundle getBundle(
              OutputReceiverFactory receiverFactory,
              TimerReceiverFactory timerReceiverFactory,
              StateRequestHandler stateRequestHandler,
              BundleProgressHandler progressHandler,
              BundleFinalizationHandler finalizationHandler,
              BundleCheckpointHandler checkpointHandler) {
            boolean checkpointThisBundle = bundles++ == 0;
            return new RemoteBundle() {
              @Override
              public String getId() {
                return "bundle-id";
              }

              @Override
              public Map<String, FnDataReceiver> getInputReceivers() {
                FnDataReceiver<WindowedValue<?>> receiver = received::add;
                return ImmutableMap.of("input", receiver);
              }

              @Override
              public Map<KV<String, String>, FnDataReceiver<Timer>> getTimerReceivers() {
                return Collections.emptyMap();
              }

              @Override
              public void requestProgress() {}

              @Override
              public void split(double fractionOfRemainder) {}

              @Override
              public void close() {
                if (checkpointThisBundle) {
                  checkpointHandler.onCheckpoint(withResidual);
                }
              }
            };
          }

          @Override
          public ProcessBundleDescriptors.ExecutableProcessBundleDescriptor
              getProcessBundleDescriptor() {
            return null;
          }

          @Override
          public InstructionRequestHandler getInstructionRequestHandler() {
            return null;
          }

          @Override
          public void close() {}
        };
    when(stageContext.getStageBundleFactory(any())).thenReturn(bundleFactory);

    SparkExecutableStageFunction<Integer, ?> function =
        new SparkExecutableStageFunction<>(
            pipelineOptions,
            sdfStagePayload,
            null,
            Collections.emptyMap(),
            contextFactory,
            Collections.emptyMap(),
            metricsAccumulator,
            GlobalWindow.Coder.INSTANCE,
            residualCoder,
            true);

    function.call(Collections.singletonList(WindowedValues.valueInGlobalWindow(1)).iterator());

    assertThat(received, contains(WindowedValues.valueInGlobalWindow(1), residualValue));
  }

  private <InputT, SideInputT> SparkExecutableStageFunction<InputT, SideInputT> getFunction(
      Map<String, Integer> outputMap) {
    return new SparkExecutableStageFunction<>(
        pipelineOptions,
        stagePayload,
        null,
        outputMap,
        contextFactory,
        Collections.emptyMap(),
        metricsAccumulator,
        null,
        null,
        true);
  }
}
