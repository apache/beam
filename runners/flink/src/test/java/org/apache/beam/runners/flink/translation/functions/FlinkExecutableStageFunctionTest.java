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
package org.apache.beam.runners.flink.translation.functions;

import static org.apache.beam.runners.core.construction.PTransformTranslation.PAR_DO_TRANSFORM_URN;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.reflect.Whitebox;

/** Tests for {@link FlinkExecutableStageFunction}. */
@RunWith(Parameterized.class)
public class FlinkExecutableStageFunctionTest {

  @Parameterized.Parameters
  public static Object[] data() {
    return new Object[] {true, false};
  }

  @Parameterized.Parameter public boolean isStateful;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private RuntimeContext runtimeContext;
  @Mock private DistributedCache distributedCache;
  @Mock private Collector<RawUnionValue> collector;
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
                  .putTransforms(
                      "transform",
                      RunnerApi.PTransform.newBuilder()
                          .putInputs("bla", "input")
                          .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(PAR_DO_TRANSFORM_URN))
                          .build())
                  .putPcollections("input", PCollection.getDefaultInstance())
                  .build())
          .addUserStates(
              ExecutableStagePayload.UserStateId.newBuilder().setTransformId("transform").build())
          .build();
  private final JobInfo jobInfo =
      JobInfo.create("job-id", "job-name", "retrieval-token", Struct.getDefaultInstance());

  @Before
  public void setUpMocks() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(runtimeContext.getDistributedCache()).thenReturn(distributedCache);
    when(stageContext.getStageBundleFactory(any())).thenReturn(stageBundleFactory);
    RemoteBundle remoteBundle = Mockito.mock(RemoteBundle.class);
    when(stageBundleFactory.getBundle(any(), any(), any())).thenReturn(remoteBundle);
    ImmutableMap input =
        ImmutableMap.builder().put("input", Mockito.mock(FnDataReceiver.class)).build();
    when(remoteBundle.getInputReceivers()).thenReturn(input);
    when(processBundleDescriptor.getTimerSpecs()).thenReturn(Collections.emptyMap());
  }

  @Test
  public void sdkErrorsSurfaceOnClose() throws Exception {
    FlinkExecutableStageFunction<Integer> function = getFunction(Collections.emptyMap());
    function.open(new Configuration());

    @SuppressWarnings("unchecked")
    RemoteBundle bundle = Mockito.mock(RemoteBundle.class);
    when(stageBundleFactory.getBundle(any(), any(), any())).thenReturn(bundle);

    @SuppressWarnings("unchecked")
    FnDataReceiver<WindowedValue<?>> receiver = Mockito.mock(FnDataReceiver.class);
    when(bundle.getInputReceivers()).thenReturn(ImmutableMap.of("input", receiver));

    Exception expected = new Exception();
    doThrow(expected).when(bundle).close();
    thrown.expect(is(expected));
    function.mapPartition(Collections.emptyList(), collector);
  }

  @Test
  public void expectedInputsAreSent() throws Exception {
    FlinkExecutableStageFunction<Integer> function = getFunction(Collections.emptyMap());
    function.open(new Configuration());

    @SuppressWarnings("unchecked")
    RemoteBundle bundle = Mockito.mock(RemoteBundle.class);
    when(stageBundleFactory.getBundle(any(), any(), any())).thenReturn(bundle);

    @SuppressWarnings("unchecked")
    FnDataReceiver<WindowedValue<?>> receiver = Mockito.mock(FnDataReceiver.class);
    when(bundle.getInputReceivers()).thenReturn(ImmutableMap.of("input", receiver));

    WindowedValue<Integer> one = WindowedValue.valueInGlobalWindow(1);
    WindowedValue<Integer> two = WindowedValue.valueInGlobalWindow(2);
    WindowedValue<Integer> three = WindowedValue.valueInGlobalWindow(3);
    function.mapPartition(Arrays.asList(one, two, three), collector);

    verify(receiver).accept(one);
    verify(receiver).accept(two);
    verify(receiver).accept(three);
    verifyNoMoreInteractions(receiver);
  }

  @Test
  public void outputsAreTaggedCorrectly() throws Exception {
    WindowedValue<Integer> three = WindowedValue.valueInGlobalWindow(3);
    WindowedValue<Integer> four = WindowedValue.valueInGlobalWindow(4);
    WindowedValue<Integer> five = WindowedValue.valueInGlobalWindow(5);
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
            return processBundleDescriptor;
          }

          @Override
          public void close() throws Exception {}
        };
    // Wire the stage bundle factory into our context.
    when(stageContext.getStageBundleFactory(any())).thenReturn(stageBundleFactory);

    FlinkExecutableStageFunction<Integer> function = getFunction(outputTagMap);
    function.open(new Configuration());

    if (isStateful) {
      function.reduce(Collections.emptyList(), collector);
    } else {
      function.mapPartition(Collections.emptyList(), collector);
    }
    // Ensure that the tagged values sent to the collector have the correct union tags as specified
    // in the output map.
    verify(collector).collect(new RawUnionValue(1, three));
    verify(collector).collect(new RawUnionValue(2, four));
    verify(collector).collect(new RawUnionValue(3, five));
    verifyNoMoreInteractions(collector);
  }

  @Test
  public void testStageBundleClosed() throws Exception {
    FlinkExecutableStageFunction<Integer> function = getFunction(Collections.emptyMap());
    function.open(new Configuration());
    function.close();
    verify(stageBundleFactory).getProcessBundleDescriptor();
    verify(stageBundleFactory).close();
    verifyNoMoreInteractions(stageBundleFactory);
  }

  /**
   * Creates a {@link FlinkExecutableStageFunction}. Sets the runtime context to {@link
   * #runtimeContext}. The context factory is mocked to return {@link #stageContext} every time. The
   * behavior of the stage context itself is unchanged.
   */
  private FlinkExecutableStageFunction<Integer> getFunction(Map<String, Integer> outputMap) {
    FlinkExecutableStageContext.Factory contextFactory =
        Mockito.mock(FlinkExecutableStageContext.Factory.class);
    when(contextFactory.get(any())).thenReturn(stageContext);
    FlinkExecutableStageFunction<Integer> function =
        new FlinkExecutableStageFunction<>(stagePayload, jobInfo, outputMap, contextFactory, null);
    function.setRuntimeContext(runtimeContext);
    Whitebox.setInternalState(function, "stateRequestHandler", stateRequestHandler);
    return function;
  }
}
