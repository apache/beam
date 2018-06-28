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

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Struct;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link FlinkExecutableStageFunction}. */
@RunWith(JUnit4.class)
public class FlinkExecutableStageFunctionTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private RuntimeContext runtimeContext;
  @Mock private DistributedCache distributedCache;
  @Mock private Collector<RawUnionValue> collector;
  @Mock private FlinkExecutableStageContext stageContext;
  @Mock private StageBundleFactory<Integer> stageBundleFactory;
  @Mock private StateRequestHandler stateRequestHandler;

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
    when(stageContext.getStateRequestHandler(any(), any())).thenReturn(stateRequestHandler);
    when(stageContext.<Integer>getStageBundleFactory(any())).thenReturn(stageBundleFactory);
  }

  @Test
  public void sdkErrorsSurfaceOnClose() throws Exception {
    FlinkExecutableStageFunction<Integer> function = getFunction(Collections.emptyMap());
    function.open(new Configuration());

    @SuppressWarnings("unchecked")
    RemoteBundle<Integer> bundle = Mockito.mock(RemoteBundle.class);
    when(stageBundleFactory.getBundle(any(), any(), any())).thenReturn(bundle);

    @SuppressWarnings("unchecked")
    FnDataReceiver<WindowedValue<Integer>> receiver = Mockito.mock(FnDataReceiver.class);
    when(bundle.getInputReceiver()).thenReturn(receiver);

    Exception expected = new Exception();
    doThrow(expected).when(bundle).close();
    thrown.expect(is(expected));
    function.mapPartition(Collections.emptyList(), collector);
  }

  @Test
  public void checksForRuntimeContextChanges() throws Exception {
    FlinkExecutableStageFunction<Integer> function = getFunction(Collections.emptyMap());
    function.open(new Configuration());
    // Change runtime context.
    function.setRuntimeContext(Mockito.mock(RuntimeContext.class));
    thrown.expect(Matchers.instanceOf(IllegalStateException.class));
    function.mapPartition(Collections.emptyList(), collector);
  }

  @Test
  public void expectedInputsAreSent() throws Exception {
    FlinkExecutableStageFunction<Integer> function = getFunction(Collections.emptyMap());
    function.open(new Configuration());

    @SuppressWarnings("unchecked")
    RemoteBundle<Integer> bundle = Mockito.mock(RemoteBundle.class);
    when(stageBundleFactory.getBundle(any(), any(), any())).thenReturn(bundle);

    @SuppressWarnings("unchecked")
    FnDataReceiver<WindowedValue<Integer>> receiver = Mockito.mock(FnDataReceiver.class);
    when(bundle.getInputReceiver()).thenReturn(receiver);

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
    StageBundleFactory<Integer> stageBundleFactory =
        new StageBundleFactory<Integer>() {
          @Override
          public RemoteBundle<Integer> getBundle(
              OutputReceiverFactory receiverFactory,
              StateRequestHandler stateRequestHandler,
              BundleProgressHandler progressHandler) {
            return new RemoteBundle<Integer>() {
              @Override
              public String getId() {
                return "bundle-id";
              }

              @Override
              public FnDataReceiver<WindowedValue<Integer>> getInputReceiver() {
                return input -> {
                  /* Ignore input*/
                };
              }

              @Override
              public void close() throws Exception {
                // Emit all values to the runner when the bundle is closed.
                receiverFactory.create("one").accept(three);
                receiverFactory.create("two").accept(four);
                receiverFactory.create("three").accept(five);
              }
            };
          }

          @Override
          public void close() throws Exception {}
        };
    // Wire the stage bundle factory into our context.
    when(stageContext.<Integer>getStageBundleFactory(any())).thenReturn(stageBundleFactory);

    FlinkExecutableStageFunction<Integer> function = getFunction(outputTagMap);
    function.open(new Configuration());

    function.mapPartition(Collections.emptyList(), collector);
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
        new FlinkExecutableStageFunction<Integer>(stagePayload, jobInfo, outputMap, contextFactory);
    function.setRuntimeContext(runtimeContext);
    return function;
  }
}
