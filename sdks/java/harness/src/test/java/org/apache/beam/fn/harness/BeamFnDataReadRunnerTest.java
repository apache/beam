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
package org.apache.beam.fn.harness;

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.fn.harness.PTransformRunnerFactory.Registrar;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitRequest.DesiredSplit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitResponse.ChannelSplit;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.CompletableFutureInboundDataClient;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link BeamFnDataReadRunner}. */
@RunWith(Enclosed.class)
public class BeamFnDataReadRunnerTest {
  private static final Coder<String> ELEMENT_CODER = StringUtf8Coder.of();
  private static final String ELEMENT_CODER_SPEC_ID = "string-coder-id";
  private static final Coder<WindowedValue<String>> CODER =
      WindowedValue.getFullCoder(ELEMENT_CODER, GlobalWindow.Coder.INSTANCE);
  private static final String CODER_SPEC_ID = "windowed-string-coder-id";
  private static final RunnerApi.Coder CODER_SPEC;
  private static final RunnerApi.Components COMPONENTS;
  private static final BeamFnApi.RemoteGrpcPort PORT_SPEC =
      BeamFnApi.RemoteGrpcPort.newBuilder()
          .setApiServiceDescriptor(Endpoints.ApiServiceDescriptor.getDefaultInstance())
          .setCoderId(CODER_SPEC_ID)
          .build();

  static {
    try {
      MessageWithComponents coderAndComponents = CoderTranslation.toProto(CODER);
      CODER_SPEC = coderAndComponents.getCoder();
      COMPONENTS =
          coderAndComponents
              .getComponents()
              .toBuilder()
              .putCoders(CODER_SPEC_ID, CODER_SPEC)
              .putCoders(ELEMENT_CODER_SPEC_ID, CoderTranslation.toProto(ELEMENT_CODER).getCoder())
              .build();
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static final String INPUT_TRANSFORM_ID = "1";

  private static final String PTRANSFORM_ID = "ptransform_id";

  // Test basic executions of BeamFnDataReadRunner.
  @RunWith(JUnit4.class)
  public static class BeamFnDataReadRunnerExecutionTest {
    @Rule public TestExecutorService executor = TestExecutors.from(Executors::newCachedThreadPool);
    @Mock private BeamFnDataClient mockBeamFnDataClient;
    @Captor private ArgumentCaptor<FnDataReceiver<WindowedValue<String>>> consumerCaptor;

    @Before
    public void setUp() {
      MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreatingAndProcessingBeamFnDataReadRunner() throws Exception {
      String bundleId = "57";

      List<WindowedValue<String>> outputValues = new ArrayList<>();

      MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
      PCollectionConsumerRegistry consumers =
          new PCollectionConsumerRegistry(
              metricsContainerRegistry, mock(ExecutionStateTracker.class));
      String localOutputId = "outputPC";
      String pTransformId = "pTransformId";
      consumers.register(
          localOutputId,
          pTransformId,
          (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) outputValues::add);
      PTransformFunctionRegistry startFunctionRegistry =
          new PTransformFunctionRegistry(
              mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
      PTransformFunctionRegistry finishFunctionRegistry =
          new PTransformFunctionRegistry(
              mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
      List<ThrowingRunnable> resetFunctions = new ArrayList<>();
      List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

      RunnerApi.PTransform pTransform =
          RemoteGrpcPortRead.readFromPort(PORT_SPEC, localOutputId).toPTransform();

      new BeamFnDataReadRunner.Factory<String>()
          .createRunnerForPTransform(
              PipelineOptionsFactory.create(),
              mockBeamFnDataClient,
              null /* beamFnStateClient */,
              null /* beamFnTimerClient */,
              pTransformId,
              pTransform,
              Suppliers.ofInstance(bundleId)::get,
              ImmutableMap.of(
                  localOutputId,
                  RunnerApi.PCollection.newBuilder().setCoderId(ELEMENT_CODER_SPEC_ID).build()),
              COMPONENTS.getCodersMap(),
              COMPONENTS.getWindowingStrategiesMap(),
              consumers,
              startFunctionRegistry,
              finishFunctionRegistry,
              resetFunctions::add,
              teardownFunctions::add,
              (PTransformRunnerFactory.ProgressRequestCallback callback) -> {},
              null /* splitListener */,
              null /* bundleFinalizer */);

      assertThat(teardownFunctions, empty());

      verifyZeroInteractions(mockBeamFnDataClient);

      InboundDataClient completionFuture = CompletableFutureInboundDataClient.create();
      when(mockBeamFnDataClient.receive(any(), any(), any(), any())).thenReturn(completionFuture);
      Iterables.getOnlyElement(startFunctionRegistry.getFunctions()).run();
      verify(mockBeamFnDataClient)
          .receive(
              eq(PORT_SPEC.getApiServiceDescriptor()),
              eq(LogicalEndpoint.data(bundleId, pTransformId)),
              eq(CODER),
              consumerCaptor.capture());

      consumerCaptor.getValue().accept(valueInGlobalWindow("TestValue"));
      assertThat(outputValues, contains(valueInGlobalWindow("TestValue")));
      outputValues.clear();

      assertThat(consumers.keySet(), containsInAnyOrder(localOutputId));

      completionFuture.complete();
      Iterables.getOnlyElement(finishFunctionRegistry.getFunctions()).run();

      verifyNoMoreInteractions(mockBeamFnDataClient);
    }

    @Test
    public void testReuseForMultipleBundles() throws Exception {
      InboundDataClient bundle1Future = CompletableFutureInboundDataClient.create();
      InboundDataClient bundle2Future = CompletableFutureInboundDataClient.create();
      when(mockBeamFnDataClient.receive(any(), any(), any(), any()))
          .thenReturn(bundle1Future)
          .thenReturn(bundle2Future);
      List<WindowedValue<String>> values = new ArrayList<>();
      FnDataReceiver<WindowedValue<String>> consumers = values::add;
      AtomicReference<String> bundleId = new AtomicReference<>("0");
      List<PTransformRunnerFactory.ProgressRequestCallback> progressCallbacks = new ArrayList<>();
      BeamFnDataReadRunner<String> readRunner =
          new BeamFnDataReadRunner<>(
              INPUT_TRANSFORM_ID,
              RemoteGrpcPortRead.readFromPort(PORT_SPEC, "localOutput").toPTransform(),
              bundleId::get,
              COMPONENTS.getCodersMap(),
              mockBeamFnDataClient,
              null /* beamFnStateClient */,
              (PTransformRunnerFactory.ProgressRequestCallback callback) -> {
                progressCallbacks.add(callback);
              },
              consumers);

      // Process for bundle id 0
      readRunner.registerInputLocation();

      assertEquals(
          createReadIndexMonitoringInfoAt(-1),
          Iterables.getOnlyElement(
              Iterables.getOnlyElement(progressCallbacks).getMonitoringInfos()));

      verify(mockBeamFnDataClient)
          .receive(
              eq(PORT_SPEC.getApiServiceDescriptor()),
              eq(LogicalEndpoint.data(bundleId.get(), INPUT_TRANSFORM_ID)),
              eq(CODER),
              consumerCaptor.capture());

      Future<?> future =
          executor.submit(
              () -> {
                // Sleep for some small amount of time simulating the parent blocking
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                try {
                  consumerCaptor.getValue().accept(valueInGlobalWindow("ABC"));
                  assertEquals(
                      createReadIndexMonitoringInfoAt(0),
                      Iterables.getOnlyElement(
                          Iterables.getOnlyElement(progressCallbacks).getMonitoringInfos()));
                  consumerCaptor.getValue().accept(valueInGlobalWindow("DEF"));
                  assertEquals(
                      createReadIndexMonitoringInfoAt(1),
                      Iterables.getOnlyElement(
                          Iterables.getOnlyElement(progressCallbacks).getMonitoringInfos()));
                } catch (Exception e) {
                  bundle1Future.fail(e);
                } finally {
                  bundle1Future.complete();
                }
              });

      readRunner.blockTillReadFinishes();
      future.get();
      assertEquals(
          createReadIndexMonitoringInfoAt(2),
          Iterables.getOnlyElement(
              Iterables.getOnlyElement(progressCallbacks).getMonitoringInfos()));
      assertThat(values, contains(valueInGlobalWindow("ABC"), valueInGlobalWindow("DEF")));

      // Process for bundle id 1
      bundleId.set("1");
      values.clear();
      readRunner.registerInputLocation();
      // Ensure that when we reuse the BeamFnDataReadRunner the read index is reset to -1
      assertEquals(
          createReadIndexMonitoringInfoAt(-1),
          Iterables.getOnlyElement(
              Iterables.getOnlyElement(progressCallbacks).getMonitoringInfos()));

      verify(mockBeamFnDataClient)
          .receive(
              eq(PORT_SPEC.getApiServiceDescriptor()),
              eq(LogicalEndpoint.data(bundleId.get(), INPUT_TRANSFORM_ID)),
              eq(CODER),
              consumerCaptor.capture());

      future =
          executor.submit(
              () -> {
                // Sleep for some small amount of time simulating the parent blocking
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                try {
                  consumerCaptor.getValue().accept(valueInGlobalWindow("GHI"));
                  assertEquals(
                      createReadIndexMonitoringInfoAt(0),
                      Iterables.getOnlyElement(
                          Iterables.getOnlyElement(progressCallbacks).getMonitoringInfos()));
                  consumerCaptor.getValue().accept(valueInGlobalWindow("JKL"));
                  assertEquals(
                      createReadIndexMonitoringInfoAt(1),
                      Iterables.getOnlyElement(
                          Iterables.getOnlyElement(progressCallbacks).getMonitoringInfos()));
                } catch (Exception e) {
                  bundle2Future.fail(e);
                } finally {
                  bundle2Future.complete();
                }
              });

      readRunner.blockTillReadFinishes();
      future.get();
      assertEquals(
          createReadIndexMonitoringInfoAt(2),
          Iterables.getOnlyElement(
              Iterables.getOnlyElement(progressCallbacks).getMonitoringInfos()));
      assertThat(values, contains(valueInGlobalWindow("GHI"), valueInGlobalWindow("JKL")));

      verifyNoMoreInteractions(mockBeamFnDataClient);
    }

    @Test
    public void testRegistration() {
      for (Registrar registrar : ServiceLoader.load(Registrar.class)) {
        if (registrar instanceof BeamFnDataReadRunner.Registrar) {
          assertThat(
              registrar.getPTransformRunnerFactories(),
              IsMapContaining.hasKey(RemoteGrpcPortRead.URN));
          return;
        }
      }
      fail("Expected registrar not found.");
    }

    @Test
    public void testSplittingWhenNoElementsProcessed() throws Exception {
      List<WindowedValue<String>> outputValues = new ArrayList<>();
      BeamFnDataReadRunner<String> readRunner =
          createReadRunner(outputValues::add, PTRANSFORM_ID, mockBeamFnDataClient);
      readRunner.registerInputLocation();
      // The split should happen at 5 since the allowedSplitPoints is empty.
      assertEquals(
          channelSplitResult(5),
          executeSplit(readRunner, PTRANSFORM_ID, -1L, 0.5, 10, Collections.EMPTY_LIST));

      // Ensure that we process the correct number of elements after splitting.
      readRunner.forwardElementToConsumer(valueInGlobalWindow("A"));
      readRunner.forwardElementToConsumer(valueInGlobalWindow("B"));
      readRunner.forwardElementToConsumer(valueInGlobalWindow("C"));
      readRunner.forwardElementToConsumer(valueInGlobalWindow("D"));
      readRunner.forwardElementToConsumer(valueInGlobalWindow("E"));
      readRunner.forwardElementToConsumer(valueInGlobalWindow("F"));
      readRunner.forwardElementToConsumer(valueInGlobalWindow("G"));
      assertThat(
          outputValues,
          contains(
              valueInGlobalWindow("A"),
              valueInGlobalWindow("B"),
              valueInGlobalWindow("C"),
              valueInGlobalWindow("D"),
              valueInGlobalWindow("E")));
    }

    @Test
    public void testSplittingWhenSomeElementsProcessed() throws Exception {
      List<WindowedValue<String>> outputValues = new ArrayList<>();
      BeamFnDataReadRunner<String> readRunner =
          createReadRunner(outputValues::add, PTRANSFORM_ID, mockBeamFnDataClient);
      readRunner.registerInputLocation();
      assertEquals(
          channelSplitResult(6),
          executeSplit(readRunner, PTRANSFORM_ID, 1L, 0.5, 10, Collections.EMPTY_LIST));

      // Ensure that we process the correct number of elements after splitting.
      readRunner.forwardElementToConsumer(valueInGlobalWindow("1"));
      readRunner.forwardElementToConsumer(valueInGlobalWindow("2"));
      readRunner.forwardElementToConsumer(valueInGlobalWindow("3"));
      readRunner.forwardElementToConsumer(valueInGlobalWindow("4"));
      readRunner.forwardElementToConsumer(valueInGlobalWindow("5"));
      assertThat(
          outputValues,
          contains(
              valueInGlobalWindow("-1"),
              valueInGlobalWindow("0"),
              valueInGlobalWindow("1"),
              valueInGlobalWindow("2"),
              valueInGlobalWindow("3"),
              valueInGlobalWindow("4")));
    }
  }

  // Test different cases of chan nel split with empty allowed split points.
  @RunWith(Parameterized.class)
  public static class ChannelSplitTest {

    @Parameterized.Parameters
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          // Split as close to the beginning as possible.
          .add(new Object[] {channelSplitResult(1L), 0L, 0, 0, 16L})
          // The closest split is at 4, even when just above or below it.
          .add(new Object[] {channelSplitResult(4L), 0L, 0, 0.24, 16L})
          .add(new Object[] {channelSplitResult(4L), 0L, 0, 0.25, 16L})
          .add(new Object[] {channelSplitResult(4L), 0L, 0, 0.26, 16L})
          // Split the *remainder* in half.
          .add(new Object[] {channelSplitResult(8L), 0L, 0, 0.5, 16L})
          .add(new Object[] {channelSplitResult(9L), 2, 0, 0.5, 16L})
          .add(new Object[] {channelSplitResult(11L), 6L, 0, 0.5, 16L})
          // Progress into the active element influences where the split of the remainder falls.
          .add(new Object[] {channelSplitResult(1L), 0L, 0.5, 0.25, 4L})
          .add(new Object[] {channelSplitResult(2L), 0L, 0.9, 0.25, 4L})
          .add(new Object[] {channelSplitResult(2L), 1L, 0, 0.25, 4L})
          .add(new Object[] {channelSplitResult(2L), 1L, 0.1, 0.25, 4L})
          .build();
    }

    @Parameterized.Parameter(0)
    public ProcessBundleSplitResponse expectedResponse;

    @Parameterized.Parameter(1)
    public long index;

    @Parameterized.Parameter(2)
    public double elementProgress;

    @Parameterized.Parameter(3)
    public double fractionOfRemainder;

    @Parameterized.Parameter(4)
    public long bufferSize;

    @Test
    public void testChannelSplit() throws Exception {
      SplittingReceiver splittingReceiver = mock(SplittingReceiver.class);
      BeamFnDataClient mockBeamFnDataClient = mock(BeamFnDataClient.class);
      when(splittingReceiver.getProgress()).thenReturn(elementProgress);
      BeamFnDataReadRunner<String> readRunner =
          createReadRunner(splittingReceiver, PTRANSFORM_ID, mockBeamFnDataClient);
      readRunner.registerInputLocation();
      assertEquals(
          expectedResponse,
          executeSplit(
              readRunner,
              PTRANSFORM_ID,
              index,
              fractionOfRemainder,
              bufferSize,
              Collections.EMPTY_LIST));
    }
  }

  // Test different cases of channel split with non-empty allowed split points.
  @RunWith(Parameterized.class)
  public static class ChannelSplitWithAllowedSplitPointsTest {
    @Parameterized.Parameters
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          // The desired split point is at 4.
          .add(
              new Object[] {
                channelSplitResult(4L), 0L, 0.25, 16L, ImmutableList.of(2L, 3L, 4L, 5L)
              })
          // If we can't split at 4, choose the closest possible split point.
          .add(new Object[] {channelSplitResult(5L), 0L, 0.25, 16L, ImmutableList.of(2L, 3L, 5L)})
          .add(new Object[] {channelSplitResult(3L), 0L, 0.25, 16L, ImmutableList.of(2L, 3L, 6L)})
          // Also test the case where all possible split points lie above or below the desired split
          // point.
          .add(new Object[] {channelSplitResult(5L), 0L, 0.25, 16L, ImmutableList.of(5L, 6L, 7L)})
          .add(new Object[] {channelSplitResult(3L), 0L, 0.25, 16L, ImmutableList.of(1L, 2L, 3L)})
          // We have progressed beyond all possible split points, so can't split.
          .add(
              new Object[] {
                ProcessBundleSplitResponse.getDefaultInstance(),
                5L,
                0.25,
                16L,
                ImmutableList.of(1L, 2L, 3L)
              })
          .build();
    }

    @Parameterized.Parameter(0)
    public ProcessBundleSplitResponse expectedResponse;

    @Parameterized.Parameter(1)
    public long index;

    @Parameterized.Parameter(2)
    public double fractionOfRemainder;

    @Parameterized.Parameter(3)
    public long bufferSize;

    @Parameterized.Parameter(4)
    public List<Long> allowedSplitPoints;

    @Test
    public void testChannelSplittingWithAllowedSplitPoints() throws Exception {
      List<WindowedValue<String>> outputValues = new ArrayList<>();
      BeamFnDataClient mockBeamFnDataClient = mock(BeamFnDataClient.class);
      BeamFnDataReadRunner<String> readRunner =
          createReadRunner(outputValues::add, PTRANSFORM_ID, mockBeamFnDataClient);
      readRunner.registerInputLocation();
      assertEquals(
          expectedResponse,
          executeSplit(
              readRunner,
              PTRANSFORM_ID,
              index,
              fractionOfRemainder,
              bufferSize,
              allowedSplitPoints));
    }
  }

  // Test different cases of element split with empty allowed split points.
  @RunWith(Parameterized.class)
  public static class ElementSplitTest {
    @Parameterized.Parameters
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          // Split between future elements at element boundaries.
          .add(new Object[] {channelSplitResult(2L), 0L, 0, 0.51, 4L})
          .add(new Object[] {channelSplitResult(2L), 0L, 0, 0.49, 4L})
          .add(new Object[] {channelSplitResult(1L), 0L, 0, 0.26, 4L})
          .add(new Object[] {channelSplitResult(1L), 0L, 0, 0.25, 4L})
          // If the split falls inside the first, splittable element, split there.
          .add(new Object[] {elementSplitResult(0L, 0.8), 0L, 0, 0.2, 4L})
          // The choice of split depends on the progress into the first element.
          .add(new Object[] {elementSplitResult(0L, 0.5), 0L, 0, 0.125, 4L})
          // Here we are far enough into the first element that splitting at 0.2 of the remainder
          // falls outside the first element.
          .add(new Object[] {channelSplitResult(1L), 0L, 0.5, 0.2, 4L})
          // Verify the above logic when we are partially through the stream.
          .add(new Object[] {channelSplitResult(3L), 2L, 0, 0.6, 4L})
          .add(new Object[] {channelSplitResult(4L), 2L, 0.9, 0.6, 4L})
          .add(new Object[] {elementSplitResult(2L, 0.6), 2L, 0.5, 0.2, 4L})
          .build();
    }

    @Parameterized.Parameter(0)
    public ProcessBundleSplitResponse expectedResponse;

    @Parameterized.Parameter(1)
    public long index;

    @Parameterized.Parameter(2)
    public double elementProgress;

    @Parameterized.Parameter(3)
    public double fractionOfRemainder;

    @Parameterized.Parameter(4)
    public long bufferSize;

    @Test
    public void testElementSplit() throws Exception {
      SplittingReceiver splittingReceiver = mock(SplittingReceiver.class);
      BeamFnDataClient mockBeamFnDataClient = mock(BeamFnDataClient.class);
      when(splittingReceiver.getProgress()).thenReturn(elementProgress);
      when(splittingReceiver.trySplit(anyDouble())).thenCallRealMethod();
      BeamFnDataReadRunner<String> readRunner =
          createReadRunner(splittingReceiver, PTRANSFORM_ID, mockBeamFnDataClient);
      readRunner.registerInputLocation();

      assertEquals(
          expectedResponse,
          executeSplit(
              readRunner,
              PTRANSFORM_ID,
              index,
              fractionOfRemainder,
              bufferSize,
              Collections.EMPTY_LIST));
    }
  }

  // Test different cases of element split with non-empty allowed split points.
  @RunWith(Parameterized.class)
  public static class ElementSplitWithAllowedSplitPointsTest {
    @Parameterized.Parameters
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          // This is where we would like to split, when all split points are available.
          .add(
              new Object[] {
                elementSplitResult(2L, 0.6), 2L, 0, 0.2, 5L, ImmutableList.of(1L, 2L, 3L, 4L, 5L)
              })
          // This is where we would like to split, when all split points are available.
          .add(
              new Object[] {
                channelSplitResult(4L), 2L, 0, 0.2, 5L, ImmutableList.of(1L, 2L, 4L, 5L)
              })
          // We can't even split element at index 4 as above, because 4 is also not a split point.
          .add(new Object[] {channelSplitResult(5L), 2L, 0, 0.2, 5L, ImmutableList.of(1L, 2L, 5L)})
          // We can't split element at index 2, because 2 is not a split point.
          .add(
              new Object[] {
                channelSplitResult(3L), 2L, 0, 0.2, 5L, ImmutableList.of(1L, 3L, 4L, 5L)
              })
          .build();
    }

    @Parameterized.Parameter(0)
    public ProcessBundleSplitResponse expectedResponse;

    @Parameterized.Parameter(1)
    public long index;

    @Parameterized.Parameter(2)
    public double elementProgress;

    @Parameterized.Parameter(3)
    public double fractionOfRemainder;

    @Parameterized.Parameter(4)
    public long bufferSize;

    @Parameterized.Parameter(5)
    public List<Long> allowedSplitPoints;

    @Test
    public void testElementSplittingWithAllowedSplitPoints() throws Exception {
      SplittingReceiver splittingReceiver = mock(SplittingReceiver.class);
      BeamFnDataClient mockBeamFnDataClient = mock(BeamFnDataClient.class);
      when(splittingReceiver.getProgress()).thenReturn(elementProgress);
      when(splittingReceiver.trySplit(anyDouble())).thenCallRealMethod();
      BeamFnDataReadRunner<String> readRunner =
          createReadRunner(splittingReceiver, PTRANSFORM_ID, mockBeamFnDataClient);
      readRunner.registerInputLocation();
      assertEquals(
          expectedResponse,
          executeSplit(
              readRunner,
              PTRANSFORM_ID,
              index,
              fractionOfRemainder,
              bufferSize,
              allowedSplitPoints));
    }
  }

  private abstract static class SplittingReceiver
      implements FnDataReceiver<WindowedValue<String>>, HandlesSplits {
    @Override
    public SplitResult trySplit(double fractionOfRemainder) {
      return SplitResult.of(
          Collections.singletonList(
              BundleApplication.newBuilder()
                  .setInputId(String.format("primary%.1f", fractionOfRemainder))
                  .build()),
          Collections.singletonList(
              DelayedBundleApplication.newBuilder()
                  .setApplication(
                      BundleApplication.newBuilder()
                          .setInputId(String.format("residual%.1f", 1 - fractionOfRemainder))
                          .build())
                  .build()));
    }
  }

  private static BeamFnDataReadRunner<String> createReadRunner(
      FnDataReceiver<WindowedValue<String>> consumer,
      String pTransformId,
      BeamFnDataClient dataClient)
      throws Exception {
    String bundleId = "57";

    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry consumers =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    String localOutputId = "outputPC";
    consumers.register(localOutputId, pTransformId, consumer);
    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "start");
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            mock(MetricsContainerStepMap.class), mock(ExecutionStateTracker.class), "finish");
    List<ThrowingRunnable> resetFunctions = new ArrayList<>();
    List<ThrowingRunnable> teardownFunctions = new ArrayList<>();

    RunnerApi.PTransform pTransform =
        RemoteGrpcPortRead.readFromPort(PORT_SPEC, localOutputId).toPTransform();

    return new BeamFnDataReadRunner.Factory<String>()
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            dataClient,
            null /* beamFnStateClient */,
            null /* beamFnTimerClient */,
            pTransformId,
            pTransform,
            Suppliers.ofInstance(bundleId)::get,
            ImmutableMap.of(
                localOutputId,
                RunnerApi.PCollection.newBuilder().setCoderId(ELEMENT_CODER_SPEC_ID).build()),
            COMPONENTS.getCodersMap(),
            COMPONENTS.getWindowingStrategiesMap(),
            consumers,
            startFunctionRegistry,
            finishFunctionRegistry,
            resetFunctions::add,
            teardownFunctions::add,
            (PTransformRunnerFactory.ProgressRequestCallback callback) -> {},
            null /* splitListener */,
            null /* bundleFinalizer */);
  }

  private static MonitoringInfo createReadIndexMonitoringInfoAt(int index) {
    return new SimpleMonitoringInfoBuilder()
        .setUrn(MonitoringInfoConstants.Urns.DATA_CHANNEL_READ_INDEX)
        .setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, INPUT_TRANSFORM_ID)
        .setInt64SumValue(index)
        .build();
  }

  private static ProcessBundleSplitResponse executeSplit(
      BeamFnDataReadRunner<String> readRunner,
      String pTransformId,
      long index,
      double fractionOfRemainder,
      long inputElements,
      List<Long> allowedSplitPoints)
      throws Exception {
    for (long i = -1; i < index; i++) {
      readRunner.forwardElementToConsumer(valueInGlobalWindow(Long.valueOf(i).toString()));
    }
    ProcessBundleSplitRequest request =
        ProcessBundleSplitRequest.newBuilder()
            .putDesiredSplits(
                pTransformId,
                DesiredSplit.newBuilder()
                    .setEstimatedInputElements(inputElements)
                    .setFractionOfRemainder(fractionOfRemainder)
                    .addAllAllowedSplitPoints(allowedSplitPoints)
                    .build())
            .build();
    ProcessBundleSplitResponse.Builder responseBuilder = ProcessBundleSplitResponse.newBuilder();
    readRunner.trySplit(request, responseBuilder);
    return responseBuilder.build();
  }

  private static ProcessBundleSplitResponse channelSplitResult(long firstResidualIndex) {
    return ProcessBundleSplitResponse.newBuilder()
        .addChannelSplits(
            ChannelSplit.newBuilder()
                .setLastPrimaryElement(firstResidualIndex - 1)
                .setFirstResidualElement(firstResidualIndex)
                .build())
        .build();
  }

  private static ProcessBundleSplitResponse elementSplitResult(
      long index, double fractionOfRemainder) {
    return ProcessBundleSplitResponse.newBuilder()
        .addPrimaryRoots(
            BundleApplication.newBuilder()
                .setInputId(String.format("primary%.1f", fractionOfRemainder))
                .build())
        .addResidualRoots(
            DelayedBundleApplication.newBuilder()
                .setApplication(
                    BundleApplication.newBuilder()
                        .setInputId(String.format("residual%.1f", 1 - fractionOfRemainder))
                        .build())
                .build())
        .addChannelSplits(
            ChannelSplit.newBuilder()
                .setLastPrimaryElement(index - 1)
                .setFirstResidualElement(index + 1)
                .build())
        .build();
  }
}
