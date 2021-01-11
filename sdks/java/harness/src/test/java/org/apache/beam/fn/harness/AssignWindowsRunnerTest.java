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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.apache.beam.fn.harness.AssignWindowsRunner.AssignWindowsMapFnFactory;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PartitioningWindowFn;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link org.apache.beam.fn.harness.AssignWindowsRunner}. */
@RunWith(JUnit4.class)
public class AssignWindowsRunnerTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  private transient AssignWindowsRunner.AssignWindowsMapFnFactory<?> factory =
      new AssignWindowsRunner.AssignWindowsMapFnFactory<>();

  @Test
  public void singleInputSingleOutputSucceeds() throws Exception {
    FixedWindows windowFn = FixedWindows.of(Duration.standardMinutes(10L));

    AssignWindowsRunner<Integer, IntervalWindow> runner = AssignWindowsRunner.create(windowFn);

    assertThat(
        runner.assignWindows(WindowedValue.valueInGlobalWindow(1)),
        equalTo(
            WindowedValue.of(
                1,
                BoundedWindow.TIMESTAMP_MIN_VALUE,
                windowFn.assignWindow(BoundedWindow.TIMESTAMP_MIN_VALUE),
                PaneInfo.NO_FIRING)));
    assertThat(
        runner.assignWindows(
            WindowedValue.of(
                2,
                new Instant(-10L),
                new IntervalWindow(new Instant(-120000L), Duration.standardMinutes(3L)),
                PaneInfo.ON_TIME_AND_ONLY_FIRING)),
        equalTo(
            WindowedValue.of(
                2,
                new Instant(-10L),
                windowFn.assignWindow(new Instant(-10L)),
                PaneInfo.ON_TIME_AND_ONLY_FIRING)));
  }

  @Test
  public void singleInputMultipleOutputSucceeds() throws Exception {
    WindowFn<Object, IntervalWindow> windowFn =
        SlidingWindows.of(Duration.standardMinutes(4L)).every(Duration.standardMinutes(2L));

    AssignWindowsRunner<Integer, IntervalWindow> runner = AssignWindowsRunner.create(windowFn);

    IntervalWindow firstWindow =
        new IntervalWindow(
            new Instant(0).minus(Duration.standardMinutes(4L)), Duration.standardMinutes(4L));
    IntervalWindow secondWindow =
        new IntervalWindow(
            new Instant(0).minus(Duration.standardMinutes(2L)), Duration.standardMinutes(4L));
    IntervalWindow thirdWindow = new IntervalWindow(new Instant(0), Duration.standardMinutes(4L));

    WindowedValue<Integer> firstValue =
        WindowedValue.timestampedValueInGlobalWindow(-3, new Instant(-12));
    assertThat(
        runner.assignWindows(firstValue),
        equalTo(
            WindowedValue.of(
                -3,
                new Instant(-12),
                ImmutableSet.of(firstWindow, secondWindow),
                firstValue.getPane())));
    WindowedValue<Integer> secondValue =
        WindowedValue.of(
            3,
            new Instant(12),
            new IntervalWindow(new Instant(-12), Duration.standardMinutes(24)),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);

    assertThat(
        runner.assignWindows(secondValue),
        equalTo(
            WindowedValue.of(
                3,
                new Instant(12),
                ImmutableSet.of(secondWindow, thirdWindow),
                secondValue.getPane())));
  }

  @Test
  public void multipleInputWindowsAsMapFnSucceeds() throws Exception {
    WindowFn<Object, BoundedWindow> windowFn =
        new WindowFn<Object, BoundedWindow>() {
          @Override
          public Collection<BoundedWindow> assignWindows(AssignContext c) {
            c.window();
            return ImmutableSet.of(
                GlobalWindow.INSTANCE,
                new IntervalWindow(new Instant(-500), Duration.standardMinutes(3)));
          }

          @Override
          public void mergeWindows(MergeContext c) {
            throw new UnsupportedOperationException();
          }

          @Override
          public WindowMappingFn<BoundedWindow> getDefaultWindowMappingFn() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isCompatible(WindowFn<?, ?> other) {
            throw new UnsupportedOperationException();
          }

          @Override
          public Coder<BoundedWindow> windowCoder() {
            throw new UnsupportedOperationException();
          }
        };
    Collection<WindowedValue<?>> outputs = new ArrayList<>();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    PCollectionConsumerRegistry pCollectionConsumerRegistry =
        new PCollectionConsumerRegistry(
            metricsContainerRegistry, mock(ExecutionStateTracker.class));
    pCollectionConsumerRegistry.register("output", "ptransform", outputs::add);
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    MapFnRunners.forWindowedValueMapFnFactory(new AssignWindowsMapFnFactory<>())
        .createRunnerForPTransform(
            null /* pipelineOptions */,
            null /* beamFnDataClient */,
            null /* beamFnStateClient */,
            null /* beamFnTimerClient */,
            "ptransform",
            PTransform.newBuilder()
                .putInputs("in", "input")
                .putOutputs("out", "output")
                .setSpec(
                    FunctionSpec.newBuilder()
                        .setUrn(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)
                        .setPayload(
                            WindowIntoPayload.newBuilder()
                                .setWindowFn(
                                    WindowingStrategyTranslation.toProto(windowFn, components))
                                .build()
                                .toByteString()))
                .build(),
            null /* processBundleInstructionId */,
            null /* pCollections */,
            null /* coders */,
            null /* windowingStrategies */,
            pCollectionConsumerRegistry,
            null /* startFunctionRegistry */,
            null /* finishFunctionRegistry */,
            null /* addResetFunction */,
            null /* tearDownRegistry */,
            null /* addProgressRequestCallback */,
            null /* splitListener */,
            null /* bundleFinalizer */);

    WindowedValue<Integer> value =
        WindowedValue.of(
            2,
            new Instant(-10L),
            ImmutableList.of(
                new IntervalWindow(new Instant(-22L), Duration.standardMinutes(5L)),
                new IntervalWindow(new Instant(-120000L), Duration.standardMinutes(3L))),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    pCollectionConsumerRegistry.getMultiplexingConsumer("input").accept(value);
    assertThat(
        outputs,
        containsInAnyOrder(
            WindowedValue.of(
                2,
                new Instant(-10L),
                ImmutableSet.of(
                    GlobalWindow.INSTANCE,
                    new IntervalWindow(new Instant(-500), Duration.standardMinutes(3))),
                PaneInfo.ON_TIME_AND_ONLY_FIRING),
            WindowedValue.of(
                2,
                new Instant(-10L),
                ImmutableSet.of(
                    GlobalWindow.INSTANCE,
                    new IntervalWindow(new Instant(-500), Duration.standardMinutes(3))),
                PaneInfo.ON_TIME_AND_ONLY_FIRING)));
  }

  @Test
  public void multipleInputWindowsThrows() throws Exception {
    WindowFn<Object, BoundedWindow> windowFn =
        new WindowFn<Object, BoundedWindow>() {
          @Override
          public Collection<BoundedWindow> assignWindows(AssignContext c) throws Exception {
            return Collections.singleton(c.window());
          }

          @Override
          public void mergeWindows(MergeContext c) throws Exception {
            throw new UnsupportedOperationException();
          }

          @Override
          public WindowMappingFn<BoundedWindow> getDefaultWindowMappingFn() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isCompatible(WindowFn<?, ?> other) {
            throw new UnsupportedOperationException();
          }

          @Override
          public Coder<BoundedWindow> windowCoder() {
            throw new UnsupportedOperationException();
          }
        };
    AssignWindowsRunner<Integer, BoundedWindow> runner = AssignWindowsRunner.create(windowFn);

    thrown.expect(IllegalArgumentException.class);
    runner.assignWindows(
        WindowedValue.of(
            2,
            new Instant(-10L),
            ImmutableList.of(
                new IntervalWindow(new Instant(-22L), Duration.standardMinutes(5L)),
                new IntervalWindow(new Instant(-120000L), Duration.standardMinutes(3L))),
            PaneInfo.ON_TIME_AND_ONLY_FIRING));
  }

  @Test
  public void factoryCreatesFromJavaWindowFn() throws Exception {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    PTransform windowPTransform =
        PTransform.newBuilder()
            .putInputs("in", "input")
            .putOutputs("out", "output")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)
                    .setPayload(
                        WindowIntoPayload.newBuilder()
                            .setWindowFn(
                                WindowingStrategyTranslation.toProto(
                                    new TestWindowFn(), components))
                            .build()
                            .toByteString())
                    .build())
            .build();

    ThrowingFunction<WindowedValue<?>, WindowedValue<?>> fn =
        (ThrowingFunction) factory.forPTransform("transform", windowPTransform);

    assertThat(
        fn.apply(
            WindowedValue.of(
                22L,
                new Instant(5),
                new IntervalWindow(new Instant(0L), new Instant(20027L)),
                PaneInfo.ON_TIME_AND_ONLY_FIRING)),
        equalTo(
            WindowedValue.of(
                22L,
                new Instant(5),
                new TestWindowFn().assignWindow(new Instant(5)),
                PaneInfo.ON_TIME_AND_ONLY_FIRING)));
  }

  @Test
  public void factoryCreatesFromKnownWindowFn() throws Exception {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    PTransform windowPTransform =
        PTransform.newBuilder()
            .putInputs("in", "input")
            .putOutputs("out", "output")
            .setSpec(
                FunctionSpec.newBuilder()
                    .setUrn(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN)
                    .setPayload(
                        WindowIntoPayload.newBuilder()
                            .setWindowFn(
                                WindowingStrategyTranslation.toProto(
                                    Sessions.withGapDuration(Duration.standardMinutes(12L)),
                                    components))
                            .build()
                            .toByteString())
                    .build())
            .build();
    ThrowingFunction<WindowedValue<?>, WindowedValue<?>> fn =
        (ThrowingFunction) factory.forPTransform("transform", windowPTransform);
    WindowedValue<?> output =
        fn.apply(
            WindowedValue.of(
                22L,
                new Instant(5),
                new IntervalWindow(new Instant(0L), new Instant(20027L)),
                PaneInfo.ON_TIME_AND_ONLY_FIRING));

    assertThat(
        output,
        equalTo(
            WindowedValue.of(
                22L,
                new Instant(5),
                new IntervalWindow(new Instant(5L), Duration.standardMinutes(12L)),
                PaneInfo.ON_TIME_AND_ONLY_FIRING)));
  }

  private static class TestWindowFn extends PartitioningWindowFn<Object, IntervalWindow> {
    @Override
    public IntervalWindow assignWindow(Instant timestamp) {
      return new IntervalWindow(
          BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE.maxTimestamp());
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return equals(other);
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
      return IntervalWindowCoder.of();
    }
  }
}
