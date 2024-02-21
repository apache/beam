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
package org.apache.beam.sdk.util.construction;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.PartitioningWindowFn;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Window.Assign;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link WindowIntoTranslation}. */
@RunWith(Parameterized.class)
public class WindowIntoTranslationTest {
  @Parameters(name = "{index}: {0}")
  public static Iterable<WindowFn<?, ?>> data() {
    // This pipeline exists for construction, not to run any test.
    return ImmutableList.<WindowFn<?, ?>>builder()
        .add(FixedWindows.of(Duration.standardMinutes(10L)))
        .add(new GlobalWindows())
        .add(Sessions.withGapDuration(Duration.standardMinutes(15L)))
        .add(SlidingWindows.of(Duration.standardMinutes(5L)).every(Duration.standardMinutes(1L)))
        .add(new CustomWindows())
        .build();
  }

  @Parameter(0)
  public WindowFn<?, ?> windowFn;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testToFromProto() throws InvalidProtocolBufferException {
    pipeline.apply(GenerateSequence.from(0)).apply(Window.<Long>into((WindowFn) windowFn));

    final AtomicReference<AppliedPTransform<?, ?, Assign<?>>> assign = new AtomicReference<>(null);
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public void visitPrimitiveTransform(Node node) {
            if (node.getTransform() instanceof Window.Assign) {
              checkState(assign.get() == null);
              assign.set(
                  (AppliedPTransform<?, ?, Assign<?>>) node.toAppliedPTransform(getPipeline()));
            }
          }
        });
    checkState(assign.get() != null);

    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    WindowIntoPayload payload =
        WindowIntoTranslation.toProto(assign.get().getTransform(), components);

    assertEquals(windowFn, WindowingStrategyTranslation.windowFnFromProto(payload.getWindowFn()));
  }

  private static class CustomWindows extends PartitioningWindowFn<String, BoundedWindow> {
    @Override
    public BoundedWindow assignWindow(Instant timestamp) {
      return GlobalWindow.INSTANCE;
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return getClass().equals(other.getClass());
    }

    @Override
    public Coder<BoundedWindow> windowCoder() {
      return (Coder) GlobalWindow.Coder.INSTANCE;
    }

    @Override
    public boolean equals(@Nullable Object other) {
      return other != null && other.getClass().equals(this.getClass());
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }
}
