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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.beam.fn.harness.MapFnRunners.WindowedValueMapFnFactory;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.WindowingStrategyTranslation;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;

/** The Java SDK Harness implementation of the {@link Window.Assign} primitive. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
class AssignWindowsRunner<T, W extends BoundedWindow> {

  /** A registrar which provides a factory to handle Java {@link WindowFn WindowFns}. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {
    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(
          PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN,
          MapFnRunners.forWindowedValueMapFnFactory(new AssignWindowsMapFnFactory<>()));
    }
  }

  @VisibleForTesting
  static class AssignWindowsMapFnFactory<T> implements WindowedValueMapFnFactory<T, T> {
    @Override
    public ThrowingFunction<WindowedValue<T>, WindowedValue<T>> forPTransform(
        String ptransformId, PTransform ptransform) throws IOException {
      checkArgument(
          PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN.equals(ptransform.getSpec().getUrn()));
      checkArgument(ptransform.getInputsCount() == 1, "Expected only one input");
      checkArgument(ptransform.getOutputsCount() == 1, "Expected only one output");
      WindowIntoPayload payload = WindowIntoPayload.parseFrom(ptransform.getSpec().getPayload());

      WindowFn<T, ?> windowFn =
          (WindowFn<T, ?>) WindowingStrategyTranslation.windowFnFromProto(payload.getWindowFn());

      return AssignWindowsRunner.create(windowFn)::assignWindows;
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  static <T, W extends BoundedWindow> AssignWindowsRunner<T, W> create(
      WindowFn<? super T, W> windowFn) {
    // Safe contravariant cast
    WindowFn<T, W> typedWindowFn = (WindowFn<T, W>) windowFn;
    return new AssignWindowsRunner<>(typedWindowFn);
  }

  private final WindowFn<T, W> windowFn;

  private AssignWindowsRunner(WindowFn<T, W> windowFn) {
    this.windowFn = windowFn;
  }

  WindowedValue<T> assignWindows(WindowedValue<T> input) throws Exception {
    // TODO: https://github.com/apache/beam/issues/18870 consider allocating only once and updating
    // the current value per call.
    WindowFn<T, W>.AssignContext ctxt =
        windowFn.new AssignContext() {
          @Override
          public T element() {
            return input.getValue();
          }

          @Override
          public Instant timestamp() {
            return input.getTimestamp();
          }

          @Override
          public BoundedWindow window() {
            return Iterables.getOnlyElement(input.getWindows());
          }
        };
    Collection<W> windows = windowFn.assignWindows(ctxt);
    return WindowedValue.of(input.getValue(), input.getTimestamp(), windows, input.getPane());
  }
}
