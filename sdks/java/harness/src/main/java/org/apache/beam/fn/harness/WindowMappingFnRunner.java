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

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.construction.PCollectionViewTranslation;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Represents mapping of main input window onto side input window.
 *
 * <p>Side input window mapping function:
 *
 * <ul>
 *   <li>Input: {@code KV<nonce, MainInputWindow>}
 *   <li>Output: {@code KV<nonce, SideInputWindow>}
 * </ul>
 *
 * <p>For each main input window, the side input window is returned. The nonce is used by a runner
 * to associate each input with its output. The nonce is represented as an opaque set of bytes.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class WindowMappingFnRunner {
  static final String URN = PTransformTranslation.MAP_WINDOWS_TRANSFORM_URN;

  /**
   * A registrar which provides a factory to handle mapping main input windows onto side input
   * windows.
   */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(
          URN,
          MapFnRunners.forValueMapFnFactory(WindowMappingFnRunner::createMapFunctionForPTransform));
    }
  }

  static <T, W1 extends BoundedWindow, W2 extends BoundedWindow>
      ThrowingFunction<KV<T, W1>, KV<T, W2>> createMapFunctionForPTransform(
          String ptransformId, PTransform pTransform) throws IOException {
    FunctionSpec windowMappingFnPayload = FunctionSpec.parseFrom(pTransform.getSpec().getPayload());
    WindowMappingFn<W2> windowMappingFn =
        (WindowMappingFn<W2>)
            PCollectionViewTranslation.windowMappingFnFromProto(windowMappingFnPayload);
    return (KV<T, W1> input) ->
        KV.of(input.getKey(), windowMappingFn.getSideInputWindow(input.getValue()));
  }
}
