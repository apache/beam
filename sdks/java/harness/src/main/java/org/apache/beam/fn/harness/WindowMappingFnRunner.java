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

import static org.apache.beam.runners.core.construction.UrnUtils.validateCommonUrn;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.fn.harness.fn.ThrowingFunction;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.runners.core.construction.PCollectionViewTranslation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.values.KV;

/**
 * Maps windows using a window mapping fn. The input is {@link KV} with the key being a nonce
 * and the value being a window, the output must be a {@link KV} with the key being the same nonce
 * as the input and the value being the mapped window.
 */
public class WindowMappingFnRunner {
  static final String URN = validateCommonUrn("beam:transform:map_windows:v1");

  /**
   * A registrar which provides a factory to handle mapping main input windows onto side input
   * windows.
   */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(URN, MapFnRunner.createMapFnRunnerFactoryWith(
          WindowMappingFnRunner::createMapFunctionForPTransform));
    }
  }

  static <T, W1 extends BoundedWindow, W2 extends BoundedWindow>
  ThrowingFunction<KV<T, W1>, KV<T, W2>> createMapFunctionForPTransform(
      String ptransformId, PTransform pTransform) throws IOException {
    SdkFunctionSpec windowMappingFnPayload =
        SdkFunctionSpec.parseFrom(pTransform.getSpec().getPayload());
    WindowMappingFn<W2> windowMappingFn =
        (WindowMappingFn<W2>) PCollectionViewTranslation.windowMappingFnFromProto(
            windowMappingFnPayload);
    return (KV<T, W1> input) ->
        KV.of(input.getKey(), windowMappingFn.getSideInputWindow(input.getValue()));
  }
}
