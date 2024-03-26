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
import java.util.Map;
import java.util.Objects;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Translates from elements to human-readable string.
 *
 * <p>Translation function:
 *
 * <ul>
 *   <li>Input: {@code KV<nonce, element>}
 *   <li>Output: {@code KV<nonce, string>}
 * </ul>
 *
 * <p>For each element, the human-readable string is returned. The nonce is used by a runner to
 * associate each input with its output. The nonce is represented as an opaque set of bytes.
 */
public class ToStringFnRunner {
  static final String URN = PTransformTranslation.TO_STRING_TRANSFORM_URN;

  /**
   * A registrar which provides a factory to handle translating elements to a human readable string.
   */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    @SuppressWarnings({
      "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
    })
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(
          URN,
          MapFnRunners.forValueMapFnFactory(ToStringFnRunner::createToStringFunctionForPTransform));
    }
  }

  static <T, V> ThrowingFunction<KV<T, V>, KV<T, String>> createToStringFunctionForPTransform(
      String ptransformId, PTransform pTransform) {
    return (KV<T, V> input) -> KV.of(input.getKey(), Objects.toString(input.getValue()));
  }
}
