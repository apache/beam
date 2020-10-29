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
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

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
@SuppressWarnings({
  "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
public class ToStringFnRunner {
  static final String URN = PTransformTranslation.TO_STRING_TRANSFORM_URN;

  /**
   * A registrar which provides a factory to handle translating elements to a human readable string.
   */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(
          URN,
          MapFnRunners.forValueMapFnFactory(ToStringFnRunner::createToStringFunctionForPTransform));
    }
  }

  static <T, V> ThrowingFunction<KV<T, V>, KV<T, String>> createToStringFunctionForPTransform(
      String ptransformId, PTransform pTransform) {
    return (KV<T, V> input) -> {
      String val = "null";

      // For some reason, the nullness checker cannot be satisfied when dereferencing
      // input.getValue(). It is only satisfied by creating a temporary variable with v.
      V v = input.getValue();
      if (v != null) {
        val = v.toString();
      }

      return KV.of(input.getKey(), val);
    };
  }
}
