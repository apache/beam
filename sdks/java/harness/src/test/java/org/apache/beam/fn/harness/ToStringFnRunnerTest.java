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

import static org.junit.Assert.assertEquals;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WindowMappingFnRunner}. */
@RunWith(JUnit4.class)
public class ToStringFnRunnerTest {
  @Test
  public void testPrimitiveToString() throws Exception {
    String pTransformId = "pTransformId";

    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.PTransform pTransform = RunnerApi.PTransform.newBuilder().build();

    ThrowingFunction<KV<String, Integer>, KV<String, String>> toStringFunction =
        ToStringFnRunner.createToStringFunctionForPTransform(pTransformId, pTransform);

    KV<String, Integer> input = KV.of("key", 12345);

    assertEquals(KV.of("key", "12345"), toStringFunction.apply(input));
  }

  @Test
  public void testToStringOverride() throws Exception {
    class ClassWithToStringOverride {
      @Override
      public String toString() {
        return "Some string";
      }
    }

    String pTransformId = "pTransformId";

    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.PTransform pTransform = RunnerApi.PTransform.newBuilder().build();

    ThrowingFunction<KV<String, ClassWithToStringOverride>, KV<String, String>> toStringFunction =
        ToStringFnRunner.createToStringFunctionForPTransform(pTransformId, pTransform);

    KV<String, ClassWithToStringOverride> input = KV.of("key", new ClassWithToStringOverride());

    assertEquals(KV.of("key", "Some string"), toStringFunction.apply(input));
  }
}
