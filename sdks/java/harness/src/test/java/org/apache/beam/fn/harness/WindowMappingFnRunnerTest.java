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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.ParDoTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WindowMappingFnRunner}. */
@RunWith(JUnit4.class)
public class WindowMappingFnRunnerTest {
  @Test
  public void testWindowMapping() throws Exception {
    String pTransformId = "pTransformId";

    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.FunctionSpec functionSpec =
        RunnerApi.FunctionSpec.newBuilder()
            .setUrn(WindowMappingFnRunner.URN)
            .setPayload(
                ParDoTranslation.translateWindowMappingFn(
                        new GlobalWindows().getDefaultWindowMappingFn(), components)
                    .toByteString())
            .build();
    RunnerApi.PTransform pTransform =
        RunnerApi.PTransform.newBuilder().setSpec(functionSpec).build();

    ThrowingFunction<KV<Object, BoundedWindow>, KV<Object, BoundedWindow>> mapFunction =
        WindowMappingFnRunner.createMapFunctionForPTransform(pTransformId, pTransform);

    KV<Object, BoundedWindow> input =
        KV.of("abc", new IntervalWindow(Instant.now(), Duration.standardMinutes(1)));

    assertEquals(KV.of(input.getKey(), GlobalWindow.INSTANCE), mapFunction.apply(input));
  }
}
