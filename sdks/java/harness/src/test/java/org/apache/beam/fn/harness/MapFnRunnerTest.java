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

import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.function.ThrowingFunction;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MapFnRunner}. */
@RunWith(JUnit4.class)
public class MapFnRunnerTest {
  private static final String EXPECTED_ID = "pTransformId";
  private static final RunnerApi.PTransform EXPECTED_PTRANSFORM = RunnerApi.PTransform.newBuilder()
      .putInputs("input", "inputPC")
      .putOutputs("output", "outputPC")
      .build();

  @Test
  public void testWindowMapping() throws Exception {

    List<WindowedValue<?>> outputConsumer = new ArrayList<>();
    Multimap<String, FnDataReceiver<WindowedValue<?>>> consumers = HashMultimap.create();
    consumers.put("outputPC", outputConsumer::add);

    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    new MapFnRunner.Factory<>(this::createMapFunctionForPTransform)
        .createRunnerForPTransform(
            PipelineOptionsFactory.create(),
            null /* beamFnDataClient */,
            null /* beamFnStateClient */,
            EXPECTED_ID,
            EXPECTED_PTRANSFORM,
            Suppliers.ofInstance("57L")::get,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            consumers,
            startFunctions::add,
            finishFunctions::add);

    assertThat(startFunctions, empty());
    assertThat(finishFunctions, empty());

    assertThat(consumers.keySet(), containsInAnyOrder("inputPC", "outputPC"));

    Iterables.getOnlyElement(
        consumers.get("inputPC")).accept(valueInGlobalWindow("abc"));

    assertThat(outputConsumer, contains(valueInGlobalWindow("ABC")));
  }

  public ThrowingFunction<String, String> createMapFunctionForPTransform(String ptransformId,
      PTransform pTransform) throws IOException {
    assertEquals(EXPECTED_ID, ptransformId);
    assertEquals(EXPECTED_PTRANSFORM, pTransform);
    return (String str) -> str.toUpperCase();
  }
}
