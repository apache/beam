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
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.beam.fn.harness.PTransformRunnerFactory.Registrar;
import org.apache.beam.fn.harness.control.ProcessBundleHandler;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BoundedSourceRunner}. */
@RunWith(JUnit4.class)
public class BoundedSourceRunnerTest {

  public static final String URN = "urn:org.apache.beam:source:java:0.1";

  @Test
  public void testRunReadLoopWithMultipleSources() throws Exception {
    List<WindowedValue<Long>> out1Values = new ArrayList<>();
    List<WindowedValue<Long>> out2Values = new ArrayList<>();
    Collection<FnDataReceiver<WindowedValue<Long>>> consumers =
        ImmutableList.of(out1Values::add, out2Values::add);

    BoundedSourceRunner<BoundedSource<Long>, Long> runner = new BoundedSourceRunner<>(
        PipelineOptionsFactory.create(),
        RunnerApi.FunctionSpec.getDefaultInstance(),
        consumers);

    runner.runReadLoop(valueInGlobalWindow(CountingSource.upTo(2)));
    runner.runReadLoop(valueInGlobalWindow(CountingSource.upTo(1)));

    assertThat(out1Values,
        contains(valueInGlobalWindow(0L), valueInGlobalWindow(1L), valueInGlobalWindow(0L)));
    assertThat(out2Values,
        contains(valueInGlobalWindow(0L), valueInGlobalWindow(1L), valueInGlobalWindow(0L)));
  }

  @Test
  public void testRunReadLoopWithEmptySource() throws Exception {
    List<WindowedValue<Long>> outValues = new ArrayList<>();
    Collection<FnDataReceiver<WindowedValue<Long>>> consumers =
        ImmutableList.of(outValues::add);

    BoundedSourceRunner<BoundedSource<Long>, Long> runner = new BoundedSourceRunner<>(
        PipelineOptionsFactory.create(),
        RunnerApi.FunctionSpec.getDefaultInstance(),
        consumers);

    runner.runReadLoop(valueInGlobalWindow(CountingSource.upTo(0)));

    assertThat(outValues, empty());
  }

  @Test
  public void testStart() throws Exception {
    List<WindowedValue<Long>> outValues = new ArrayList<>();
    Collection<FnDataReceiver<WindowedValue<Long>>> consumers =
        ImmutableList.of(outValues::add);

    ByteString encodedSource =
        ByteString.copyFrom(SerializableUtils.serializeToByteArray(CountingSource.upTo(3)));

    BoundedSourceRunner<BoundedSource<Long>, Long> runner = new BoundedSourceRunner<>(
        PipelineOptionsFactory.create(),
        RunnerApi.FunctionSpec.newBuilder()
            .setUrn(ProcessBundleHandler.JAVA_SOURCE_URN).setPayload(encodedSource).build(),
        consumers);

    runner.start();

    assertThat(outValues,
        contains(valueInGlobalWindow(0L), valueInGlobalWindow(1L), valueInGlobalWindow(2L)));
  }

  @Test
  public void testCreatingAndProcessingSourceFromFactory() throws Exception {
    List<WindowedValue<String>> outputValues = new ArrayList<>();

    Multimap<String, FnDataReceiver<WindowedValue<?>>> consumers = HashMultimap.create();
    consumers.put("outputPC",
        (FnDataReceiver) (FnDataReceiver<WindowedValue<String>>) outputValues::add);
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    RunnerApi.FunctionSpec functionSpec =
        RunnerApi.FunctionSpec.newBuilder()
            .setUrn("urn:org.apache.beam:source:java:0.1")
            .setPayload(
                ByteString.copyFrom(SerializableUtils.serializeToByteArray(CountingSource.upTo(3))))
            .build();

    RunnerApi.PTransform pTransform = RunnerApi.PTransform.newBuilder()
        .setSpec(functionSpec)
        .putInputs("input", "inputPC")
        .putOutputs("output", "outputPC")
        .build();

    new BoundedSourceRunner.Factory<>().createRunnerForPTransform(
        PipelineOptionsFactory.create(),
        null /* beamFnDataClient */,
        null /* beamFnStateClient */,
        "pTransformId",
        pTransform,
        Suppliers.ofInstance("57L")::get,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        consumers,
        startFunctions::add,
        finishFunctions::add);

    // This is testing a deprecated way of running sources and should be removed
    // once all source definitions are instead propagated along the input edge.
    Iterables.getOnlyElement(startFunctions).run();
    assertThat(outputValues, contains(
        valueInGlobalWindow(0L),
        valueInGlobalWindow(1L),
        valueInGlobalWindow(2L)));
    outputValues.clear();

    // Check that when passing a source along as an input, the source is processed.
    assertThat(consumers.keySet(), containsInAnyOrder("inputPC", "outputPC"));
    Iterables.getOnlyElement(consumers.get("inputPC")).accept(
        valueInGlobalWindow(CountingSource.upTo(2)));
    assertThat(outputValues, contains(
        valueInGlobalWindow(0L),
        valueInGlobalWindow(1L)));

    assertThat(finishFunctions, Matchers.empty());
  }

  @Test
  public void testRegistration() {
    for (Registrar registrar :
        ServiceLoader.load(Registrar.class)) {
      if (registrar instanceof BoundedSourceRunner.Registrar) {
        assertThat(registrar.getPTransformRunnerFactories(), IsMapContaining.hasKey(URN));
        return;
      }
    }
    fail("Expected registrar not found.");
  }
}
