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

package org.apache.beam.runners.core;

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BoundedSourceRunner}. */
@RunWith(JUnit4.class)
public class BoundedSourceRunnerTest {
  @Test
  public void testRunReadLoopWithMultipleSources() throws Exception {
    List<WindowedValue<Long>> out1ValuesA = new ArrayList<>();
    List<WindowedValue<Long>> out1ValuesB = new ArrayList<>();
    List<WindowedValue<Long>> out2Values = new ArrayList<>();
    Map<String, Collection<ThrowingConsumer<WindowedValue<Long>>>> outputMap = ImmutableMap.of(
        "out1", ImmutableList.of(out1ValuesA::add, out1ValuesB::add),
        "out2", ImmutableList.of(out2Values::add));

    BoundedSourceRunner<BoundedSource<Long>, Long> runner =
        new BoundedSourceRunner<>(
        PipelineOptionsFactory.create(),
        BeamFnApi.FunctionSpec.getDefaultInstance(),
        outputMap);

    runner.runReadLoop(valueInGlobalWindow(CountingSource.upTo(2)));
    runner.runReadLoop(valueInGlobalWindow(CountingSource.upTo(1)));

    assertThat(out1ValuesA,
        contains(valueInGlobalWindow(0L), valueInGlobalWindow(1L), valueInGlobalWindow(0L)));
    assertThat(out1ValuesB,
        contains(valueInGlobalWindow(0L), valueInGlobalWindow(1L), valueInGlobalWindow(0L)));
    assertThat(out2Values,
        contains(valueInGlobalWindow(0L), valueInGlobalWindow(1L), valueInGlobalWindow(0L)));
  }

  @Test
  public void testRunReadLoopWithEmptySource() throws Exception {
    List<WindowedValue<Long>> out1Values = new ArrayList<>();
    Map<String, Collection<ThrowingConsumer<WindowedValue<Long>>>> outputMap = ImmutableMap.of(
        "out1", ImmutableList.of(out1Values::add));

    BoundedSourceRunner<BoundedSource<Long>, Long> runner =
        new BoundedSourceRunner<>(
        PipelineOptionsFactory.create(),
        BeamFnApi.FunctionSpec.getDefaultInstance(),
        outputMap);

    runner.runReadLoop(valueInGlobalWindow(CountingSource.upTo(0)));

    assertThat(out1Values, empty());
  }

  @Test
  public void testStart() throws Exception {
    List<WindowedValue<Long>> outValues = new ArrayList<>();
    Map<String, Collection<ThrowingConsumer<WindowedValue<Long>>>> outputMap = ImmutableMap.of(
        "out", ImmutableList.of(outValues::add));

    ByteString encodedSource =
        ByteString.copyFrom(SerializableUtils.serializeToByteArray(CountingSource.upTo(3)));

    BoundedSourceRunner<BoundedSource<Long>, Long> runner =
        new BoundedSourceRunner<>(
        PipelineOptionsFactory.create(),
        BeamFnApi.FunctionSpec.newBuilder().setData(
            Any.pack(BytesValue.newBuilder().setValue(encodedSource).build())).build(),
        outputMap);

    runner.start();

    assertThat(outValues,
        contains(valueInGlobalWindow(0L), valueInGlobalWindow(1L), valueInGlobalWindow(2L)));
  }
}
