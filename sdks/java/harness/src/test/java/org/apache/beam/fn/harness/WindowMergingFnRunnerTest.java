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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Collections;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.sdk.fn.function.ThrowingFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WindowMergingFnRunner}. */
@RunWith(JUnit4.class)
public class WindowMergingFnRunnerTest {
  @Test
  public void testWindowMergingWithNonMergingWindowFn() throws Exception {
    ThrowingFunction<
            KV<Object, Iterable<BoundedWindow>>,
            KV<
                Object,
                KV<Iterable<BoundedWindow>, Iterable<KV<BoundedWindow, Iterable<BoundedWindow>>>>>>
        mapFunction =
            WindowMergingFnRunner.createMapFunctionForPTransform(
                "ptransformId", createMergeTransformForWindowFn(new GlobalWindows()));

    KV<Object, Iterable<BoundedWindow>> input =
        KV.of(
            "abc",
            ImmutableList.of(new IntervalWindow(Instant.now(), Duration.standardMinutes(1))));

    assertEquals(
        KV.of(input.getKey(), KV.of(input.getValue(), Collections.emptyList())),
        mapFunction.apply(input));
  }

  @Test
  public void testWindowMergingWithMergingWindowFn() throws Exception {
    ThrowingFunction<
            KV<Object, Iterable<BoundedWindow>>,
            KV<
                Object,
                KV<Iterable<BoundedWindow>, Iterable<KV<BoundedWindow, Iterable<BoundedWindow>>>>>>
        mapFunction =
            WindowMergingFnRunner.createMapFunctionForPTransform(
                "ptransformId",
                createMergeTransformForWindowFn(Sessions.withGapDuration(Duration.millis(5L))));

    // 7, 8 and 10 should all be merged. 1 and 20 should remain in the original set.
    BoundedWindow[] expectedToBeMerged =
        new BoundedWindow[] {
          new IntervalWindow(new Instant(9L), new Instant(11L)),
          new IntervalWindow(new Instant(10L), new Instant(10L)),
          new IntervalWindow(new Instant(7L), new Instant(10L))
        };
    Iterable<BoundedWindow> expectedToBeUnmerged =
        Sets.newHashSet(
            new IntervalWindow(new Instant(1L), new Instant(1L)),
            new IntervalWindow(new Instant(20L), new Instant(20L)));
    KV<Object, Iterable<BoundedWindow>> input =
        KV.of(
            "abc",
            ImmutableList.<BoundedWindow>builder()
                .add(expectedToBeMerged)
                .addAll(expectedToBeUnmerged)
                .build());

    KV<Object, KV<Iterable<BoundedWindow>, Iterable<KV<BoundedWindow, Iterable<BoundedWindow>>>>>
        output = mapFunction.apply(input);
    assertEquals(input.getKey(), output.getKey());
    assertEquals(expectedToBeUnmerged, output.getValue().getKey());
    KV<BoundedWindow, Iterable<BoundedWindow>> mergedOutput =
        Iterables.getOnlyElement(output.getValue().getValue());
    assertEquals(new IntervalWindow(new Instant(7L), new Instant(11L)), mergedOutput.getKey());
    assertThat(mergedOutput.getValue(), containsInAnyOrder(expectedToBeMerged));
  }

  private static <W extends BoundedWindow> RunnerApi.PTransform createMergeTransformForWindowFn(
      WindowFn<?, W> windowFn) throws Exception {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environment.newBuilder().setUrl("java").build());
    RunnerApi.FunctionSpec functionSpec =
        RunnerApi.FunctionSpec.newBuilder()
            .setUrn(WindowMergingFnRunner.URN)
            .setPayload(WindowingStrategyTranslation.toProto(windowFn, components).toByteString())
            .build();
    return RunnerApi.PTransform.newBuilder().setSpec(functionSpec).build();
  }
}
