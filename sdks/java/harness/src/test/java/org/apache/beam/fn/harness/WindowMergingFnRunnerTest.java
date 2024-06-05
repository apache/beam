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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.WindowingStrategyTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
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

    // Process a new group of windows, make sure that previous result has been cleaned up.
    BoundedWindow[] expectedToBeMergedGroup2 =
        new BoundedWindow[] {
          new IntervalWindow(new Instant(15L), new Instant(17L)),
          new IntervalWindow(new Instant(16L), new Instant(18L))
        };

    input =
        KV.of(
            "abc",
            ImmutableList.<BoundedWindow>builder()
                .add(expectedToBeMergedGroup2)
                .addAll(expectedToBeUnmerged)
                .build());

    output = mapFunction.apply(input);
    assertEquals(input.getKey(), output.getKey());
    assertEquals(expectedToBeUnmerged, output.getValue().getKey());
    mergedOutput = Iterables.getOnlyElement(output.getValue().getValue());
    assertEquals(new IntervalWindow(new Instant(15L), new Instant(18L)), mergedOutput.getKey());
    assertThat(mergedOutput.getValue(), containsInAnyOrder(expectedToBeMergedGroup2));
  }

  private static <W extends BoundedWindow> RunnerApi.PTransform createMergeTransformForWindowFn(
      WindowFn<?, W> windowFn) throws Exception {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("test"));
    RunnerApi.FunctionSpec functionSpec =
        RunnerApi.FunctionSpec.newBuilder()
            .setUrn(WindowMergingFnRunner.URN)
            .setPayload(WindowingStrategyTranslation.toProto(windowFn, components).toByteString())
            .build();
    return RunnerApi.PTransform.newBuilder().setSpec(functionSpec).build();
  }
}
