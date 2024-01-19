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
package org.apache.beam.runners.dataflow.worker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

import java.util.List;
import org.apache.beam.runners.dataflow.worker.GroupAlsoByWindowParDoFnFactory.MergingCombineFn;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineLongFn;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GroupAlsoByWindowParDoFnFactory}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class GroupAlsoByWindowParDoFnFactoryTest {
  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void testJavaWindowingStrategyDeserialization() throws Exception {
    WindowFn windowFn = FixedWindows.of(Duration.millis(17));

    WindowingStrategy windowingStrategy = WindowingStrategy.of(windowFn);

    assertThat(windowingStrategy.getWindowFn(), equalTo(windowFn));
  }

  @Test
  public void testMultipleAccumulatorsInMergingCombineFn() {
    BinaryCombineLongFn originalFn = Sum.ofLongs();
    MergingCombineFn<Void, long[]> fn =
        new MergingCombineFn<>(
            originalFn, originalFn.getAccumulatorCoder(p.getCoderRegistry(), VarLongCoder.of()));

    long[] inputAccum = originalFn.createAccumulator();
    inputAccum = originalFn.addInput(inputAccum, 1L);
    inputAccum = originalFn.addInput(inputAccum, 2L);
    assertThat(inputAccum[0], equalTo(3L));

    long[] otherAccum = originalFn.createAccumulator();
    otherAccum = originalFn.addInput(otherAccum, 4L);
    assertThat(otherAccum[0], equalTo(4L));

    List<long[]> first = fn.createAccumulator();
    first = fn.addInput(first, inputAccum);
    assertThat(first, hasItem(inputAccum));
    assertThat(inputAccum.length, equalTo(1));
    assertThat(inputAccum[0], equalTo(3L));

    List<long[]> second = fn.createAccumulator();
    second = fn.addInput(second, inputAccum);
    assertThat(second, hasItem(inputAccum));
    assertThat(inputAccum.length, equalTo(1));
    assertThat(inputAccum[0], equalTo(3L));

    List<long[]> mergeToSecond = fn.createAccumulator();
    mergeToSecond = fn.addInput(mergeToSecond, otherAccum);
    assertThat(mergeToSecond, hasItem(otherAccum));
    assertThat(otherAccum.length, equalTo(1));
    assertThat(otherAccum[0], equalTo(4L));

    List<long[]> firstSelfMerged = fn.mergeAccumulators(ImmutableList.of(first));
    List<long[]> compactedFirst = fn.compact(firstSelfMerged);
    assertThat(firstSelfMerged, equalTo(compactedFirst));
    assertThat(firstSelfMerged, hasSize(1));
    assertThat(firstSelfMerged.get(0)[0], equalTo(3L));

    List<long[]> secondMerged = fn.mergeAccumulators(ImmutableList.of(second, mergeToSecond));
    List<long[]> secondCompacted = fn.compact(secondMerged);
    assertThat(secondCompacted, hasSize(1));
    assertThat(secondCompacted.get(0)[0], equalTo(7L));
    assertThat(firstSelfMerged, equalTo(compactedFirst));
    assertThat(firstSelfMerged, hasSize(1));
    assertThat(firstSelfMerged.get(0)[0], equalTo(3L));
  }
}
