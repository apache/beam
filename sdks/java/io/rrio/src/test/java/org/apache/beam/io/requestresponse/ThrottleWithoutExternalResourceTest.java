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
package org.apache.beam.io.requestresponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.stat.inference.ChiSquareTest;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ThrottleWithoutExternalResource}. */
@RunWith(JUnit4.class)
public class ThrottleWithoutExternalResourceTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  /**
   * The purpose of this test is to assert, in the setting of elements greater than the desired
   * rate, that a Chi Square hypothesis cannot be rejected against a uniform integer distribution.
   * In other words, does {@link ThrottleWithoutExternalResource#assignChannels} uniformly
   * distribute the key space among the elements? While we cannot "prove" a distribution, failure to
   * reject a null hypothesis suggests that the code is doing what we expect.
   */
  @Test
  public void givenNonSparseElements_thenAssignChannelsDistribution_cannotRejectChiSqNullTest() {
    Rate rate = Rate.of(1000, Duration.standardSeconds(1L));
    testAssignChannelsDistribution(rate, 10000);
  }

  /**
   * This test is similar to {@link
   * #givenNonSparseElements_thenAssignChannelsDistribution_cannotRejectChiSqNullTest()} in the
   * setting of elements less than the desired rate.
   */
  @Test
  public void givenSparseElements_thenAssignChannelsNominallyDistributed() {
    Rate rate = Rate.of(1000, Duration.standardSeconds(1L));
    testAssignChannelsDistribution(rate, 100);
  }

  private void testAssignChannelsDistribution(Rate rate, int size) {
    List<Integer> list = Stream.iterate(0, i -> i + 1).limit(size).collect(Collectors.toList());

    ThrottleWithoutExternalResource<Integer> transform = transformOf(rate);

    PCollection<KV<Integer, Integer>> kv =
        pipeline.apply(Create.of(list)).apply(transform.assignChannels());
    PCollection<KV<Integer, Long>> countPerKey = kv.apply(Count.perKey());
    PAssert.that(countPerKey)
        .satisfies(
            itr -> {
              // Instantiate a uniform integer distribution [0, Rate#numElements).
              UniformIntegerDistribution distribution =
                  new UniformIntegerDistribution(0, rate.getNumElements() - 1);
              ChiSquareTest chiSquareTest = new ChiSquareTest();
              // Compute the expected distribution of counts.
              double[] expected = new double[rate.getNumElements()];
              for (int i = 0; i < expected.length; i++) {
                expected[i] =
                    (double) list.size()
                        / (double) rate.getNumElements()
                        * distribution.probability(i);
              }
              // Compute the observed distribution of counts.
              long[] observed = new long[rate.getNumElements()];
              for (KV<Integer, Long> entry : itr) {
                observed[entry.getKey()] = entry.getValue();
              }

              // Perform the statistical test.
              boolean chiSq = chiSquareTest.chiSquareTest(expected, observed, 0.05);

              // Assert that we cannot reject the null hypothesis.
              assertThat(chiSq, is(false));
              return null;
            });

    pipeline.run();
  }

  @Test
  public void givenSparseElementPulse_thenEmitsAllImmediately() {
    Rate rate = Rate.of(1000, Duration.standardSeconds(1L));
    List<Integer> list = Stream.iterate(0, i -> i + 1).limit(100).collect(Collectors.toList());

    PCollection<Integer> throttled = pipeline.apply(Create.of(list)).apply(transformOf(rate));

    PAssert.that(throttled).containsInAnyOrder(list);

    pipeline.run();
  }

  @Test
  public void offsetRange_isSerializable() {
    SerializableUtils.ensureSerializable(ThrottleWithoutExternalResource.OffsetRange.empty());
  }

  @Test
  public void offsetRangeTracker_isSerializable() {
    SerializableUtils.ensureSerializable(
        ThrottleWithoutExternalResource.OffsetRange.empty().newTracker());
  }

  @Test
  public void configuration_isSerializable() {
    SerializableUtils.ensureSerializable(
        ThrottleWithoutExternalResource.Configuration.builder()
            .setMaximumRate(Rate.of(1, Duration.ZERO))
            .build());
  }

  private static ThrottleWithoutExternalResource<Integer> transformOf(Rate rate) {
    return ThrottleWithoutExternalResource.of(configurationOf(rate));
  }

  private static ThrottleWithoutExternalResource.Configuration configurationOf(Rate rate) {
    return ThrottleWithoutExternalResource.Configuration.builder().setMaximumRate(rate).build();
  }
}
