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

package org.apache.beam.sdk.testing;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CombineFnTester}.
 */
@RunWith(JUnit4.class)
public class CombineFnTesterTest {
  @Test
  public void checksMergeWithEmptyAccumulators() {
    final AtomicBoolean sawEmpty = new AtomicBoolean(false);
    CombineFn<Integer, Integer, Integer> combineFn =
        new CombineFn<Integer, Integer, Integer>() {
          @Override
          public Integer createAccumulator() {
            return 0;
          }

          @Override
          public Integer addInput(Integer accumulator, Integer input) {
            return accumulator + input;
          }

          @Override
          public Integer mergeAccumulators(Iterable<Integer> accumulators) {
            int result = 0;
            for (int accum : accumulators) {
              if (accum == 0) {
                sawEmpty.set(true);
              }
              result += accum;
            }
            return result;
          }

          @Override
          public Integer extractOutput(Integer accumulator) {
            return accumulator;
          }
        };

    CombineFnTester.testCombineFn(combineFn, Arrays.asList(1, 2, 3, 4, 5), 15);
    assertThat(sawEmpty.get(), is(true));
  }

  @Test
  public void checksWithSingleShard() {
    final AtomicBoolean sawSingleShard = new AtomicBoolean();
    CombineFn<Integer, Integer, Integer> combineFn =
        new CombineFn<Integer, Integer, Integer>() {
          int accumCount = 0;

          @Override
          public Integer createAccumulator() {
            accumCount++;
            return 0;
          }

          @Override
          public Integer addInput(Integer accumulator, Integer input) {
            return accumulator + input;
          }

          @Override
          public Integer mergeAccumulators(Iterable<Integer> accumulators) {
            int result = 0;
            for (int accum : accumulators) {
              result += accum;
            }
            return result;
          }

          @Override
          public Integer extractOutput(Integer accumulator) {
            if (accumCount == 1) {
              sawSingleShard.set(true);
            }
            accumCount = 0;
            return accumulator;
          }
        };

    CombineFnTester.testCombineFn(combineFn, Arrays.asList(1, 2, 3, 4, 5), 15);
    assertThat(sawSingleShard.get(), is(true));
  }

  @Test
  public void checksWithShards() {
    final AtomicBoolean sawManyShards = new AtomicBoolean();
    CombineFn<Integer, Integer, Integer> combineFn =
        new CombineFn<Integer, Integer, Integer>() {

          @Override
          public Integer createAccumulator() {
            return 0;
          }

          @Override
          public Integer addInput(Integer accumulator, Integer input) {
            return accumulator + input;
          }

          @Override
          public Integer mergeAccumulators(Iterable<Integer> accumulators) {
            if (Iterables.size(accumulators) > 2) {
              sawManyShards.set(true);
            }
            int result = 0;
            for (int accum : accumulators) {
              result += accum;
            }
            return result;
          }

          @Override
          public Integer extractOutput(Integer accumulator) {
            return accumulator;
          }
        };

    CombineFnTester.testCombineFn(
        combineFn, Arrays.asList(1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3), 30);
    assertThat(sawManyShards.get(), is(true));
  }

  @Test
  public void checksWithMultipleMerges() {
    final AtomicBoolean sawMultipleMerges = new AtomicBoolean();
    CombineFn<Integer, KV<Integer, Integer>, Integer> combineFn =
        new CombineFn<Integer, KV<Integer, Integer>, Integer>() {

          @Override
          public KV<Integer, Integer> createAccumulator() {
            return KV.of(0, 0);
          }

          @Override
          public KV<Integer, Integer> addInput(KV<Integer, Integer> accumulator, Integer input) {
            return KV.of(accumulator.getKey() + input, accumulator.getValue());
          }

          @Override
          public KV<Integer, Integer> mergeAccumulators(
            Iterable<KV<Integer, Integer>> accumulators) {
            int result = 0;
            int numMerges = 0;
            for (KV<Integer, Integer> accum : accumulators) {
              result += accum.getKey();
              numMerges += accum.getValue();
            }
            return KV.of(result, numMerges + 1);
          }

          @Override
          public Integer extractOutput(KV<Integer, Integer> accumulator) {
            if (accumulator.getValue() > 1) {
              sawMultipleMerges.set(true);
            }
            return accumulator.getKey();
          }
        };

    CombineFnTester.testCombineFn(combineFn, Arrays.asList(1, 1, 2, 2, 3, 3, 4, 4, 5, 5), 30);
    assertThat(sawMultipleMerges.get(), is(true));
  }

  @Test
  public void checksAlternateOrder() {
    final AtomicBoolean sawOutOfOrder = new AtomicBoolean();
    CombineFn<Integer, List<Integer>, Integer> combineFn =
        new CombineFn<Integer, List<Integer>, Integer>() {
          @Override
          public List<Integer> createAccumulator() {
            return new ArrayList<>();
          }

          @Override
          public List<Integer> addInput(List<Integer> accumulator, Integer input) {
            // If the input is being added to an empty accumulator, it's not known to be
            // out of order, and it cannot be compared to the previous element. If the elements
            // are out of order (relative to the input) a greater element will be added before
            // a smaller one.
            if (!accumulator.isEmpty() && accumulator.get(accumulator.size() - 1) > input) {
              sawOutOfOrder.set(true);
            }
            accumulator.add(input);
            return accumulator;
          }

          @Override
          public List<Integer> mergeAccumulators(Iterable<List<Integer>> accumulators) {
            List<Integer> result = new ArrayList<>();
            for (List<Integer> accum : accumulators) {
              result.addAll(accum);
            }
            return result;
          }

          @Override
          public Integer extractOutput(List<Integer> accumulator) {
            int value = 0;
            for (int i : accumulator) {
              value += i;
            }
            return value;
          }
        };

    CombineFnTester.testCombineFn(
        combineFn, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14), 105);
    assertThat(sawOutOfOrder.get(), is(true));
  }

  @Test
  public void usesMatcher() {
    final AtomicBoolean matcherUsed = new AtomicBoolean();
    Matcher<Integer> matcher =
        new TypeSafeMatcher<Integer>() {
          @Override
          public void describeTo(Description description) {}

          @Override
          protected boolean matchesSafely(Integer item) {
            matcherUsed.set(true);
            return item == 30;
          }
        };
    CombineFnTester.testCombineFn(
        Sum.ofIntegers(), Arrays.asList(1, 1, 2, 2, 3, 3, 4, 4, 5, 5), matcher);
    assertThat(matcherUsed.get(), is(true));
    try {
      CombineFnTester.testCombineFn(
          Sum.ofIntegers(), Arrays.asList(1, 2, 3, 4, 5), Matchers.not(Matchers.equalTo(15)));
    } catch (AssertionError ignored) {
      // Success! Return to avoid the call to fail();
      return;
    }
    fail("The matcher should have failed, throwing an error");
  }
}
