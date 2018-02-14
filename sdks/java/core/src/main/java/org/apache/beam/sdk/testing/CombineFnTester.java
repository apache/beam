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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.hamcrest.Matcher;

/**
 * Utilities for testing {@link CombineFn CombineFns}. Ensures that the {@link CombineFn} gives
 * correct results across various permutations and shardings of the input.
 */
public class CombineFnTester {
  /**
   * Tests that the the {@link CombineFn}, when applied to the provided input, produces the provided
   * output. Tests a variety of permutations of the input.
   */
  public static <InputT, AccumT, OutputT> void testCombineFn(
      CombineFn<InputT, AccumT, OutputT> fn, List<InputT> input, final OutputT expected) {
    testCombineFn(fn, input, is(expected));
    Collections.shuffle(input);
    testCombineFn(fn, input, is(expected));
  }

  public static <InputT, AccumT, OutputT> void testCombineFn(
      CombineFn<InputT, AccumT, OutputT> fn, List<InputT> input, Matcher<? super OutputT> matcher) {
    int size = input.size();
    checkCombineFnShardsMultipleOrders(fn, Collections.singletonList(input), matcher);
    checkCombineFnShardsMultipleOrders(fn, shardEvenly(input, 2), matcher);
    if (size > 4) {
      checkCombineFnShardsMultipleOrders(fn, shardEvenly(input, size / 2), matcher);
      checkCombineFnShardsMultipleOrders(
          fn, shardEvenly(input, (int) (size / Math.sqrt(size))), matcher);
    }
    checkCombineFnShardsMultipleOrders(fn, shardExponentially(input, 1.4), matcher);
    checkCombineFnShardsMultipleOrders(fn, shardExponentially(input, 2), matcher);
    checkCombineFnShardsMultipleOrders(fn, shardExponentially(input, Math.E), matcher);
  }

  private static <InputT, AccumT, OutputT> void checkCombineFnShardsMultipleOrders(
      CombineFn<InputT, AccumT, OutputT> fn,
      List<? extends Iterable<InputT>> shards,
      Matcher<? super OutputT> matcher) {
    checkCombineFnShardsSingleMerge(fn, shards, matcher);
    checkCombineFnShardsWithEmptyAccumulators(fn, shards, matcher);
    checkCombineFnShardsIncrementalMerging(fn, shards, matcher);
    Collections.shuffle(shards);
    checkCombineFnShardsSingleMerge(fn, shards, matcher);
    checkCombineFnShardsWithEmptyAccumulators(fn, shards, matcher);
    checkCombineFnShardsIncrementalMerging(fn, shards, matcher);
  }

  private static <InputT, AccumT, OutputT> void checkCombineFnShardsSingleMerge(
      CombineFn<InputT, AccumT, OutputT> fn,
      Iterable<? extends Iterable<InputT>> shards,
      Matcher<? super OutputT> matcher) {
    List<AccumT> accumulators = combineInputs(fn, shards);
    AccumT merged = fn.mergeAccumulators(accumulators);
    assertThat(fn.extractOutput(merged), matcher);
  }

  private static <InputT, AccumT, OutputT> void checkCombineFnShardsWithEmptyAccumulators(
      CombineFn<InputT, AccumT, OutputT> fn,
      Iterable<? extends Iterable<InputT>> shards,
      Matcher<? super OutputT> matcher) {
    List<AccumT> accumulators = combineInputs(fn, shards);
    accumulators.add(0, fn.createAccumulator());
    accumulators.add(fn.createAccumulator());
    AccumT merged = fn.mergeAccumulators(accumulators);
    assertThat(fn.extractOutput(merged), matcher);
  }

  private static <InputT, AccumT, OutputT> void checkCombineFnShardsIncrementalMerging(
      CombineFn<InputT, AccumT, OutputT> fn,
      List<? extends Iterable<InputT>> shards,
      Matcher<? super OutputT> matcher) {
    AccumT accumulator = shards.isEmpty() ? fn.createAccumulator() : null;
    for (AccumT inputAccum : combineInputs(fn, shards)) {
      if (accumulator == null) {
        accumulator = inputAccum;
      } else {
        accumulator = fn.mergeAccumulators(Arrays.asList(accumulator, inputAccum));
      }
      fn.extractOutput(accumulator); // Extract output to simulate multiple firings.
    }
    assertThat(fn.extractOutput(accumulator), matcher);
  }

  private static <InputT, AccumT, OutputT> List<AccumT> combineInputs(
      CombineFn<InputT, AccumT, OutputT> fn, Iterable<? extends Iterable<InputT>> shards) {
    List<AccumT> accumulators = new ArrayList<>();
    int maybeCompact = 0;
    for (Iterable<InputT> shard : shards) {
      AccumT accumulator = fn.createAccumulator();
      for (InputT elem : shard) {
        accumulator = fn.addInput(accumulator, elem);
      }
      if (maybeCompact++ % 2 == 0) {
        accumulator = fn.compact(accumulator);
      }
      accumulators.add(accumulator);
    }
    return accumulators;
  }

  private static <T> List<List<T>> shardEvenly(List<T> input, int numShards) {
    List<List<T>> shards = new ArrayList<>(numShards);
    for (int i = 0; i < numShards; i++) {
      shards.add(input.subList(i * input.size() / numShards,
          (i + 1) * input.size() / numShards));
    }
    return shards;
  }

  private static <T> List<List<T>> shardExponentially(
      List<T> input, double base) {
    assert base > 1.0;
    List<List<T>> shards = new ArrayList<>();
    int end = input.size();
    while (end > 0) {
      int start = (int) (end / base);
      shards.add(input.subList(start, end));
      end = start;
    }
    return shards;
  }
}
