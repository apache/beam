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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matcher;

/**
 * Utilities for testing {@link CombineFn CombineFns}. Ensures that the {@link CombineFn} gives
 * correct results across various permutations and shardings of the input.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CombineFnTester {
  /**
   * Tests that the {@link CombineFn}, when applied to the provided input, produces the provided
   * output. Tests a variety of permutations of the input.
   */
  public static <InputT, AccumT, OutputT> void testCombineFn(
      CombineFn<InputT, AccumT, OutputT> fn, List<InputT> input, final OutputT expected) {
    testCombineFnHelper(fn, input, is(expected), null);
    Collections.shuffle(input);
    testCombineFnHelper(fn, input, is(expected), null);
  }

  public static <InputT, AccumT, OutputT> void testCombineFn(
      CombineFn<InputT, AccumT, OutputT> fn, List<InputT> input, Matcher<? super OutputT> matcher) {
    testCombineFnHelper(fn, input, matcher, null);
  }

  /**
   * Tests that the {@link CombineFn}, when applied to the provided input, produces the provided
   * output with intermediate encoding/decoding. Tests a variety of permutations of the input.
   */
  public static <InputT, AccumT, OutputT> void testCombineFnWithCoding(
      CombineFn<InputT, AccumT, OutputT> fn,
      List<InputT> input,
      final OutputT expected,
      final Coder<AccumT> accumulatorCoder) {
    testCombineFnHelper(fn, input, is(expected), accumulatorCoder);
    Collections.shuffle(input);
    testCombineFnHelper(fn, input, is(expected), accumulatorCoder);
  }

  public static <InputT, AccumT, OutputT> void testCombineFnWithCoding(
      CombineFn<InputT, AccumT, OutputT> fn,
      List<InputT> input,
      Matcher<? super OutputT> matcher,
      final Coder<AccumT> accumulatorCoder) {
    testCombineFnHelper(fn, input, matcher, accumulatorCoder);
  }

  private static <InputT, AccumT, OutputT> void testCombineFnHelper(
      CombineFn<InputT, AccumT, OutputT> fn,
      List<InputT> input,
      Matcher<? super OutputT> matcher,
      @Nullable Coder<AccumT> accumulatorCoder) {
    int size = input.size();
    checkCombineFnShardsMultipleOrders(
        fn, Collections.singletonList(input), matcher, accumulatorCoder);
    checkCombineFnShardsMultipleOrders(fn, shardEvenly(input, 2), matcher, accumulatorCoder);
    if (size > 4) {
      checkCombineFnShardsMultipleOrders(
          fn, shardEvenly(input, size / 2), matcher, accumulatorCoder);
      checkCombineFnShardsMultipleOrders(
          fn, shardEvenly(input, (int) (size / Math.sqrt(size))), matcher, accumulatorCoder);
    }
    checkCombineFnShardsMultipleOrders(
        fn, shardExponentially(input, 1.4), matcher, accumulatorCoder);
    checkCombineFnShardsMultipleOrders(fn, shardExponentially(input, 2), matcher, accumulatorCoder);
    checkCombineFnShardsMultipleOrders(
        fn, shardExponentially(input, Math.E), matcher, accumulatorCoder);
  }

  private static <InputT, AccumT, OutputT> void checkCombineFnShardsMultipleOrders(
      CombineFn<InputT, AccumT, OutputT> fn,
      List<? extends Iterable<InputT>> shards,
      Matcher<? super OutputT> matcher,
      @Nullable Coder<AccumT> accumulatorCoder) {
    checkCombineFnShardsSingleMerge(fn, shards, matcher, accumulatorCoder);
    checkCombineFnShardsWithEmptyAccumulators(fn, shards, matcher, accumulatorCoder);
    checkCombineFnShardsIncrementalMerging(fn, shards, matcher, accumulatorCoder);
    Collections.shuffle(shards);
    checkCombineFnShardsSingleMerge(fn, shards, matcher, accumulatorCoder);
    checkCombineFnShardsWithEmptyAccumulators(fn, shards, matcher, accumulatorCoder);
    checkCombineFnShardsIncrementalMerging(fn, shards, matcher, accumulatorCoder);
  }

  private static <InputT, AccumT, OutputT> void checkCombineFnShardsSingleMerge(
      CombineFn<InputT, AccumT, OutputT> fn,
      Iterable<? extends Iterable<InputT>> shards,
      Matcher<? super OutputT> matcher,
      @Nullable Coder<AccumT> accumulatorCoder) {
    List<AccumT> accumulators = combineInputs(fn, shards, accumulatorCoder);

    AccumT merged = fn.mergeAccumulators(accumulators);
    assertThat(fn.extractOutput(merged), matcher);
  }

  private static <InputT, AccumT, OutputT> void checkCombineFnShardsWithEmptyAccumulators(
      CombineFn<InputT, AccumT, OutputT> fn,
      Iterable<? extends Iterable<InputT>> shards,
      Matcher<? super OutputT> matcher,
      @Nullable Coder<AccumT> accumulatorCoder) {
    List<AccumT> accumulators = combineInputs(fn, shards, accumulatorCoder);
    accumulators.add(0, fn.createAccumulator());
    accumulators.add(fn.createAccumulator());
    AccumT merged = encodeDecode(fn.mergeAccumulators(accumulators), accumulatorCoder);
    assertThat(fn.extractOutput(merged), matcher);
  }

  private static <InputT, AccumT, OutputT> void checkCombineFnShardsIncrementalMerging(
      CombineFn<InputT, AccumT, OutputT> fn,
      List<? extends Iterable<InputT>> shards,
      Matcher<? super OutputT> matcher,
      @Nullable Coder<AccumT> accumulatorCoder) {
    AccumT accumulator = shards.isEmpty() ? fn.createAccumulator() : null;
    for (AccumT inputAccum : combineInputs(fn, shards, accumulatorCoder)) {
      if (accumulator == null) {
        accumulator = inputAccum;
      } else {
        accumulator = fn.mergeAccumulators(Arrays.asList(accumulator, inputAccum));
      }
      fn.extractOutput(accumulator); // Extract output to simulate multiple firings
    }
    assertThat(fn.extractOutput(encodeDecode(accumulator, accumulatorCoder)), matcher);
  }

  private static <InputT, AccumT, OutputT> List<AccumT> combineInputs(
      CombineFn<InputT, AccumT, OutputT> fn,
      Iterable<? extends Iterable<InputT>> shards,
      @Nullable Coder<AccumT> accumulatorCoder) {
    List<AccumT> accumulators = new ArrayList<>();
    int maybeCompact = 0;
    for (Iterable<InputT> shard : shards) {
      AccumT accumulator = fn.createAccumulator();
      for (InputT elem : shard) {
        accumulator = fn.addInput(accumulator, elem);
      }
      if (maybeCompact++ % 2 == 0) {
        accumulator = fn.compact(encodeDecode(accumulator, accumulatorCoder));
      }
      accumulators.add(accumulator);
    }
    return accumulators;
  }

  private static <AccumT> AccumT encodeDecode(AccumT accumulator, @Nullable Coder<AccumT> coder) {
    if (coder == null) {
      return accumulator;
    }
    ByteStringOutputStream outStream = new ByteStringOutputStream();
    try {
      coder.encode(accumulator, outStream);
      return coder.decode(outStream.toByteString().newInput());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static <T> List<List<T>> shardEvenly(List<T> input, int numShards) {
    List<List<T>> shards = new ArrayList<>(numShards);
    for (int i = 0; i < numShards; i++) {
      shards.add(input.subList(i * input.size() / numShards, (i + 1) * input.size() / numShards));
    }
    return shards;
  }

  private static <T> List<List<T>> shardExponentially(List<T> input, double base) {
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
