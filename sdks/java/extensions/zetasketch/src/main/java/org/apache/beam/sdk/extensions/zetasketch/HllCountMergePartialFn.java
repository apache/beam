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
package org.apache.beam.sdk.extensions.zetasketch;

import com.google.zetasketch.HyperLogLogPlusPlus;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Combine.CombineFn} for the {@link HllCount.MergePartial} combiner.
 *
 * @param <HllT> type of the HLL++ sketch to be merged
 */
class HllCountMergePartialFn<HllT>
    extends Combine.CombineFn<byte @Nullable [], @Nullable HyperLogLogPlusPlus<HllT>, byte[]> {

  private static final Logger LOG = LoggerFactory.getLogger(HllCountMergePartialFn.class);

  // Call HllCountMergePartialFn.create() to instantiate
  private HllCountMergePartialFn() {}

  static HllCountMergePartialFn<?> create() {
    return new HllCountMergePartialFn();
  }

  @Override
  public Coder<@Nullable HyperLogLogPlusPlus<HllT>> getAccumulatorCoder(
      CoderRegistry registry, Coder<byte @Nullable []> inputCoder) {
    // Use null to represent the "identity element" of the merge operation.
    return NullableCoder.of(HyperLogLogPlusPlusCoder.of());
  }

  @Override
  public @Nullable HyperLogLogPlusPlus<HllT> createAccumulator() {
    // Cannot create a sketch corresponding to an empty data set, because we do not know the sketch
    // type and precision. So use null to represent the "identity element" of the merge operation.
    return null;
  }

  @Override
  public @Nullable HyperLogLogPlusPlus<HllT> addInput(
      @Nullable HyperLogLogPlusPlus<HllT> accumulator, byte @Nullable [] input) {
    if (input == null) {
      LOG.warn(
          "Received a null and treated it as an empty sketch. "
              + "Consider replacing nulls with empty byte arrays (byte[0]) "
              + "in upstream transforms for better space-efficiency and safety.");
      return accumulator;
    } else if (input.length == 0) {
      return accumulator;
    } else if (accumulator == null) {
      @SuppressWarnings("unchecked")
      HyperLogLogPlusPlus<HllT> result =
          (HyperLogLogPlusPlus<HllT>) HyperLogLogPlusPlus.forProto(input);
      return result;
    } else {
      accumulator.merge(input);
      return accumulator;
    }
  }

  @Override
  // Spotbugs doesn't understand Nullable generics
  @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
  public @Nullable HyperLogLogPlusPlus<HllT> mergeAccumulators(
      Iterable<@Nullable HyperLogLogPlusPlus<HllT>> accumulators) {
    HyperLogLogPlusPlus<HllT> merged = createAccumulator();
    for (HyperLogLogPlusPlus<HllT> accumulator : accumulators) {
      if (accumulator == null) {
        continue;
      }
      if (merged == null) {
        @SuppressWarnings("unchecked")
        HyperLogLogPlusPlus<HllT> clonedAccumulator =
            (HyperLogLogPlusPlus<HllT>)
                HyperLogLogPlusPlus.forProto(accumulator.serializeToProto());
        // Cannot set merged to accumulator directly because we shouldn't mutate accumulator
        merged = clonedAccumulator;
      } else {
        merged.merge(accumulator);
      }
    }
    return merged;
  }

  @Override
  public byte[] extractOutput(@Nullable HyperLogLogPlusPlus<HllT> accumulator) {
    if (accumulator == null) {
      return new byte[0];
    } else {
      return accumulator.serializeToByteArray();
    }
  }
}
