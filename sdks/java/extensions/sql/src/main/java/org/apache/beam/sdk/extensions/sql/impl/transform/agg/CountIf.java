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
package org.apache.beam.sdk.extensions.sql.impl.transform.agg;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;

/**
 * Returns the count of TRUE values for expression. Returns 0 if there are zero input rows, or if
 * expression evaluates to FALSE or NULL for all rows.
 */
public class CountIf {
  private CountIf() {}

  public static Combine.CombineFn<Boolean, ?, Long> combineFn() {
    return new CountIfFn();
  }

  public static class CountIfFn extends Combine.CombineFn<Boolean, long[], Long> {
    private final Combine.CombineFn<Boolean, long[], Long> countFn =
        (Combine.CombineFn<Boolean, long[], Long>) Count.<Boolean>combineFn();

    @Override
    public long[] createAccumulator() {
      return countFn.createAccumulator();
    }

    @Override
    public long[] addInput(long[] accumulator, Boolean input) {
      if (Boolean.TRUE.equals(input)) {
        countFn.addInput(accumulator, input);
      }
      return accumulator;
    }

    @Override
    public long[] mergeAccumulators(Iterable<long[]> accumulators) {
      return countFn.mergeAccumulators(accumulators);
    }

    @Override
    public Long extractOutput(long[] accumulator) {
      return countFn.extractOutput(accumulator);
    }

    @Override
    public Coder<long[]> getAccumulatorCoder(CoderRegistry registry, Coder<Boolean> inputCoder)
        throws CannotProvideCoderException {
      return countFn.getAccumulatorCoder(registry, inputCoder);
    }
  }
}
