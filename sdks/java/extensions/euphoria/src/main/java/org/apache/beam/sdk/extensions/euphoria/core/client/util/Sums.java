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
package org.apache.beam.sdk.extensions.euphoria.core.client.util;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Provides commonly used function objects around computing sums.
 *
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@Audience(Audience.Type.CLIENT)
@Deprecated
public class Sums {

  private static class SumFunction<T> implements ReduceByKey.CombineFunctionWithIdentity<T> {

    private final T identity;
    private final TypeDescriptor<T> valueDesc;
    private final BinaryFunction<T, T, T> reduce;

    SumFunction(T identity, TypeDescriptor<T> valueDesc, BinaryFunction<T, T, T> reduce) {
      this.identity = identity;
      this.valueDesc = valueDesc;
      this.reduce = reduce;
    }

    @Override
    public T identity() {
      return identity;
    }

    @Override
    public TypeDescriptor<T> valueDesc() {
      return valueDesc;
    }

    @Override
    public T apply(T left, T right) {
      return reduce.apply(left, right);
    }
  }

  private static final SumFunction<Long> SUMS_OF_LONG =
      new SumFunction<>(0L, TypeDescriptors.longs(), (a, b) -> a + b);
  private static final SumFunction<Integer> SUMS_OF_INT =
      new SumFunction<>(0, TypeDescriptors.integers(), (a, b) -> a + b);
  private static final SumFunction<Float> SUMS_OF_FLOAT =
      new SumFunction<>(0.0f, TypeDescriptors.floats(), (a, b) -> a + b);
  private static final SumFunction<Double> SUMS_OF_DOUBLE =
      new SumFunction<>(0.0, TypeDescriptors.doubles(), (a, b) -> a + b);

  private Sums() {}

  public static ReduceByKey.CombineFunctionWithIdentity<Long> ofLongs() {
    return SUMS_OF_LONG;
  }

  public static ReduceByKey.CombineFunctionWithIdentity<Integer> ofInts() {
    return SUMS_OF_INT;
  }

  public static ReduceByKey.CombineFunctionWithIdentity<Float> ofFloats() {
    return SUMS_OF_FLOAT;
  }

  public static ReduceByKey.CombineFunctionWithIdentity<Double> ofDoubles() {
    return SUMS_OF_DOUBLE;
  }
}
