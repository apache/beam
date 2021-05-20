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
package org.apache.beam.sdk.extensions.sql.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.sql.udf.AggregateFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link LazyAggregateCombineFn}. */
@RunWith(JUnit4.class)
public class LazyAggregateCombineFnTest {

  @Test
  public void getAccumulatorCoderInfersCoderForWildcardTypeParameter()
      throws CannotProvideCoderException {
    LazyAggregateCombineFn<Long, ?, ?> combiner = new LazyAggregateCombineFn<>(new Sum());
    Coder<?> coder = combiner.getAccumulatorCoder(CoderRegistry.createDefault(), VarLongCoder.of());
    assertThat(coder, instanceOf(VarLongCoder.class));
  }

  @Test
  public void mergeAccumulators() {
    LazyAggregateCombineFn<Long, Long, Long> combiner = new LazyAggregateCombineFn<>(new Sum());
    long merged = combiner.mergeAccumulators(ImmutableList.of(1L, 1L));
    assertEquals(2L, merged);
  }

  public static class Sum implements AggregateFn<Long, Long, Long> {

    @Override
    public Long createAccumulator() {
      return 0L;
    }

    @Override
    public Long addInput(Long mutableAccumulator, Long input) {
      return mutableAccumulator + input;
    }

    @Override
    public Long mergeAccumulators(Long mutableAccumulator, Iterable<Long> immutableAccumulators) {
      for (Long x : immutableAccumulators) {
        mutableAccumulator += x;
      }
      return mutableAccumulator;
    }

    @Override
    public Long extractOutput(Long mutableAccumulator) {
      return mutableAccumulator;
    }
  }
}
