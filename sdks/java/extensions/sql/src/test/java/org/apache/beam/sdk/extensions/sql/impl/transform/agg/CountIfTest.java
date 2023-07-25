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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.junit.Test;

/** Unit tests for {@link CountIf}. */
public class CountIfTest {

  @Test
  public void testCreatesEmptyAccumulator() {
    long[] accumulator = (long[]) CountIf.combineFn().createAccumulator();

    assertEquals(0, accumulator[0]);
  }

  @Test
  public void testReturnsAccumulatorUnchangedForNullInput() {
    Combine.CombineFn countIfFn = CountIf.combineFn();
    long[] accumulator = (long[]) countIfFn.addInput(countIfFn.createAccumulator(), null);

    assertEquals(0L, accumulator[0]);
  }

  @Test
  public void testAddsInputToAccumulator() {
    Combine.CombineFn countIfFn = CountIf.combineFn();
    long[] accumulator = (long[]) countIfFn.addInput(countIfFn.createAccumulator(), Boolean.TRUE);

    assertEquals(1L, accumulator[0]);
  }

  @Test
  public void testCreatesAccumulatorCoder() throws CannotProvideCoderException {
    assertNotNull(
        CountIf.combineFn().getAccumulatorCoder(CoderRegistry.createDefault(), BooleanCoder.of()));
  }

  @Test
  public void testMergeAccumulators() {
    Combine.CombineFn countIfFn = CountIf.combineFn();
    List<long[]> accums = Arrays.asList(new long[] {2}, new long[] {2});
    long[] accumulator = (long[]) countIfFn.mergeAccumulators(accums);

    assertEquals(4L, accumulator[0]);
  }

  @Test
  public void testExtractsOutput() {
    Combine.CombineFn countIfFn = CountIf.combineFn();

    assertEquals(0L, countIfFn.extractOutput(countIfFn.createAccumulator()));
  }
}
