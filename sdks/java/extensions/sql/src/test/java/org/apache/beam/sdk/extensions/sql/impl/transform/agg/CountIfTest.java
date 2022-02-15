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
import org.apache.beam.sdk.coders.CoderRegistry;
import org.junit.Test;

/** Unit tests for {@link CountIf}. */
public class CountIfTest {

  @Test
  public void testCreatesEmptyAccumulator() {
    assertEquals(CountIf.CountIfFn.Accum.empty(), CountIf.combineFn().createAccumulator());
  }

  @Test
  public void testReturnsAccumulatorUnchangedForNullInput() {
    CountIf.CountIfFn countIfFn = new CountIf.CountIfFn();
    CountIf.CountIfFn.Accum accumulator = countIfFn.createAccumulator();
    assertEquals(accumulator, countIfFn.addInput(accumulator, null));
  }

  @Test
  public void testAddsInputToAccumulator() {
    CountIf.CountIfFn countIfFn = new CountIf.CountIfFn();
    CountIf.CountIfFn.Accum expectedAccumulator = CountIf.CountIfFn.Accum.of(false, 1);

    assertEquals(
        expectedAccumulator, countIfFn.addInput(countIfFn.createAccumulator(), Boolean.TRUE));
  }

  @Test
  public void testCreatesAccumulatorCoder() {
    assertNotNull(
        CountIf.combineFn().getAccumulatorCoder(CoderRegistry.createDefault(), BooleanCoder.of()));
  }

  @Test
  public void testMergeAccumulators() {
    CountIf.CountIfFn countIfFn = new CountIf.CountIfFn();
    List<CountIf.CountIfFn.Accum> accums =
        Arrays.asList(CountIf.CountIfFn.Accum.of(false, 2), CountIf.CountIfFn.Accum.of(false, 2));
    CountIf.CountIfFn.Accum expectedAccumulator = CountIf.CountIfFn.Accum.of(false, 4);

    assertEquals(expectedAccumulator, countIfFn.mergeAccumulators(accums));
  }

  @Test
  public void testExtractsOutput() {
    CountIf.CountIfFn countIfFn = new CountIf.CountIfFn();
    CountIf.CountIfFn.Accum expectedAccumulator = countIfFn.createAccumulator();
    assertEquals(Long.valueOf(0), countIfFn.extractOutput(expectedAccumulator));
  }
}
