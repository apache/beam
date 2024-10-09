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

import static java.math.BigDecimal.ZERO;
import static org.apache.beam.sdk.extensions.sql.impl.transform.agg.VarianceAccumulator.newVarianceAccumulator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/** Unit tests for {@link VarianceFnTest}. */
@RunWith(Parameterized.class)
public class VarianceFnTest {
  private static final BigDecimal FIFTEEN = new BigDecimal(15);
  private static final BigDecimal THREE = new BigDecimal(3);
  private static final BigDecimal FOUR = new BigDecimal(4);

  @Parameters(name = "varianceFn {index}")
  public static Iterable<Object[]> varianceFns() {
    return Arrays.asList(
        new Object[][] {
          {
            VarianceFn.newPopulation(BigDecimal::intValue),
            newVarianceAccumulator(FIFTEEN, THREE, ZERO),
            5
          },
          {
            VarianceFn.newSample(BigDecimal::intValue),
            newVarianceAccumulator(FIFTEEN, FOUR, ZERO),
            5
          }
        });
  }

  private VarianceFn varianceFn;
  private VarianceAccumulator testAccumulatorInput;
  private int expectedExtractedResult;

  public VarianceFnTest(
      VarianceFn varianceFn,
      VarianceAccumulator testAccumulatorInput,
      int expectedExtractedResult) {

    this.varianceFn = varianceFn;
    this.testAccumulatorInput = testAccumulatorInput;
    this.expectedExtractedResult = expectedExtractedResult;
  }

  @Test
  public void testCreatesEmptyAccumulator() {
    assertEquals(VarianceAccumulator.EMPTY, varianceFn.createAccumulator());
  }

  @Test
  public void testReturnsAccumulatorUnchangedForNullInput() {
    VarianceAccumulator accumulator = newVarianceAccumulator(ZERO, BigDecimal.ONE, BigDecimal.TEN);

    assertEquals(accumulator, varianceFn.addInput(accumulator, null));
  }

  @Test
  public void testAddsInputToAccumulator() {
    VarianceAccumulator expectedAccumulator =
        newVarianceAccumulator(ZERO, BigDecimal.ONE, new BigDecimal(2));

    assertEquals(
        expectedAccumulator,
        varianceFn.addInput(varianceFn.createAccumulator(), new BigDecimal(2)));
  }

  @Test
  public void testCreatesAccumulatorCoder() {
    assertNotNull(
        varianceFn.getAccumulatorCoder(CoderRegistry.createDefault(null), VarIntCoder.of()));
  }

  @Test
  public void testExtractsOutput() {
    assertEquals(expectedExtractedResult, varianceFn.extractOutput(testAccumulatorInput));
  }
}
