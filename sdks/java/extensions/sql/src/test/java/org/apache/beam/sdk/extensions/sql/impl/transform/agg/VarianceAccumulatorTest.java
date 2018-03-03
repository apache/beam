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

import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static org.apache.beam.sdk.extensions.sql.impl.transform.agg.VarianceAccumulator.newVarianceAccumulator;
import static org.apache.beam.sdk.extensions.sql.impl.transform.agg.VarianceAccumulator.ofSingleElement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.math.BigDecimal;
import org.junit.Test;

/**
 * Unit tests for {@link VarianceAccumulator}.
 */
public class VarianceAccumulatorTest {
  private static final double DELTA = 1e-7;
  private static final BigDecimal FIFTEEN = new BigDecimal(15);
  private static final BigDecimal SIXTEEN = new BigDecimal(16);
  private static final BigDecimal THREE = new BigDecimal(3);
  private static final BigDecimal FOUR = new BigDecimal(4);
  private static final BigDecimal FIVE = new BigDecimal(5);

  @Test
  public void testInitialized() {
    VarianceAccumulator accumulator = newVarianceAccumulator(FIFTEEN, THREE, FOUR);

    assertNotEquals(VarianceAccumulator.EMPTY, accumulator);
    assertEquals(FIFTEEN, accumulator.variance());
    assertEquals(THREE, accumulator.count());
    assertEquals(FOUR, accumulator.sum());
  }

  @Test
  public void testEmpty() {
    VarianceAccumulator zeroAccumulator = newVarianceAccumulator(ZERO, ZERO, ZERO);

    assertEquals(zeroAccumulator, VarianceAccumulator.EMPTY);
    assertEquals(zeroAccumulator, VarianceAccumulator.ofZeroElements());
  }

  @Test
  public void testAccumulatorOfSingleElement() {
    VarianceAccumulator accumulatorOfSingleElement = ofSingleElement(THREE);

    assertEquals(newVarianceAccumulator(ZERO, ONE, THREE), accumulatorOfSingleElement);
  }

  @Test
  public void testCombinesEmptyWithEmpty() {
    VarianceAccumulator result = VarianceAccumulator.EMPTY
        .combineWith(VarianceAccumulator.EMPTY);

    assertEquals(VarianceAccumulator.EMPTY, result);
  }

  @Test
  public void testCombinesEmptyWithNonEmpty() {
    VarianceAccumulator result = VarianceAccumulator.EMPTY.combineWith(ofSingleElement(THREE));

    assertEquals(newVarianceAccumulator(ZERO, ONE, THREE), result);
  }

  @Test
  public void testCombinesNonEmptyWithEmpty() {
    VarianceAccumulator result = ofSingleElement(THREE).combineWith(VarianceAccumulator.EMPTY);

    assertEquals(newVarianceAccumulator(ZERO, ONE, THREE), result);
  }

  @Test
  public void testCombinesNonTrivial() {
    VarianceAccumulator accumulator1 = newVarianceAccumulator(FIFTEEN, THREE, FOUR);
    VarianceAccumulator accumulator2 = newVarianceAccumulator(SIXTEEN, FOUR, FIVE);

    // values:
    //   var(x)=15, m=3, sum(x)=4;
    //   var(y)=16, n=4, sum(y)=5;
    //
    // formula:
    //   var(combine(x,y)) = var(x) + var(y) + increment();
    //   increment() = m/(n(m+n))  *  (sum(x) * n/m  - sum(y))^2;
    //
    // result:
    //   increment() = 3/(4(3+4))  *  (4 * 4/3 - 5)^2 = 0.107142857 * 0.111111111 = 0.011904762
    //   var(combine(x,y)) = 15 + 16 + increment() = 31.011904762
    VarianceAccumulator expectedCombined = newVarianceAccumulator(
        FIFTEEN.add(SIXTEEN).add(new BigDecimal(0.011904762)),
        THREE.add(FOUR),
        FOUR.add(FIVE));

    VarianceAccumulator combined1 = accumulator1.combineWith(accumulator2);
    VarianceAccumulator combined2 = accumulator2.combineWith(accumulator1);

    assertEquals(
        expectedCombined.variance().doubleValue(),
        combined1.variance().doubleValue(),
        DELTA);

    assertEquals(
        expectedCombined.variance().doubleValue(),
        combined2.variance().doubleValue(),
        DELTA);

    assertEquals(expectedCombined.count(), combined1.count());
    assertEquals(expectedCombined.sum(), combined1.sum());

    assertEquals(expectedCombined.count(), combined2.count());
    assertEquals(expectedCombined.sum(), combined2.sum());

  }
}
