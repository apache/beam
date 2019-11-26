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
package org.apache.beam.sdk.io.range;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Combinatorial tests for {@link ByteKeyRange#interpolateKey}, which also checks {@link
 * ByteKeyRange#estimateFractionForKey} by converting the interpolated keys back to fractions.
 */
@RunWith(Parameterized.class)
public class ByteKeyRangeInterpolateKeyTest {
  private static final ByteKey[] TEST_KEYS = ByteKeyRangeTest.RANGE_TEST_KEYS;

  @Parameters(name = "{index}: {0}")
  public static Iterable<Object[]> data() {
    ImmutableList.Builder<Object[]> ret = ImmutableList.builder();
    for (int i = 0; i < TEST_KEYS.length; ++i) {
      for (int j = i + 1; j < TEST_KEYS.length; ++j) {
        ret.add(new Object[] {ByteKeyRange.of(TEST_KEYS[i], TEST_KEYS[j])});
      }
    }
    return ret.build();
  }

  @Parameter public ByteKeyRange range;

  @Test
  public void testInterpolateKeyAndEstimateFraction() {
    double delta = 0.0000001;
    double[] testFractions =
        new double[] {0.01, 0.1, 0.123, 0.2, 0.3, 0.45738, 0.5, 0.6, 0.7182, 0.8, 0.95, 0.97, 0.99};
    ByteKey last = range.getStartKey();
    for (double fraction : testFractions) {
      String message = Double.toString(fraction);
      try {
        ByteKey key = range.interpolateKey(fraction);
        assertThat(message, key, greaterThanOrEqualTo(last));
        assertThat(message, range.estimateFractionForKey(key), closeTo(fraction, delta));
        last = key;
      } catch (IllegalStateException e) {
        assertThat(message, e.getMessage(), containsString("near-empty ByteKeyRange"));
      }
    }
  }
}
