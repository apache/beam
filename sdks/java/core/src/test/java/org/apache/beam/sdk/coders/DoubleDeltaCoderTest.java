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
package org.apache.beam.sdk.coders;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DoubleDeltaCoderTest {
  private static final Coder<List<Double>> TEST_CODER = DoubleDeltaCoder.of();

  private static final List<Double> TEST_VALUES =
      Arrays.asList(
          0.0,
          -0.5,
          0.5,
          0.3,
          -0.3,
          1.0,
          -43.89568740,
          Math.PI,
          Double.MAX_VALUE,
          Double.MIN_VALUE,
          Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY,
          Double.NaN);

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    CoderProperties.coderDecodeEncodeEqual(TEST_CODER, TEST_VALUES);
  }
}
