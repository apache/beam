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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FloatCoder}. */
@RunWith(JUnit4.class)
public class FloatCoderTest {

  private static final Coder<Float> TEST_CODER = FloatCoder.of();

  private static final List<Float> TEST_VALUES =
      Arrays.asList(0.0f - 0.5f, 0.5f, 0.3f, -0.3f, 1.0f, Float.MAX_VALUE, Float.MIN_VALUE);

  @Test
  public void testStructuralValueReturnTheSameValue() {
    Float expected = 23.45F;
    Object actual = TEST_CODER.structuralValue(expected);
    assertEquals(expected, actual);
  }

  @Test
  public void testStructuralValueDecodeEncodeEqual() throws Exception {
    for (Float value : TEST_VALUES) {
      CoderProperties.structuralValueDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testStructuralValueConsistentWithEquals() throws Exception {
    for (Float value1 : TEST_VALUES) {
      for (Float value2 : TEST_VALUES) {
        CoderProperties.structuralValueConsistentWithEquals(
            TEST_CODER, Float.valueOf(value1), Float.valueOf(value2));
      }
    }
  }
}
