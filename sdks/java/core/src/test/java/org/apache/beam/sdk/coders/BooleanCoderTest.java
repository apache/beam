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

import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BooleanCoder}. */
@RunWith(JUnit4.class)
public class BooleanCoderTest {
  private static final Coder<Boolean> TEST_CODER = BooleanCoder.of();

  private static final ImmutableList<Boolean> TEST_VALUES =
      ImmutableList.of(Boolean.TRUE, Boolean.FALSE);

  @Test
  public void testStructuralValueReturnTheSameValue() {
    Boolean expected = Boolean.TRUE;
    Object actual = TEST_CODER.structuralValue(expected);
    assertEquals(expected, actual);
  }

  @Test
  public void testStructuralValueDecodeEncodeEqual() throws Exception {
    for (Boolean value : TEST_VALUES) {
      CoderProperties.structuralValueDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testStructuralValueConsistentWithEquals() throws Exception {
    for (Boolean value1 : TEST_VALUES) {
      for (Boolean value2 : TEST_VALUES) {
        CoderProperties.structuralValueConsistentWithEquals(TEST_CODER, value1, value2);
      }
    }
  }
}
