/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.util.common.CounterTestUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link ByteArrayCoder}.
 */
@RunWith(JUnit4.class)
public class ByteArrayCoderTest {

  private static final byte[][] TEST_VALUES = {
    {0xa, 0xb, 0xc}, {}, {}, {0xd, 0xe}, {0xd, 0xe}, {}};

  @Test
  public void testDecodeEncodeEquals() throws Exception {
    ByteArrayCoder coder = ByteArrayCoder.of();
    for (byte[] value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(coder, value);
    }
  }

  @Test
  public void testRegisterByteSizeObserver() throws Exception {
    CounterTestUtils.testByteCount(ByteArrayCoder.of(), Coder.Context.OUTER,
                                   new byte[][]{{ 0xa, 0xb, 0xc }});

    CounterTestUtils.testByteCount(ByteArrayCoder.of(), Coder.Context.NESTED,
                                   new byte[][]{{ 0xa, 0xb, 0xc }, {}, {}, { 0xd, 0xe }, {}});
  }

  @Test
  public void testStructuralValueConsistentWithEquals() throws Exception {
    ByteArrayCoder coder = ByteArrayCoder.of();
    // We know that byte array coders are NOT compatible with equals
    // (aka injective w.r.t. Object.equals)
    for (byte[] value1 : TEST_VALUES) {
      for (byte[] value2 : TEST_VALUES) {
        CoderProperties.structuralValueConsistentWithEquals(coder, value1, value2);
      }
    }
  }
}
