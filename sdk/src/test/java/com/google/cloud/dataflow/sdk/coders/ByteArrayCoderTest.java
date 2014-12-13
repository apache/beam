/*
 * Copyright (C) 2014 Google Inc.
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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.util.common.CounterTestUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** Unit tests for {@link ByteArrayCoder}. */
@RunWith(JUnit4.class)
public class ByteArrayCoderTest {
  @Test public void testOuterContext() throws CoderException, IOException {
    byte[] buffer = {0xa, 0xb, 0xc};

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ByteArrayCoder.of().encode(buffer, os, Coder.Context.OUTER);
    byte[] encoded = os.toByteArray();

    ByteArrayInputStream is = new ByteArrayInputStream(encoded);
    byte[] decoded = ByteArrayCoder.of().decode(is, Coder.Context.OUTER);
    assertThat(decoded, equalTo(buffer));
  }

  @Test public void testNestedContext() throws CoderException, IOException {
    byte[][] buffers = {{0xa, 0xb, 0xc}, {}, {}, {0xd, 0xe}, {}};

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    for (byte[] buffer : buffers) {
      ByteArrayCoder.of().encode(buffer, os, Coder.Context.NESTED);
    }
    byte[] encoded = os.toByteArray();

    ByteArrayInputStream is = new ByteArrayInputStream(encoded);
    for (byte[] buffer : buffers) {
      byte[] decoded = ByteArrayCoder.of().decode(is, Coder.Context.NESTED);
      assertThat(decoded, equalTo(buffer));
    }
  }

  @Test public void testRegisterByteSizeObserver() throws Exception {
    CounterTestUtils.testByteCount(ByteArrayCoder.of(), Coder.Context.OUTER,
                                   new byte[][]{{ 0xa, 0xb, 0xc }});

    CounterTestUtils.testByteCount(ByteArrayCoder.of(), Coder.Context.NESTED,
                                   new byte[][]{{ 0xa, 0xb, 0xc }, {}, {}, { 0xd, 0xe }, {}});
  }
}
