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
import static org.junit.Assert.assertNotEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link ZstdCoder}. */
@RunWith(JUnit4.class)
public class ZstdCoderTest {
  private static final ZstdCoder<String> TEST_CODER = ZstdCoder.of(StringUtf8Coder.of());
  private static final ZstdCoder<byte[]> BYTE_ARRAY_TEST_CODER = ZstdCoder.of(ByteArrayCoder.of());

  private static final List<String> TEST_VALUES =
      Arrays.asList(
          "",
          "a",
          "aaabbbccc",
          "Hello world!",
          "I am Groot. I am Groot. I am Groot.",
          "{\"foo\":32417897,\"bar\":true}",
          "<html><head></head><body></body></html>");

  @Test
  public void testDecodeEncodeEquals() throws Exception {
    for (String value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testEncodingNotBuffered() throws Exception {
    // This test ensures that the coder does not buffer any data from the inner stream.
    // This is not of much importance today, since the coder relies on direct compression and uses
    // ByteArrayCoder to encode the resulting byte[], but this may change if the coder switches to
    // stream based compression in which case the stream must not buffer input from the inner
    // stream.
    for (String value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(
          KvCoder.of(TEST_CODER, StringUtf8Coder.of()), KV.of(value, value));
    }
  }

  @Test
  public void testCoderSerializable() throws Exception {
    CoderProperties.coderSerializable(TEST_CODER);
  }

  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() throws Exception {
    CoderProperties.coderSerializable(ZstdCoder.of(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testConsistentWithEquals() throws Exception {
    assertEquals(StringUtf8Coder.of().consistentWithEquals(), TEST_CODER.consistentWithEquals());
    assertEquals(
        ByteArrayCoder.of().consistentWithEquals(), BYTE_ARRAY_TEST_CODER.consistentWithEquals());
  }

  @Test
  public void testStructuralValue() throws Exception {
    // This might change if compression proves beneficial for encoded structural values.
    for (String value : TEST_VALUES) {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      assertEquals(StringUtf8Coder.of().structuralValue(value), TEST_CODER.structuralValue(value));
      assertEquals(
          ByteArrayCoder.of().structuralValue(bytes), BYTE_ARRAY_TEST_CODER.structuralValue(bytes));
    }
  }

  @Test
  public void testStructuralValueConsistentWithEquals() throws Exception {
    for (String value1 : TEST_VALUES) {
      for (String value2 : TEST_VALUES) {
        byte[] bytes1 = value1.getBytes(StandardCharsets.UTF_8);
        byte[] bytes2 = value2.getBytes(StandardCharsets.UTF_8);
        CoderProperties.structuralValueConsistentWithEquals(TEST_CODER, value1, value2);
        CoderProperties.structuralValueConsistentWithEquals(BYTE_ARRAY_TEST_CODER, bytes1, bytes2);
      }
    }
  }

  @Test
  public void testCoderEquals() throws Exception {
    // True if coder, dict and level are equal.
    assertEquals(
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), null, 0),
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), null, 0));
    assertEquals(
        ZstdCoder.of(ListCoder.of(ByteArrayCoder.of()), new byte[0], 1),
        ZstdCoder.of(ListCoder.of(ByteArrayCoder.of()), new byte[0], 1));
    // False if coder, dict or level differs.
    assertNotEquals(
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), null, 0),
        ZstdCoder.of(ListCoder.of(ByteArrayCoder.of()), null, 0));
    assertNotEquals(
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), null, 0),
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), new byte[0], 0));
    assertNotEquals(
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), null, 0),
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), null, 1));
    assertNotEquals(
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), null, 0),
        ZstdCoder.of(ListCoder.of(ByteArrayCoder.of()), new byte[0], 1));
  }

  @Test
  public void testCoderHashCode() throws Exception {
    // Equal if coder, dict and level are equal.
    assertEquals(
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), null, 0).hashCode(),
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), null, 0).hashCode());
    assertEquals(
        ZstdCoder.of(ListCoder.of(ByteArrayCoder.of()), new byte[0], 1).hashCode(),
        ZstdCoder.of(ListCoder.of(ByteArrayCoder.of()), new byte[0], 1).hashCode());
    // Not equal if coder, dict or level differs.
    assertNotEquals(
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), null, 0).hashCode(),
        ZstdCoder.of(ListCoder.of(ByteArrayCoder.of()), null, 0).hashCode());
    assertNotEquals(
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), null, 0).hashCode(),
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), new byte[0], 0).hashCode());
    assertNotEquals(
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), null, 0).hashCode(),
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), null, 1).hashCode());
    assertNotEquals(
        ZstdCoder.of(ListCoder.of(StringUtf8Coder.of()), null, 0).hashCode(),
        ZstdCoder.of(ListCoder.of(ByteArrayCoder.of()), new byte[0], 1).hashCode());
  }

  @Test
  public void testToString() throws Exception {
    assertEquals(
        "ZstdCoder{innerCoder=StringUtf8Coder, dict=null, level=0}",
        ZstdCoder.of(StringUtf8Coder.of(), null, 0).toString());
    assertEquals(
        "ZstdCoder{innerCoder=ByteArrayCoder, dict=base64:, level=1}",
        ZstdCoder.of(ByteArrayCoder.of(), new byte[0], 1).toString());
    assertEquals(
        "ZstdCoder{innerCoder=TextualIntegerCoder, dict=base64:AA==, level=2}",
        ZstdCoder.of(TextualIntegerCoder.of(), new byte[1], 2).toString());
  }
}
