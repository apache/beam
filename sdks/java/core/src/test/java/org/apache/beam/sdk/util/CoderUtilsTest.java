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
package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.CoderPropertiesTest.ClosingCoder;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for CoderUtils. */
@RunWith(JUnit4.class)
public class CoderUtilsTest {

  @Rule public transient ExpectedException expectedException = ExpectedException.none();

  static class TestCoder extends AtomicCoder<Integer> {
    public static TestCoder of() {
      return new TestCoder();
    }

    @Override
    public void encode(Integer value, OutputStream outStream) {
      throw new RuntimeException("not expecting to be called");
    }

    @Override
    public Integer decode(InputStream inStream) {
      throw new RuntimeException("not expecting to be called");
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      throw new NonDeterministicException(this, "TestCoder does not actually encode or decode.");
    }
  }

  @Test
  public void testCoderExceptionPropagation() throws Exception {
    @SuppressWarnings("unchecked")
    Coder<String> crashingCoder = mock(Coder.class);
    doThrow(new CoderException("testing exception"))
        .when(crashingCoder)
        .encode(anyString(), any(OutputStream.class), any(Coder.Context.class));

    expectedException.expect(CoderException.class);
    expectedException.expectMessage("testing exception");

    CoderUtils.encodeToByteArray(crashingCoder, "hello");
  }

  @Test
  public void testClosingCoderFailsWhenDecodingBase64() throws Exception {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Caller does not own the underlying");
    CoderUtils.decodeFromBase64(new ClosingCoder(), "test-value");
  }

  @Test
  public void testClosingCoderFailsWhenDecodingByteArray() throws Exception {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Caller does not own the underlying");
    CoderUtils.decodeFromByteArray(new ClosingCoder(), new byte[0]);
  }

  @Test
  public void testClosingCoderFailsWhenDecodingByteArrayInContext() throws Exception {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Caller does not own the underlying");
    CoderUtils.decodeFromByteArray(new ClosingCoder(), new byte[0], Context.NESTED);
  }

  @Test
  public void testClosingCoderFailsWhenEncodingToBase64() throws Exception {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Caller does not own the underlying");
    CoderUtils.encodeToBase64(new ClosingCoder(), "test-value");
  }

  @Test
  public void testClosingCoderFailsWhenEncodingToByteArray() throws Exception {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Caller does not own the underlying");
    CoderUtils.encodeToByteArray(new ClosingCoder(), "test-value");
  }

  @Test
  public void testClosingCoderFailsWhenEncodingToByteArrayInContext() throws Exception {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Caller does not own the underlying");
    CoderUtils.encodeToByteArray(new ClosingCoder(), "test-value", Context.NESTED);
  }

  @Test
  public void testDecodeFromByteString() throws Exception {
    String expected = "test string";
    byte[] data = CoderUtils.encodeToByteArray(StringUtf8Coder.of(), expected);
    ByteString byteString = ByteString.copyFrom(data);
    String result = CoderUtils.decodeFromByteString(StringUtf8Coder.of(), byteString);
    assertEquals(expected, result);
  }

  @Test
  public void testDecodeFromByteStringWithExtraDataThrows() throws Exception {
    String expected = "test string";
    byte[] data = CoderUtils.encodeToByteArray(StringUtf8Coder.of(), expected, Context.NESTED);
    ByteString byteString = ByteString.copyFrom(data).concat(ByteString.copyFromUtf8("more text"));

    assertThrows(
        "9 unexpected extra bytes after decoding test string",
        CoderException.class,
        () -> CoderUtils.decodeFromByteString(StringUtf8Coder.of(), byteString, Context.NESTED));
  }
}
