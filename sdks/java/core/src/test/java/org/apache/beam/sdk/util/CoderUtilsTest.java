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

import static org.apache.beam.sdk.util.CoderUtils.makeCloudEncoding;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.testing.CoderPropertiesTest.ClosingCoder;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for CoderUtils.
 */
@RunWith(JUnit4.class)
public class CoderUtilsTest {

  @Rule
  public transient ExpectedException expectedException = ExpectedException.none();

  static class TestCoder extends CustomCoder<Integer> {
    public static TestCoder of() {
      return new TestCoder();
    }

    @Override
    public void encode(Integer value, OutputStream outStream, Context context) {
      throw new RuntimeException("not expecting to be called");
    }

    @Override
    public Integer decode(InputStream inStream, Context context) {
      throw new RuntimeException("not expecting to be called");
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      throw new NonDeterministicException(this,
        "TestCoder does not actually encode or decode.");
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
  public void testCreateAtomicCoders() throws Exception {
    Assert.assertEquals(
        BigEndianIntegerCoder.of(),
        Serializer.deserialize(makeCloudEncoding("BigEndianIntegerCoder"), Coder.class));
    Assert.assertEquals(
        StringUtf8Coder.of(),
        Serializer.deserialize(
            makeCloudEncoding(StringUtf8Coder.class.getName()), Coder.class));
    Assert.assertEquals(
        VoidCoder.of(),
        Serializer.deserialize(makeCloudEncoding("VoidCoder"), Coder.class));
    Assert.assertEquals(
        TestCoder.of(),
        Serializer.deserialize(makeCloudEncoding(TestCoder.class.getName()), Coder.class));
  }

  @Test
  public void testCreateCompositeCoders() throws Exception {
    Assert.assertEquals(
        IterableCoder.of(StringUtf8Coder.of()),
        Serializer.deserialize(
            makeCloudEncoding("IterableCoder",
                makeCloudEncoding("StringUtf8Coder")), Coder.class));
    Assert.assertEquals(
        KvCoder.of(BigEndianIntegerCoder.of(), VoidCoder.of()),
        Serializer.deserialize(
            makeCloudEncoding(
                "KvCoder",
                makeCloudEncoding(BigEndianIntegerCoder.class.getName()),
                makeCloudEncoding("VoidCoder")), Coder.class));
    Assert.assertEquals(
        IterableCoder.of(
            KvCoder.of(IterableCoder.of(BigEndianIntegerCoder.of()),
                       KvCoder.of(VoidCoder.of(),
                                  TestCoder.of()))),
        Serializer.deserialize(
            makeCloudEncoding(
                IterableCoder.class.getName(),
                makeCloudEncoding(
                    KvCoder.class.getName(),
                    makeCloudEncoding(
                        "IterableCoder",
                        makeCloudEncoding("BigEndianIntegerCoder")),
                    makeCloudEncoding(
                        "KvCoder",
                        makeCloudEncoding("VoidCoder"),
                        makeCloudEncoding(TestCoder.class.getName())))), Coder.class));
  }

  @Test
  public void testCreateUntypedCoders() throws Exception {
    Assert.assertEquals(
        IterableCoder.of(StringUtf8Coder.of()),
        Serializer.deserialize(
            makeCloudEncoding(
                "kind:stream",
                makeCloudEncoding("StringUtf8Coder")), Coder.class));
    Assert.assertEquals(
        KvCoder.of(BigEndianIntegerCoder.of(), VoidCoder.of()),
        Serializer.deserialize(
            makeCloudEncoding(
                "kind:pair",
                makeCloudEncoding(BigEndianIntegerCoder.class.getName()),
                makeCloudEncoding("VoidCoder")), Coder.class));
    Assert.assertEquals(
        IterableCoder.of(
            KvCoder.of(IterableCoder.of(BigEndianIntegerCoder.of()),
                       KvCoder.of(VoidCoder.of(),
                                  TestCoder.of()))),
        Serializer.deserialize(
            makeCloudEncoding(
                "kind:stream",
                makeCloudEncoding(
                    "kind:pair",
                    makeCloudEncoding(
                        "kind:stream",
                        makeCloudEncoding("BigEndianIntegerCoder")),
                    makeCloudEncoding(
                        "kind:pair",
                        makeCloudEncoding("VoidCoder"),
                        makeCloudEncoding(TestCoder.class.getName())))), Coder.class));
  }

  @Test
  public void testCreateUnknownCoder() throws Exception {
    try {
      Serializer.deserialize(makeCloudEncoding("UnknownCoder"), Coder.class);
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(),
                        CoreMatchers.containsString(
                            "Unable to convert coder ID UnknownCoder to class"));
    }
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
}
