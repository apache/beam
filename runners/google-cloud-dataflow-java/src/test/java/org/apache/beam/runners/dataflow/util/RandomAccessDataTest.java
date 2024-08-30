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
package org.apache.beam.runners.dataflow.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.beam.runners.dataflow.util.RandomAccessData.RandomAccessDataCoder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedBytes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RandomAccessData}. */
@RunWith(JUnit4.class)
public class RandomAccessDataTest {
  private static final byte[] TEST_DATA_A = new byte[] {0x01, 0x02, 0x03};
  private static final byte[] TEST_DATA_B = new byte[] {0x06, 0x05, 0x04, 0x03};
  private static final byte[] TEST_DATA_C = new byte[] {0x06, 0x05, 0x03, 0x03};

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testCoder() throws Exception {
    RandomAccessData streamA = new RandomAccessData();
    streamA.asOutputStream().write(TEST_DATA_A);
    RandomAccessData streamB = new RandomAccessData();
    streamB.asOutputStream().write(TEST_DATA_A);
    CoderProperties.coderDecodeEncodeEqual(RandomAccessDataCoder.of(), streamA);
    CoderProperties.coderDeterministic(RandomAccessDataCoder.of(), streamA, streamB);
    CoderProperties.coderConsistentWithEquals(RandomAccessDataCoder.of(), streamA, streamB);
    CoderProperties.coderSerializable(RandomAccessDataCoder.of());
    CoderProperties.structuralValueConsistentWithEquals(
        RandomAccessDataCoder.of(), streamA, streamB);
    assertTrue(RandomAccessDataCoder.of().isRegisterByteSizeObserverCheap(streamA));
    assertEquals(4, RandomAccessDataCoder.of().getEncodedElementByteSize(streamA));
  }

  @Test
  public void testCoderWithPositiveInfinityIsError() throws Exception {
    expectedException.expect(CoderException.class);
    expectedException.expectMessage("Positive infinity can not be encoded");
    RandomAccessDataCoder.of()
        .encode(RandomAccessData.POSITIVE_INFINITY, new ByteArrayOutputStream(), Context.OUTER);
  }

  @Test
  public void testLexicographicalComparator() throws Exception {
    RandomAccessData streamA = new RandomAccessData();
    streamA.asOutputStream().write(TEST_DATA_A);
    RandomAccessData streamB = new RandomAccessData();
    streamB.asOutputStream().write(TEST_DATA_B);
    RandomAccessData streamC = new RandomAccessData();
    streamC.asOutputStream().write(TEST_DATA_C);
    assertTrue(RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(streamA, streamB) < 0);
    assertTrue(RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(streamB, streamA) > 0);
    assertTrue(RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(streamB, streamB) == 0);
    // Check common prefix length.
    assertEquals(
        2,
        RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.commonPrefixLength(streamB, streamC));
    // Check that we honor the start offset.
    assertTrue(
        RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(streamB, streamC, 3) == 0);
    // Test positive infinity comparisons.
    assertTrue(
        RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(
                streamA, RandomAccessData.POSITIVE_INFINITY)
            < 0);
    assertTrue(
        RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(
                RandomAccessData.POSITIVE_INFINITY, RandomAccessData.POSITIVE_INFINITY)
            == 0);
    assertTrue(
        RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(
                RandomAccessData.POSITIVE_INFINITY, streamA)
            > 0);
  }

  @Test
  public void testEqualsAndHashCode() throws Exception {
    // Test that equality by reference works
    RandomAccessData streamA = new RandomAccessData();
    streamA.asOutputStream().write(TEST_DATA_A);
    assertEquals(streamA, streamA);
    assertEquals(streamA.hashCode(), streamA.hashCode());

    // Test different objects containing the same data are the same
    RandomAccessData streamACopy = new RandomAccessData();
    streamACopy.asOutputStream().write(TEST_DATA_A);
    assertEquals(streamA, streamACopy);
    assertEquals(streamA.hashCode(), streamACopy.hashCode());

    // Test same length streams with different data differ
    RandomAccessData streamB = new RandomAccessData();
    streamB.asOutputStream().write(new byte[] {0x01, 0x02, 0x04});
    assertNotEquals(streamA, streamB);
    assertNotEquals(streamA.hashCode(), streamB.hashCode());

    // Test different length streams differ
    streamB.asOutputStream().write(TEST_DATA_B);
    assertNotEquals(streamA, streamB);
    assertNotEquals(streamA.hashCode(), streamB.hashCode());
  }

  @Test
  public void testResetTo() throws Exception {
    RandomAccessData stream = new RandomAccessData();
    stream.asOutputStream().write(TEST_DATA_A);
    stream.resetTo(1);
    assertEquals(1, stream.size());
    stream.asOutputStream().write(TEST_DATA_A);
    assertArrayEquals(
        new byte[] {0x01, 0x01, 0x02, 0x03}, Arrays.copyOf(stream.array(), stream.size()));
  }

  @Test
  public void testAsInputStream() throws Exception {
    RandomAccessData stream = new RandomAccessData();
    stream.asOutputStream().write(TEST_DATA_A);
    InputStream in = stream.asInputStream(1, 1);
    assertEquals(0x02, in.read());
    assertEquals(-1, in.read());
    in.close();
  }

  @Test
  public void testReadFrom() throws Exception {
    ByteArrayInputStream bais = new ByteArrayInputStream(TEST_DATA_A);
    RandomAccessData stream = new RandomAccessData();
    stream.readFrom(bais, 3, 2);
    assertArrayEquals(
        new byte[] {0x00, 0x00, 0x00, 0x01, 0x02}, Arrays.copyOf(stream.array(), stream.size()));
    bais.close();
  }

  @Test
  public void testWriteTo() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RandomAccessData stream = new RandomAccessData();
    stream.asOutputStream().write(TEST_DATA_B);
    stream.writeTo(baos, 1, 2);
    assertArrayEquals(new byte[] {0x05, 0x04}, baos.toByteArray());
    baos.close();
  }

  @Test
  public void testThatRandomAccessDataGrowsWhenResettingToPositionBeyondEnd() throws Exception {
    RandomAccessData stream = new RandomAccessData(0);
    assertArrayEquals(new byte[0], stream.array());
    stream.resetTo(3); // force resize
    assertArrayEquals(new byte[] {0x00, 0x00, 0x00}, stream.array());
  }

  @Test
  public void testThatRandomAccessDataGrowsWhenReading() throws Exception {
    RandomAccessData stream = new RandomAccessData(0);
    assertArrayEquals(new byte[0], stream.array());
    stream.readFrom(new ByteArrayInputStream(TEST_DATA_A), 0, TEST_DATA_A.length);
    assertArrayEquals(TEST_DATA_A, Arrays.copyOf(stream.array(), TEST_DATA_A.length));
  }

  @Test
  public void testIncrement() throws Exception {
    assertEquals(
        new RandomAccessData(new byte[] {0x00, 0x01}),
        new RandomAccessData(new byte[] {0x00, 0x00}).increment());
    assertEquals(
        new RandomAccessData(new byte[] {0x01, UnsignedBytes.MAX_VALUE}),
        new RandomAccessData(new byte[] {0x00, UnsignedBytes.MAX_VALUE}).increment());

    // Test for positive infinity
    assertSame(RandomAccessData.POSITIVE_INFINITY, new RandomAccessData(new byte[0]).increment());
    assertSame(
        RandomAccessData.POSITIVE_INFINITY,
        new RandomAccessData(new byte[] {UnsignedBytes.MAX_VALUE}).increment());
    assertSame(RandomAccessData.POSITIVE_INFINITY, RandomAccessData.POSITIVE_INFINITY.increment());
  }
}
