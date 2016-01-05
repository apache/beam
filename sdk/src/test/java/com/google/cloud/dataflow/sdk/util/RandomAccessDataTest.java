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
package com.google.cloud.dataflow.sdk.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.util.RandomAccessData.RandomAccessDataCoder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Tests for {@link RandomAccessData}.
 */
@RunWith(JUnit4.class)
public class RandomAccessDataTest {
  private static final byte[] TEST_DATA_A = new byte[]{ 0x01, 0x02, 0x03 };
  private static final byte[] TEST_DATA_B = new byte[]{ 0x06, 0x05, 0x04, 0x03 };

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
    assertTrue(RandomAccessDataCoder.of().isRegisterByteSizeObserverCheap(streamA, Context.NESTED));
    assertTrue(RandomAccessDataCoder.of().isRegisterByteSizeObserverCheap(streamA, Context.OUTER));
    assertEquals(4, RandomAccessDataCoder.of().getEncodedElementByteSize(streamA, Context.NESTED));
    assertEquals(3, RandomAccessDataCoder.of().getEncodedElementByteSize(streamA, Context.OUTER));
  }

  @Test
  public void testLexicographicalComparator() throws Exception {
    RandomAccessData streamA = new RandomAccessData();
    streamA.asOutputStream().write(TEST_DATA_A);
    RandomAccessData streamB = new RandomAccessData();
    streamB.asOutputStream().write(TEST_DATA_B);
    assertTrue(RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(streamA, streamB) < 0);
    assertTrue(RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(streamB, streamA) > 0);
    assertTrue(RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(streamB, streamB) == 0);
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
    streamB.asOutputStream().write(new byte[]{ 0x01, 0x02, 0x04 });
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
    assertArrayEquals(new byte[]{ 0x01, 0x01, 0x02, 0x03 },
        Arrays.copyOf(stream.array(), stream.size()));
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
    assertArrayEquals(new byte[]{ 0x00, 0x00, 0x00, 0x01, 0x02 },
        Arrays.copyOf(stream.array(), stream.size()));
    bais.close();
  }

  @Test
  public void testWriteTo() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    RandomAccessData stream = new RandomAccessData();
    stream.asOutputStream().write(TEST_DATA_B);
    stream.writeTo(baos, 1, 2);
    assertArrayEquals(new byte[]{ 0x05, 0x04 }, baos.toByteArray());
    baos.close();
  }

  @Test
  public void testThatRandomAccessDataGrowsWhenResettingToPositionBeyondEnd() throws Exception {
    RandomAccessData stream = new RandomAccessData(0);
    assertArrayEquals(new byte[0], stream.array());
    stream.resetTo(3);  // force resize
    assertArrayEquals(new byte[]{ 0x00, 0x00, 0x00 }, stream.array());
  }

  @Test
  public void testThatRandomAccessDataGrowsWhenReading() throws Exception {
    RandomAccessData stream = new RandomAccessData(0);
    assertArrayEquals(new byte[0], stream.array());
    stream.readFrom(new ByteArrayInputStream(TEST_DATA_A), 0, TEST_DATA_A.length);
    assertArrayEquals(TEST_DATA_A,
        Arrays.copyOf(stream.array(), TEST_DATA_A.length));
  }

}

