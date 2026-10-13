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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ExposedByteArrayInputStream}. */
@RunWith(JUnit4.class)
public class StreamUtilsTest {

  private byte[] testData = null;

  @Before
  public void setUp() {
    testData = new byte[60 * 1024];
    Arrays.fill(testData, (byte) 32);
  }

  @Test
  public void testGetBytesFromExposedByteArrayInputStream() throws IOException {
    InputStream stream = new ExposedByteArrayInputStream(testData);
    byte[] bytes = StreamUtils.getBytesWithoutClosing(stream);
    assertArrayEquals(testData, bytes);
    assertSame(testData, bytes);
    assertEquals(0, stream.available());
  }

  @Test
  public void testGetBytesFromByteArrayInputStream() throws IOException {
    InputStream stream = new ByteArrayInputStream(testData);
    byte[] bytes = StreamUtils.getBytesWithoutClosing(stream);
    assertArrayEquals(testData, bytes);
    assertEquals(0, stream.available());
  }

  @Test
  public void testGetBytesFromInputStream() throws IOException {
    // Any stream which is not a ByteArrayInputStream.
    InputStream stream = new BufferedInputStream(new ByteArrayInputStream(testData));
    byte[] bytes = StreamUtils.getBytesWithoutClosing(stream);
    assertArrayEquals(testData, bytes);
    assertEquals(0, stream.available());
  }

  @Test
  public void testGetBytesFromUnownedInputStreamAroundExposed() throws IOException {
    InputStream stream = new UnownedInputStream(new ExposedByteArrayInputStream(testData));
    byte[] bytes = StreamUtils.getBytesWithoutClosing(stream);
    assertArrayEquals(testData, bytes);
    assertSame(testData, bytes);
    assertEquals(0, stream.available());
  }

  @Test
  public void testGetBytesFromUnownedInputStreamAroundArray() throws IOException {
    InputStream stream = new UnownedInputStream(new ByteArrayInputStream(testData));
    byte[] bytes = StreamUtils.getBytesWithoutClosing(stream);
    assertArrayEquals(testData, bytes);
    assertEquals(0, stream.available());
  }

  @Test
  public void testGetBytesFromLimitedInputStream() throws IOException {
    InputStream stream = ByteStreams.limit(new ByteArrayInputStream(testData), Integer.MAX_VALUE);
    byte[] bytes = StreamUtils.getBytesWithoutClosing(stream);
    assertArrayEquals(testData, bytes);
    assertEquals(0, stream.available());
  }

  @Test
  public void testGetBytesFromEmptyLimitedInputStream() throws IOException {
    InputStream stream = ByteStreams.limit(new ByteArrayInputStream(testData), 0);
    byte[] bytes = StreamUtils.getBytesWithoutClosing(stream);
    assertArrayEquals(new byte[0], bytes);
    assertEquals(0, stream.available());
  }

  @Test
  public void testGetBytesFromRepeatedInputStream() throws IOException {
    byte[] largeBytes = new byte[2 * 1024 * 1024];
    Arrays.fill(largeBytes, (byte) 1);
    InputStream stream = ByteStreams.limit(new ByteArrayInputStream(largeBytes), Integer.MAX_VALUE);
    byte[] bytes = StreamUtils.getBytesWithoutClosing(stream);
    assertArrayEquals(largeBytes, bytes);
    assertEquals(0, stream.available());
  }

  public static class LyingInputStream extends FilterInputStream {
    private final int availableLie;

    public LyingInputStream(InputStream in, int availableLie) {
      super(in);
      this.availableLie = availableLie;
    }

    @Override
    public int available() throws IOException {
      return availableLie;
    }
  }

  @Test
  public void testGetBytesFromHugeAvailable() throws IOException {
    InputStream wrappedStream = new ByteArrayInputStream(testData);
    InputStream stream = new LyingInputStream(wrappedStream, Integer.MAX_VALUE - 1);
    byte[] bytes = StreamUtils.getBytesWithoutClosing(stream);
    assertArrayEquals(testData, bytes);
    assertEquals(0, wrappedStream.available());
  }

  @Test
  public void testGetBytesFromZeroAvailable() throws IOException {
    InputStream wrappedStream = new ByteArrayInputStream(testData);
    InputStream stream = new LyingInputStream(wrappedStream, 0);
    byte[] bytes = StreamUtils.getBytesWithoutClosing(stream);
    assertArrayEquals(testData, bytes);
    assertEquals(0, wrappedStream.available());
  }

  @Test
  public void testGetBytesFromOneExtraAvailable() throws IOException {
    InputStream wrappedStream = new ByteArrayInputStream(testData);
    InputStream stream = new LyingInputStream(wrappedStream, wrappedStream.available() + 1);
    byte[] bytes = StreamUtils.getBytesWithoutClosing(stream);
    assertArrayEquals(testData, bytes);
    assertEquals(0, wrappedStream.available());
  }

  @Test
  public void testGetBytesFromOneLessAvailable() throws IOException {
    InputStream wrappedStream = new ByteArrayInputStream(testData);
    InputStream stream = new LyingInputStream(wrappedStream, wrappedStream.available() - 1);
    byte[] bytes = StreamUtils.getBytesWithoutClosing(stream);
    assertArrayEquals(testData, bytes);
    assertEquals(0, wrappedStream.available());
  }
}
