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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ExposedByteArrayInputStream}. */
@RunWith(JUnit4.class)
public class ExposedByteArrayInputStreamTest {

  private static final byte[] TEST_DATA = "Hello World!".getBytes(StandardCharsets.UTF_8);

  private ByteArrayInputStream stream = new ByteArrayInputStream(TEST_DATA);

  private ExposedByteArrayInputStream exposedStream = new ExposedByteArrayInputStream(TEST_DATA);

  @Test
  public void testConstructWithEmptyArray() throws IOException {
    try (ExposedByteArrayInputStream s = new ExposedByteArrayInputStream(new byte[0])) {
      assertEquals(0, s.available());
      byte[] data = s.readAll();
      assertEquals(0, data.length);
    }
  }

  @Test
  public void testReadAll() throws IOException {
    assertEquals(TEST_DATA.length, exposedStream.available());
    byte[] data = exposedStream.readAll();
    assertArrayEquals(TEST_DATA, data);
    assertSame(TEST_DATA, data);
    assertEquals(0, exposedStream.available());
  }

  @Test
  public void testReadPartial() throws IOException {
    assertEquals(TEST_DATA.length, exposedStream.available());
    assertEquals(TEST_DATA.length, stream.available());
    byte[] data1 = new byte[4];
    byte[] data2 = new byte[4];
    int ret1 = exposedStream.read(data1);
    int ret2 = stream.read(data2);
    assertEquals(ret2, ret1);
    assertArrayEquals(data2, data1);
    assertEquals(stream.available(), exposedStream.available());
  }

  @Test
  public void testReadAllAfterReadPartial() throws IOException {
    assertNotEquals(-1, exposedStream.read());
    byte[] ret = exposedStream.readAll();
    assertArrayEquals("ello World!".getBytes(StandardCharsets.UTF_8), ret);
  }
}
