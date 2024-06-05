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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.UnsafeByteOperations;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ByteStringOutputStreamTest {

  @Test
  public void testInvalidInitialCapacity() throws Exception {
    assertThrows(
        "Initial capacity < 0",
        IllegalArgumentException.class,
        () -> new ByteStringOutputStream(-1));
  }

  @Test
  public void testWriteBytes() throws Exception {
    ByteStringOutputStream out = new ByteStringOutputStream();
    assertEquals(0, out.size());
    for (int numElements = 0; numElements < 1024 * 1024; numElements = next(numElements)) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dataOut = new DataOutputStream(baos);
      try {
        for (int i = 0; i < numElements; ++i) {
          dataOut.writeInt(i);
        }
        dataOut.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      dataOut.close();
      byte[] testBuffer = baos.toByteArray();

      for (int pos = 0; pos < testBuffer.length; ) {
        if (testBuffer[pos] == 0) {
          out.write(testBuffer[pos]);
          pos += 1;
        } else {
          int len = Math.min(testBuffer.length - pos, Math.abs(testBuffer[pos]));
          out.write(testBuffer, pos, len);
          pos += len;
        }
        assertEquals(pos, out.size());
      }
      assertEquals(UnsafeByteOperations.unsafeWrap(testBuffer), out.toByteString());
      assertEquals(UnsafeByteOperations.unsafeWrap(testBuffer), out.toByteStringAndReset());
    }
  }

  @Test
  public void testWriteBytesConsumePrefix() throws Exception {
    ByteStringOutputStream out = new ByteStringOutputStream();
    assertEquals(0, out.size());
    for (int numElements = 140; numElements < 1024 * 1024; numElements = next(numElements)) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dataOut = new DataOutputStream(baos);
      try {
        for (int i = 0; i < numElements; ++i) {
          dataOut.writeInt(i);
        }
        dataOut.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      dataOut.close();
      byte[] testBuffer = baos.toByteArray();

      for (int pos = 0; pos < testBuffer.length; ) {
        if (testBuffer[pos] == 0) {
          out.write(testBuffer[pos]);
          pos += 1;
        } else {
          int len = Math.min(testBuffer.length - pos, Math.abs(testBuffer[pos]));
          out.write(testBuffer, pos, len);
          pos += len;
        }
        assertEquals(pos, out.size());
      }

      assertEquals(UnsafeByteOperations.unsafeWrap(testBuffer), out.toByteString());
      assertEquals(
          UnsafeByteOperations.unsafeWrap(testBuffer, 0, 2), out.consumePrefixToByteString(2));
      assertEquals(testBuffer.length - 2, out.size());
      assertEquals(
          UnsafeByteOperations.unsafeWrap(testBuffer, 2, 100), out.consumePrefixToByteString(100));
      assertEquals(testBuffer.length - 102, out.size());
      assertEquals(
          UnsafeByteOperations.unsafeWrap(testBuffer, 102, 0), out.consumePrefixToByteString(0));
      assertEquals(testBuffer.length - 102, out.size());
      assertEquals(
          UnsafeByteOperations.unsafeWrap(testBuffer, 102, testBuffer.length - 112),
          out.consumePrefixToByteString(out.size() - 10));
      assertEquals(10, out.size());
      assertEquals(
          UnsafeByteOperations.unsafeWrap(testBuffer, testBuffer.length - 10, 10),
          out.consumePrefixToByteString(10));
      assertEquals(0, out.size());
    }
  }

  @Test
  public void testWriteBytesWithZeroInitialCapacity() throws Exception {
    for (int numElements = 0; numElements < 1024 * 1024; numElements = next(numElements)) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dataOut = new DataOutputStream(baos);
      try {
        for (int i = 0; i < numElements; ++i) {
          dataOut.writeInt(i);
        }
        dataOut.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      dataOut.close();
      byte[] testBuffer = baos.toByteArray();

      ByteStringOutputStream out = new ByteStringOutputStream(0);
      assertEquals(0, out.size());

      for (int pos = 0; pos < testBuffer.length; ) {
        if (testBuffer[pos] == 0) {
          out.write(testBuffer[pos]);
          pos += 1;
        } else {
          int len = Math.min(testBuffer.length - pos, Math.abs(testBuffer[pos]));
          out.write(testBuffer, pos, len);
          pos += len;
        }
        assertEquals(pos, out.size());
      }
      assertEquals(UnsafeByteOperations.unsafeWrap(testBuffer), out.toByteString());
      assertEquals(UnsafeByteOperations.unsafeWrap(testBuffer), out.toByteStringAndReset());
    }
  }

  // Grow the elements based upon an approximation of the fibonacci sequence.
  private static int next(int current) {
    double a = Math.max(1, current * (1 + Math.sqrt(5)) / 2.0);
    return (int) Math.round(a);
  }
}
