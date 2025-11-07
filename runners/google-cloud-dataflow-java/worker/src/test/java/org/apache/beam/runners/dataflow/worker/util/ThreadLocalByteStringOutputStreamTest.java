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
package org.apache.beam.runners.dataflow.worker.util;

import static org.junit.Assert.*;

import org.apache.beam.runners.dataflow.worker.util.ThreadLocalByteStringOutputStream.StreamHandle;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.junit.Test;

public class ThreadLocalByteStringOutputStreamTest {

  @Test
  public void simple() {
    try (StreamHandle streamHandle = ThreadLocalByteStringOutputStream.acquire()) {
      ByteStringOutputStream stream = streamHandle.stream();
      stream.write(1);
      stream.write(2);
      stream.write(3);
      assertEquals(ByteString.copyFrom(new byte[] {1, 2, 3}), stream.toByteStringAndReset());
    }
  }

  @Test
  public void nested() {
    try (StreamHandle streamHandle = ThreadLocalByteStringOutputStream.acquire()) {
      ByteStringOutputStream stream = streamHandle.stream();
      stream.write(1);
      try (StreamHandle streamHandle1 = ThreadLocalByteStringOutputStream.acquire()) {
        ByteStringOutputStream stream1 = streamHandle.stream();
        stream1.write(2);
        assertEquals(ByteString.copyFrom(new byte[] {2}), stream1.toByteStringAndReset());
      }
      stream.write(3);
      assertEquals(ByteString.copyFrom(new byte[] {1, 3}), stream.toByteStringAndReset());
    }
  }

  @Test
  public void resetDirtyStream() {
    try (StreamHandle streamHandle = ThreadLocalByteStringOutputStream.acquire()) {
      ByteStringOutputStream stream = streamHandle.stream();
      stream.write(1);
      // Don't read/reset stream
    }

    try (StreamHandle streamHandle = ThreadLocalByteStringOutputStream.acquire()) {
      ByteStringOutputStream stream = streamHandle.stream();
      assertEquals(ByteString.EMPTY, stream.toByteStringAndReset());
    }
  }
}
