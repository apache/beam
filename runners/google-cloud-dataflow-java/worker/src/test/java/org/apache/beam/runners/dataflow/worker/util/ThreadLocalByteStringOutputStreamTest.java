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

import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

public class ThreadLocalByteStringOutputStreamTest {

  @Test
  public void simple() {
    ByteString bytes =
        ThreadLocalByteStringOutputStream.withThreadLocalStream(
            stream -> {
              stream.write(1);
              stream.write(2);
              stream.write(3);
              return stream.toByteStringAndReset();
            });
    assertEquals(ByteString.copyFrom(new byte[] {1, 2, 3}), bytes);
  }

  @Test
  public void nested() {
    ByteString bytes =
        ThreadLocalByteStringOutputStream.withThreadLocalStream(
            stream -> {
              stream.write(1);
              ByteString nestedBytes =
                  ThreadLocalByteStringOutputStream.withThreadLocalStream(
                      nestedStream -> {
                        nestedStream.write(2);
                        return nestedStream.toByteStringAndReset();
                      });
              assertEquals(ByteString.copyFrom(new byte[] {2}), nestedBytes);
              stream.write(3);
              return stream.toByteStringAndReset();
            });
    assertEquals(ByteString.copyFrom(new byte[] {1, 3}), bytes);
  }

  @Test
  public void resetDirtyStream() {
    @Nullable
    Object unused =
        ThreadLocalByteStringOutputStream.withThreadLocalStream(
            stream -> {
              stream.write(1);
              // Don't read/reset stream
              return null;
            });
    ByteString bytes =
        ThreadLocalByteStringOutputStream.withThreadLocalStream(
            stream -> {
              return stream.toByteStringAndReset();
            });
    assertEquals(ByteString.EMPTY, bytes);
  }
}
