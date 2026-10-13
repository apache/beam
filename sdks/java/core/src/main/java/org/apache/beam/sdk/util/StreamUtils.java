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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.SoftReference;
import java.util.Arrays;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;

/** Utility functions for stream operations. */
@Internal
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class StreamUtils {

  private StreamUtils() {}

  private static final int BUF_SIZE = 8192;

  private static final ThreadLocal<SoftReference<byte[]>> threadLocalBuffer = new ThreadLocal<>();

  /** Efficient converting stream to bytes. */
  public static byte[] getBytesWithoutClosing(InputStream stream) throws IOException {
    // Unwrap the stream so the below optimizations based upon class type function properly.
    // We don't use mark or reset in this function.
    while (stream instanceof UnownedInputStream) {
      stream = ((UnownedInputStream) stream).getWrappedStream();
    }

    if (stream instanceof ExposedByteArrayInputStream) {
      // Fast path for the exposed version.
      return ((ExposedByteArrayInputStream) stream).readAll();
    }
    if (stream instanceof ByteArrayInputStream) {
      // Fast path for ByteArrayInputStream.
      byte[] ret = new byte[stream.available()];
      stream.read(ret);
      return ret;
    }

    // Most inputs are fully available so we attempt to first read directly
    // into a buffer of the right size, assuming available reflects all the bytes.
    int available = stream.available();
    @Nullable ByteArrayOutputStream outputStream = null;
    if (available > 0 && available < 1024 * 1024) {
      byte[] initialBuffer = new byte[available];
      int initialReadSize = stream.read(initialBuffer);
      if (initialReadSize == -1) {
        return new byte[0];
      }
      int nextByte = stream.read();
      if (nextByte == -1) {
        if (initialReadSize == available) {
          // Available reflected the full buffer and we copied directly to the
          // right size.
          return initialBuffer;
        }
        return Arrays.copyOf(initialBuffer, initialReadSize);
      }
      outputStream = new ByteArrayOutputStream();
      outputStream.write(initialBuffer, 0, initialReadSize);
      outputStream.write(nextByte);
    } else {
      outputStream = new ByteArrayOutputStream();
    }

    // Normal stream copying using the thread-local buffer.
    SoftReference<byte[]> refBuffer = threadLocalBuffer.get();
    byte[] buffer = refBuffer == null ? null : refBuffer.get();
    if (buffer == null) {
      buffer = new byte[BUF_SIZE];
      threadLocalBuffer.set(new SoftReference<>(buffer));
    }
    while (true) {
      int r = stream.read(buffer);
      if (r == -1) {
        break;
      }
      outputStream.write(buffer, 0, r);
    }
    return outputStream.toByteArray();
  }
}
