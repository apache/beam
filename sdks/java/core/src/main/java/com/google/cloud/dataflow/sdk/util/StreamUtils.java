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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.SoftReference;

/**
 * Utility functions for stream operations.
 */
public class StreamUtils {

  private StreamUtils() {
  }

  private static final int BUF_SIZE = 8192;

  private static ThreadLocal<SoftReference<byte[]>> threadLocalBuffer = new ThreadLocal<>();

  /**
   * Efficient converting stream to bytes.
   */
  public static byte[] getBytes(InputStream stream) throws IOException {
    if (stream instanceof ExposedByteArrayInputStream) {
      // Fast path for the exposed version.
      return ((ExposedByteArrayInputStream) stream).readAll();
    } else if (stream instanceof ByteArrayInputStream) {
      // Fast path for ByteArrayInputStream.
      byte[] ret = new byte[stream.available()];
      stream.read(ret);
      return ret;
    }
    // Falls back to normal stream copying.
    SoftReference<byte[]> refBuffer = threadLocalBuffer.get();
    byte[] buffer = refBuffer == null ? null : refBuffer.get();
    if (buffer == null) {
      buffer = new byte[BUF_SIZE];
      threadLocalBuffer.set(new SoftReference<byte[]>(buffer));
    }
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    while (true) {
      int r = stream.read(buffer);
      if (r == -1) {
        break;
      }
      outStream.write(buffer, 0, r);
    }
    return outStream.toByteArray();
  }

}
