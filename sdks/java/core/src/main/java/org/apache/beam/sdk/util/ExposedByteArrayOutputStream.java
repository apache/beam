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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.annotations.Internal;

/**
 * {@link ByteArrayOutputStream} special cased to treat writes of a single byte-array specially.
 * When calling {@link #toByteArray()} after writing only one {@code byte[]} using {@link
 * #writeAndOwn(byte[])}, it will return that array directly.
 */
@Internal
public class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

  private byte[] swappedBuffer;

  /**
   * If true, this stream doesn't allow direct access to the passed in byte-array. It behaves just
   * like a normal {@link ByteArrayOutputStream}.
   *
   * <p>It is set to true after any write operations other than the first call to {@link
   * #writeAndOwn(byte[])}.
   */
  private boolean isFallback = false;

  /** Fall back to the behavior of a normal {@link ByteArrayOutputStream}. */
  private void fallback() {
    isFallback = true;
    if (swappedBuffer != null) {
      // swappedBuffer != null means buf is actually provided by the caller of writeAndOwn(),
      // while swappedBuffer is the original buffer.
      // Recover the buffer and copy the bytes from buf.
      byte[] tempBuffer = buf;
      count = 0;
      buf = swappedBuffer;
      super.write(tempBuffer, 0, tempBuffer.length);
      swappedBuffer = null;
    }
  }

  /**
   * Write {@code b} to the stream and take the ownership of {@code b}. If the stream is empty,
   * {@code b} itself will be used as the content of the stream and no content copy will be
   * involved.
   *
   * <p><i>Note: After passing any byte array to this method, it must not be modified again.</i>
   */
  // Takes ownership of input buffer by design - Spotbugs is right to warn that this is dangerous
  public synchronized void writeAndOwn(byte[] b) throws IOException {
    if (b.length == 0) {
      return;
    }
    if (count == 0) {
      // Optimized first-time whole write.
      // The original buffer will be swapped to swappedBuffer, while the input b is used as buf.
      swappedBuffer = buf;
      buf = b;
      count = b.length;
    } else {
      fallback();
      super.write(b);
    }
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) {
    fallback();
    super.write(b, off, len);
  }

  @Override
  public synchronized void write(int b) {
    fallback();
    super.write(b);
  }

  @Override
  // Exposes internal mutable reference by design - Spotbugs is right to warn that this is dangerous
  public synchronized byte[] toByteArray() {
    // Note: count == buf.length is not a correct criteria to "return buf;", because the internal
    // buf may be reused after reset().
    if (!isFallback && count > 0) {
      return buf;
    } else {
      return super.toByteArray();
    }
  }

  @Override
  public synchronized void reset() {
    if (count == 0) {
      return;
    }
    count = 0;
    if (isFallback) {
      isFallback = false;
    } else {
      buf = swappedBuffer;
      swappedBuffer = null;
    }
  }
}
