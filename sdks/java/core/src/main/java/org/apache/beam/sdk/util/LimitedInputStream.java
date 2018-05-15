/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * Decorate an input stream enforcing its max length.
 */
public class LimitedInputStream extends InputStream {
  private final InputStream in;
  private long maxBytes;

  public LimitedInputStream(final int max, final InputStream delegate) {
    this.maxBytes = max;
    this.in = delegate;
  }

  @Override
  public int read() throws IOException {
    if (maxBytes == 0) {
      return -1;
    }
    maxBytes--;
    return in.read();
  }

  @Override
  public int read(final byte[] b) throws IOException {
    if (maxBytes == 0) {
      return -1;
    }
    final int read = in.read(b, 0, maxBytesOrMaxValue());
    maxBytes -= read;
    return read;
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    if (maxBytes == 0) {
      return -1;
    }
    final int read = in.read(b, off, Math.min(maxBytesOrMaxValue(), len));
    maxBytes -= read;
    return read;
  }

  @Override
  public long skip(final long n) throws IOException {
    if (maxBytes == 0) {
      return 0;
    }
    final long skip = in.skip(Math.min(maxBytes, n));
    maxBytes -= skip;
    return skip;
  }

  @Override
  public int available() throws IOException {
    return Math.min(in.available(), maxBytesOrMaxValue());
  }

  @Override
  public void mark(final int readlimit) {
    in.mark(readlimit);
  }

  @Override
  public void reset() throws IOException {
    in.reset();
  }

  @Override
  public boolean markSupported() {
    return in.markSupported();
  }

  private int maxBytesOrMaxValue() {
    return maxBytes > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxBytes;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
