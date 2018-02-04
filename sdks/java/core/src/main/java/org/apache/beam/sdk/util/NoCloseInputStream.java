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
 * not the filter version for perf reasons.
 * just skips the close on the delegate instance.
 */
public class NoCloseInputStream extends InputStream {
  private final InputStream delegate;

  public NoCloseInputStream(final InputStream delegate) {
    this.delegate = delegate;
  }

  @Override
  public void close() throws IOException {
    // no-pop
  }

  @Override
  public int read() throws IOException {
    return delegate.read();
  }

  @Override
  public int read(final byte[] b) throws IOException {
    return delegate.read(b);
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    return delegate.read(b, off, len);
  }

  @Override
  public long skip(final long n) throws IOException {
    return delegate.skip(n);
  }

  @Override
  public int available() throws IOException {
    return delegate.available();
  }

  @Override
  public void mark(final int readlimit) {
    delegate.mark(readlimit);
  }

  @Override
  public void reset() throws IOException {
    delegate.reset();
  }

  @Override
  public boolean markSupported() {
    return delegate.markSupported();
  }
}
