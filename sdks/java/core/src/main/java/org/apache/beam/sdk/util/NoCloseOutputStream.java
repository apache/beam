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
import java.io.OutputStream;

/**
 * not the filter version for perf reasons.
 * just skips the close on the delegate stream when it is assumed
 * done at another layer.
 */
public class NoCloseOutputStream extends OutputStream {
  private final OutputStream delegate;

  public NoCloseOutputStream(final OutputStream delegate) {
    this.delegate = delegate;
  }

  @Override
  public void close() throws IOException {
    // no-pop
  }

  @Override
  public void write(final int b) throws IOException {
    delegate.write(b);
  }

  @Override
  public void write(final byte[] b) throws IOException {
    delegate.write(b);
  }

  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
    delegate.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    delegate.flush();
  }
}
