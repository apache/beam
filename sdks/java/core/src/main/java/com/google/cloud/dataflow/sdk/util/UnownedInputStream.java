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

import com.google.common.base.MoreObjects;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link OutputStream} wrapper which protects against the user attempting to modify
 * the underlying stream by closing it or using mark.
 */
public class UnownedInputStream extends FilterInputStream {
  public UnownedInputStream(InputStream delegate) {
    super(delegate);
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Caller does not own the underlying input stream "
        + " and should not call close().");
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof UnownedInputStream
        && ((UnownedInputStream) obj).in.equals(in);
  }

  @Override
  public int hashCode() {
    return in.hashCode();
  }

  @SuppressWarnings("UnsynchronizedOverridesSynchronized")
  @Override
  public void mark(int readlimit) {
    throw new UnsupportedOperationException("Caller does not own the underlying input stream "
        + " and should not call mark().");
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @SuppressWarnings("UnsynchronizedOverridesSynchronized")
  @Override
  public void reset() throws IOException {
    throw new UnsupportedOperationException("Caller does not own the underlying input stream "
        + " and should not call reset().");
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(UnownedInputStream.class).add("in", in).toString();
  }
}

