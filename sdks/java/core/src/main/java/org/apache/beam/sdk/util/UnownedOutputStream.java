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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link OutputStream} wrapper which protects against the user attempting to modify the
 * underlying stream by closing it.
 */
@Internal
public class UnownedOutputStream extends FilterOutputStream {
  public UnownedOutputStream(OutputStream delegate) {
    super(delegate);
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException(
        "Caller does not own the underlying output stream " + " and should not call close().");
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    return obj instanceof UnownedOutputStream && ((UnownedOutputStream) obj).out.equals(out);
  }

  @Override
  public int hashCode() {
    return out.hashCode();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(UnownedOutputStream.class).add("out", out).toString();
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
  }
}
