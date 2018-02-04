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
package org.apache.beam.sdk.coders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.util.Delegating;
import org.apache.beam.sdk.util.NoCloseInputStream;
import org.apache.beam.sdk.util.NoCloseOutputStream;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Skips close() invocations on input/output streams.
 *
 * @param <T> the supported type of the instance which can be encoded/decoded.
 */
public class SkipCloseCoder<T> extends Coder<T> implements Delegating<Coder<T>> {
  private Coder<T> delegate;

  protected SkipCloseCoder() {
    // no-op
  }

  public SkipCloseCoder(final Coder<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void encode(final T value, final OutputStream outStream) throws IOException {
    delegate.encode(value, new NoCloseOutputStream(outStream));
  }

  @Override
  public T decode(final InputStream inStream) throws IOException {
    return delegate.decode(new NoCloseInputStream(inStream));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return delegate.getCoderArguments();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    delegate.verifyDeterministic();
  }

  @Override
  public TypeDescriptor<T> getEncodedTypeDescriptor() {
    return delegate.getEncodedTypeDescriptor();
  }

  @Override
  public boolean equals(final Object o) {
    return this == o
      || o != null && getClass() == o.getClass()
      && delegate.equals(SkipCloseCoder.class.cast(o).delegate);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public Coder<T> getDelegate() {
    return delegate;
  }

  @Override
  public String toString() {
    return "SkipCloseCoder{"
            + "delegate=" + delegate
            + '}';
  }
}
