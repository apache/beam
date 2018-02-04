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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.util.Delegating;
import org.apache.beam.sdk.util.LimitedInputStream;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A coder encoding in memory and marking the size of the encoded value
 * to let be decoded enforcing this size.
 * @param <T> the supported type of the instance which can be encoded/decoded.
 */
public class LengthAwareCoder<T> extends Coder<T> implements Delegating<Coder<T>> {
  private Coder<T> delegate;

  protected LengthAwareCoder() {
    this.delegate = null;
  }

  public LengthAwareCoder(final Coder<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void encode(final T value, final OutputStream outStream) throws IOException {
    final ByteArrayOutputStream tmpOut = new ByteArrayOutputStream();
    delegate.encode(value, tmpOut);
    final byte[] buffer = tmpOut.toByteArray();
    VarInt.encode(buffer.length, outStream);
    outStream.write(buffer);
  }

  @Override
  public T decode(final InputStream inStream) throws IOException {
    final int maxLen = VarInt.decodeInt(inStream);
    return delegate.decode(new LimitedInputStream(maxLen, inStream));
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
      && delegate.equals(LengthAwareCoder.class.cast(o).delegate);
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
    return "LengthAwareCoder{"
            + "delegate=" + delegate
            + '}';
  }
}
