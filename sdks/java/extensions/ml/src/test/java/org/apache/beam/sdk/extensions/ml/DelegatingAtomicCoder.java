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
package org.apache.beam.sdk.extensions.ml;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;

public abstract class DelegatingAtomicCoder<X, W> extends AtomicCoder<X> {

  private final Coder<W> delegate;

  protected DelegatingAtomicCoder(Coder<W> delegate) {
    this.delegate = delegate;
  }

  @Override
  public final X decode(InputStream inStream) throws CoderException, IOException {
    return from(delegate.decode(inStream));
  }

  @Override
  public final void encode(X value, OutputStream outStream) throws CoderException, IOException {
    delegate.encode(to(value), outStream);
  }

  protected abstract X from(W object) throws CoderException, IOException;

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    delegate.verifyDeterministic();
  }

  protected abstract W to(X object) throws CoderException, IOException;
}
