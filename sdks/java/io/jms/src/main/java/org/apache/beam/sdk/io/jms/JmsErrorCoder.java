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
package org.apache.beam.sdk.io.jms;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StructuredCoder;

public class JmsErrorCoder<T> extends StructuredCoder<JmsError<T>> {
  private final Coder<T> recordCoder;
  private final Coder<Exception> exceptionCoder;

  public JmsErrorCoder(Coder<T> recordCoder) {
    this.recordCoder = recordCoder;
    this.exceptionCoder = SerializableCoder.of(Exception.class);
  }

  public static <T> JmsErrorCoder<T> of(Coder<T> recordCoder) {
    return new JmsErrorCoder<>(recordCoder);
  }

  @Override
  public void encode(JmsError<T> value, OutputStream outStream) throws CoderException, IOException {
    recordCoder.encode(value.getRecord(), outStream);
    exceptionCoder.encode(value.getException(), outStream);
  }

  @Override
  public JmsError<T> decode(InputStream inStream) throws CoderException, IOException {
    T record = recordCoder.decode(inStream);
    Exception exception = exceptionCoder.decode(inStream);
    return new JmsError<>(record, exception);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.singletonList(recordCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    recordCoder.verifyDeterministic();
    exceptionCoder.verifyDeterministic();
  }
}
