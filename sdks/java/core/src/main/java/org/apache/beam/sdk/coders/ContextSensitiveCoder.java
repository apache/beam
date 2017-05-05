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
package org.apache.beam.sdk.coders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder.Context;

/**
 * An abstract base class to implement a {@link Coder} that has a nested and unnested encoding.
 */
@Experimental(Kind.CODER_CONTEXT)
public abstract class ContextSensitiveCoder<T> extends StructuredCoder<T> {
  protected ContextSensitiveCoder() {}

  /**
   * Encodes the given value of type {@code T} onto the given output stream
   * in the given context.
   *
   * @throws IOException if writing to the {@code OutputStream} fails
   * for some reason
   * @throws CoderException if the value could not be encoded for some reason
   */
  @Experimental(Kind.CODER_CONTEXT)
  public static <T> void encode(Coder<T> coder, T value, OutputStream outStream, Context context)
      throws CoderException, IOException {
    if (context.isWholeStream && coder instanceof ContextSensitiveCoder) {
      ((ContextSensitiveCoder<T>) coder).encode(value, outStream, context);
    } else {
      coder.encode(value, outStream);
    }
  }

  /**
   * Decodes a value of type {@code T} from the given input stream in
   * the given context.  Returns the decoded value.
   *
   * @throws IOException if reading from the {@code InputStream} fails
   * for some reason
   * @throws CoderException if the value could not be decoded for some reason
   */
  @Experimental(Kind.CODER_CONTEXT)
  public static <T> T decode(Coder<T> coder, InputStream inStream, Context context)
      throws CoderException, IOException {
    if (context.isWholeStream && coder instanceof ContextSensitiveCoder) {
      return ((ContextSensitiveCoder<T>) coder).decode(inStream, context);
    } else {
      return coder.decode(inStream);
    }
  }

  /**
   * Encodes the given value of type {@code T} onto the given output stream
   * in the given context.
   *
   * @throws IOException if writing to the {@code OutputStream} fails
   * for some reason
   * @throws CoderException if the value could not be encoded for some reason
   */
  @Experimental(Kind.CODER_CONTEXT)
  public abstract void encode(T value, OutputStream outStream, Context context)
      throws CoderException, IOException;

  public final void encode(T value, OutputStream outStream)
      throws CoderException, IOException {
    encode(value, outStream, Context.NESTED);
  }

  /**
   * Decodes a value of type {@code T} from the given input stream in
   * the given context.  Returns the decoded value.
   *
   * @throws IOException if reading from the {@code InputStream} fails
   * for some reason
   * @throws CoderException if the value could not be decoded for some reason
   */
  @Experimental(Kind.CODER_CONTEXT)
  public abstract T decode(InputStream inStream, Context context)
      throws CoderException, IOException;

  public final T decode(InputStream inStream)
      throws CoderException, IOException {
    return decode(inStream, Context.NESTED);
  }
}
