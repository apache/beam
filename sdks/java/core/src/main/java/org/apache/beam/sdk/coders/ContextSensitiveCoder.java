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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * An abstract base class to implement a {@link Coder} that has a nested and unnested encoding.
 */
@Experimental(Kind.CODER_CONTEXT)
public abstract class ContextSensitiveCoder<T> extends StructuredCoder<T> {

  /** The context in which encoding or decoding is being done. */
  @Deprecated
  @Experimental(Kind.CODER_CONTEXT)
  public static class Context {
    /**
     * The outer context: the value being encoded or decoded takes
     * up the remainder of the record/stream contents.
     */
    public static final Context OUTER = new Context(true);

    /**
     * The nested context: the value being encoded or decoded is
     * (potentially) a part of a larger record/stream contents, and
     * may have other parts encoded or decoded after it.
     */
    public static final Context NESTED = new Context(false);

    /**
     * Whether the encoded or decoded value fills the remainder of the
     * output or input (resp.) record/stream contents.  If so, then
     * the size of the decoded value can be determined from the
     * remaining size of the record/stream contents, and so explicit
     * lengths aren't required.
     */
    public final boolean isWholeStream;

    public Context(boolean isWholeStream) {
      this.isWholeStream = isWholeStream;
    }

    public Context nested() {
      return NESTED;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Context)) {
        return false;
      }
      return Objects.equal(isWholeStream, ((Context) obj).isWholeStream);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(isWholeStream);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(Context.class)
          .addValue(isWholeStream ? "OUTER" : "NESTED").toString();
    }
  }

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
