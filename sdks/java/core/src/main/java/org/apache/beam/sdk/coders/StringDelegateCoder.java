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

import org.apache.beam.sdk.coders.DelegateCoder.CodingFunction;
import org.apache.beam.sdk.coders.protobuf.ProtoCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

/**
 * A {@link Coder} that wraps a {@code Coder<String>}
 * and encodes/decodes values via string representations.
 *
 * <p>To decode, the input byte stream is decoded to
 * a {@link String}, and this is passed to the single-argument
 * constructor for {@code T}.
 *
 * <p>To encode, the input value is converted via {@code toString()},
 * and this string is encoded.
 *
 * <p>In order for this to operate correctly for a class {@code Clazz},
 * it must be the case for any instance {@code x} that
 * {@code x.equals(new Clazz(x.toString()))}.
 *
 * <p>This method of encoding is not designed for ease of evolution of {@code Clazz};
 * it should only be used in cases where the class is stable or the encoding is not
 * important. If evolution of the class is important, see {@link ProtoCoder}, {@link AvroCoder},
 * or {@link JAXBCoder}.
 *
 * @param <T> The type of objects coded.
 */
public final class StringDelegateCoder<T> extends CustomCoder<T> {
  public static <T> StringDelegateCoder<T> of(Class<T> clazz) {
    return new StringDelegateCoder<T>(clazz);
  }

  @Override
  public String toString() {
    return "StringDelegateCoder(" + clazz + ")";
  }

  private final DelegateCoder<T, String> delegateCoder;
  private final Class<T> clazz;

  protected StringDelegateCoder(final Class<T> clazz) {
    delegateCoder = DelegateCoder.of(StringUtf8Coder.of(),
      new CodingFunction<T, String>() {
        @Override
        public String apply(T input) {
          return input.toString();
        }
      },
      new CodingFunction<String, T>() {
        @Override
        public T apply(String input) throws
            NoSuchMethodException,
            InstantiationException,
            IllegalAccessException,
            InvocationTargetException {
          return clazz.getConstructor(String.class).newInstance(input);
        }
      });

    this.clazz = clazz;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    StringDelegateCoder<?> that = (StringDelegateCoder<?>) o;
    return this.clazz.equals(that.clazz);
  }

  @Override
  public int hashCode() {
    return this.clazz.hashCode();
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context)
      throws CoderException, IOException {
    delegateCoder.encode(value, outStream, context);
  }

  @Override
  public T decode(InputStream inStream, Context context) throws CoderException, IOException {
    return delegateCoder.decode(inStream, context);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    delegateCoder.verifyDeterministic();
  }

  @Override
  public Object structuralValue(T value) throws Exception {
    return delegateCoder.structuralValue(value);
  }

  /**
   * The encoding id is the fully qualified name of the encoded/decoded class.
   */
  @Override
  public String getEncodingId() {
    return clazz.getName();
  }

  @Override
  public Collection<String> getAllowedEncodings() {
    return delegateCoder.getAllowedEncodings();
  }
}

