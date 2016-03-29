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

package com.google.cloud.dataflow.sdk.coders;

import com.google.cloud.dataflow.sdk.coders.protobuf.ProtoCoder;

import java.lang.reflect.InvocationTargetException;

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
public class StringDelegateCoder<T> extends DelegateCoder<T, String> {
  public static <T> StringDelegateCoder<T> of(Class<T> clazz) {
    return new StringDelegateCoder<T>(clazz);
  }

  @Override
  public String toString() {
    return "StringDelegateCoder(" + clazz + ")";
  }

  private final Class<T> clazz;

  protected StringDelegateCoder(final Class<T> clazz) {
    super(StringUtf8Coder.of(),
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

  /**
   * The encoding id is the fully qualified name of the encoded/decoded class.
   */
  @Override
  public String getEncodingId() {
    return clazz.getName();
  }
}
