/*
 * Copyright (C) 2014 Google Inc.
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

import java.lang.reflect.InvocationTargetException;

/**
 * A {@code StringDelegateCoder<T>} wraps a {@link Coder<String>}
 * and encodes/decodes values of type {@code T} via string representations.
 *
 * <p> To decode, the input byte stream is decoded to
 * a {@code String}, and this is passed to the single-arg
 * constructor for {@code T}.
 *
 * <p> To encode, the input value is converted via {@code toString()},
 * and this string is encoded.
 *
 * <p> In order for this to operate correctly for a class {@code Clazz},
 * it must be the case for any instance {@code x} that
 * {@code x.equals(new Clazz(x.toString()))}.
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
}
