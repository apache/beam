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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Properties for use in {@link Coder} tests. These are implemented with junit assertions
 * rather than as predicates for the sake of error messages.
 */
public class CoderProperties {

  /**
   * Verifies that for the given {@link Coder<T>}, {@link Coder.Context}, and values of
   * type {@code T}, if the values are equal then the encoded bytes are equal.
   */
  public static <T> void coderDeterministic(
      Coder<T> coder, Coder.Context context, T value1, T value2)
      throws Exception {
    assumeThat(value1, equalTo(value2));
    assertArrayEquals(encode(coder, context, value1), encode(coder, context, value2));
  }

  /**
   * Verifies that for the given {@link Coder<T>}, {@link Coder.Context},
   * and value of type {@code T}, encoding followed by decoding yields an
   * equal of type {@code T}.
   */
  public static <T> void coderDecodeEncodeEqual(
      Coder<T> coder, Coder.Context context, T value)
      throws Exception {
    assertEquals(
        decode(coder, context, encode(coder, context, value)),
        value);
  }

  //////////////////////////////////////////////////////////////////////////

  private static <T> byte[] encode(
      Coder<T> coder, Coder.Context context, T value) throws CoderException, IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    coder.encode(value, os, context);
    return os.toByteArray();
  }

  private static <T> T decode(
      Coder<T> coder, Coder.Context context, byte[] bytes) throws CoderException, IOException {
    ByteArrayInputStream is = new ByteArrayInputStream(bytes);
    return coder.decode(is, context);
  }

}
