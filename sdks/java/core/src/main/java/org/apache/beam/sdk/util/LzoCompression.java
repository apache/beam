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
package org.apache.beam.sdk.util;

import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.lzo.LzopCodec;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class LzoCompression {

  /**
   * Create an {@link InputStream} that will read from the given {@link InputStream} using {@link
   * LzoCodec}.
   *
   * @param inputStream the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  public static InputStream createLzoInputStream(InputStream inputStream) throws IOException {
    return new LzoCodec().createInputStream(inputStream);
  }

  /**
   * Create an {@link InputStream} that will read from the given {@link InputStream} using {@link
   * LzopCodec}.
   *
   * @param inputStream the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  public static InputStream createLzopInputStream(InputStream inputStream) throws IOException {
    return new LzopCodec().createInputStream(inputStream);
  }

  /**
   * Create an {@link OutputStream} that will write to the given {@link OutputStream}.
   *
   * @param outputStream the location for the final output stream
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  public static OutputStream createLzoOutputStream(OutputStream outputStream) throws IOException {
    return new LzoCodec().createOutputStream(outputStream);
  }

  /**
   * Create a {@link OutputStream} that will write to the given {@link OutputStream}.
   *
   * @param outputStream the location for the final output stream
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  public static OutputStream createLzopOutputStream(OutputStream outputStream) throws IOException {
    return new LzopCodec().createOutputStream(outputStream);
  }
}
