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
import java.util.List;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.xerial.snappy.Snappy;

/**
 * Wraps an existing coder with Snappy compression. It makes sense to use this coder only when it's
 * likely that the encoded value is quite large and compressible.
 */
public class SnappyCoder<T> extends StructuredCoder<T> {
  private final Coder<T> innerCoder;

  /** Wraps the given coder into a {@link SnappyCoder}. */
  public static <T> SnappyCoder<T> of(Coder<T> innerCoder) {
    return new SnappyCoder<>(innerCoder);
  }

  private SnappyCoder(Coder<T> innerCoder) {
    this.innerCoder = innerCoder;
  }

  @Override
  public void encode(T value, OutputStream os) throws IOException {
    ByteArrayCoder.of()
        .encode(Snappy.compress(CoderUtils.encodeToByteArray(innerCoder, value)), os);
  }

  @Override
  public T decode(InputStream is) throws IOException {
    return CoderUtils.decodeFromByteArray(
        innerCoder, Snappy.uncompress(ByteArrayCoder.of().decode(is)));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return ImmutableList.of(innerCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    innerCoder.verifyDeterministic();
  }
}
