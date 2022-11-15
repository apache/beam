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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * Wraps an existing coder with Zstandard compression. It makes sense to use this coder when it's
 * likely that the encoded value is quite large and compressible or when a dictionary is available
 * to improve compression performance.
 */
public class ZstdCoder<T> extends StructuredCoder<T> {
  private final Coder<T> innerCoder;
  private final @Nullable byte[] dict;
  private final int level;

  /** Wraps the given coder into a {@link ZstdCoder}. */
  public static <T> ZstdCoder<T> of(Coder<T> innerCoder, byte[] dict, int level) {
    return new ZstdCoder<>(innerCoder, dict, level);
  }

  /** Wraps the given coder into a {@link ZstdCoder}. */
  public static <T> ZstdCoder<T> of(Coder<T> innerCoder, byte[] dict) {
    return new ZstdCoder<>(innerCoder, dict, Zstd.defaultCompressionLevel());
  }

  /** Wraps the given coder into a {@link ZstdCoder}. */
  public static <T> ZstdCoder<T> of(Coder<T> innerCoder, int level) {
    return new ZstdCoder<>(innerCoder, null, level);
  }

  /** Wraps the given coder into a {@link ZstdCoder}. */
  public static <T> ZstdCoder<T> of(Coder<T> innerCoder) {
    return new ZstdCoder<>(innerCoder, null, Zstd.defaultCompressionLevel());
  }

  private ZstdCoder(Coder<T> innerCoder, @Nullable byte[] dict, int level) {
    this.innerCoder = innerCoder;
    this.dict = dict;
    this.level = level;
  }

  @Override
  public void encode(T value, OutputStream os) throws IOException {
    ZstdCompressCtx ctx = new ZstdCompressCtx();
    try {
      ctx.setLevel(level);
      ctx.setMagicless(true); // No magic since we know this will be compressed data on decode.
      ctx.setDictID(false); // No dict ID since we initialize the coder with the expected dict.
      ctx.loadDict(dict);

      byte[] encoded = CoderUtils.encodeToByteArray(innerCoder, value);
      byte[] compressed = ctx.compress(encoded);
      ByteArrayCoder.of().encode(compressed, os);
    } finally {
      ctx.close();
    }
  }

  @Override
  public T decode(InputStream is) throws IOException {
    ZstdDecompressCtx ctx = new ZstdDecompressCtx();
    try {
      ctx.setMagicless(true);
      ctx.loadDict(dict);

      byte[] compressed = ByteArrayCoder.of().decode(is);
      int decompressedSize = (int) Zstd.decompressedSize(compressed, 0, compressed.length, true);
      byte[] encoded = ctx.decompress(compressed, decompressedSize);
      return CoderUtils.decodeFromByteArray(innerCoder, encoded);
    } finally {
      ctx.close();
    }
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
