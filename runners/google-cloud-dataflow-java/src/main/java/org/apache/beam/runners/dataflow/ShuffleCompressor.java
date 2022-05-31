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
package org.apache.beam.runners.dataflow;

import java.io.Closeable;
import java.nio.ByteBuffer;
import org.apache.beam.runners.dataflow.util.RandomAccessData;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface ShuffleCompressor extends Closeable {
  interface Factory {
    @Nullable ShuffleCompressor create(String datasetId);
  }

  /**
   * Compress data "in-place". `data` contains the bytes to compress starting at `start` and `len`
   * bytes long. After compression, the compressed data should be in `data` starting at `start`.
   *
   * <p>When compress returns `data` MUST be set to the position 1 byte after the last byte written.
   *
   * @param data The buffer to read data from and compress into.
   * @param start The starting position of the data to compress.
   * @param len The number of bytes to compress.
   */
  void compress(RandomAccessData data, int start, int len);

  /**
   * Decompress `input`.
   *
   * @param input The data to decompress. This buffer is guaranteed to always be a heap-backed
   *     buffer.
   * @return A ByteBuffer containing the decompressed data.
   */
  ByteBuffer decompress(ByteBuffer input);
}
