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
package org.apache.beam.runners.dataflow.worker;

import java.io.Closeable;
import java.io.IOException;

/** ShuffleReader reads chunks of data from a shuffle dataset for a given position range. */
interface ShuffleReader extends Closeable {
  /** Represents a chunk of data read from a shuffle dataset. */
  class ReadChunkResult {
    public final byte[] chunk;
    public final byte[] nextStartPosition;

    public ReadChunkResult(byte[] chunk, byte[] nextStartPosition) {
      this.chunk = chunk;
      this.nextStartPosition = nextStartPosition;
    }
  }

  /**
   * Reads a chunk of data for keys in the given position range. The chunk is a sequence of pairs
   * encoded as: {@code <position-size><position><key-size><key>
   * <secondary-key-size><secondary-key><value-size><value>} where the sizes are 4-byte big-endian
   * integers.
   *
   * @param startPosition the start of the requested range (inclusive)
   * @param endPosition the end of the requested range (exclusive)
   */
  ReadChunkResult readIncludingPosition(byte[] startPosition, byte[] endPosition)
      throws IOException;
}
