/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleBatchReader;
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;
import com.google.cloud.dataflow.sdk.util.common.worker.ShufflePosition;
import com.google.common.io.ByteStreams;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import javax.annotation.Nullable;

/**
 * ChunkingShuffleBatchReader reads data from a shuffle dataset using a
 * ShuffleReader.
 */
final class ChunkingShuffleBatchReader implements ShuffleBatchReader {
  private ShuffleReader reader;

  /**
   * @param reader used to read from a shuffle dataset
   */
  public ChunkingShuffleBatchReader(ShuffleReader reader) throws IOException {
    this.reader = reader;
  }

  @Override
  public ShuffleBatchReader.Batch read(
      @Nullable ShufflePosition startShufflePosition,
      @Nullable ShufflePosition endShufflePosition) throws IOException {
    @Nullable byte[] startPosition =
        ByteArrayShufflePosition.getPosition(startShufflePosition);
    @Nullable byte[] endPosition =
        ByteArrayShufflePosition.getPosition(endShufflePosition);

    ShuffleReader.ReadChunkResult result =
        reader.readIncludingPosition(startPosition, endPosition);
    InputStream input = new ByteArrayInputStream(result.chunk);
    ArrayList<ShuffleEntry> entries = new ArrayList<>();
    while (input.available() > 0) {
      entries.add(getShuffleEntry(input));
    }
    return new Batch(entries, result.nextStartPosition == null ? null
        : ByteArrayShufflePosition.of(result.nextStartPosition));
  }

  /**
   * Extracts a ShuffleEntry by parsing bytes from a given InputStream.
   *
   * @param input stream to read from
   * @return parsed ShuffleEntry
   */
  static ShuffleEntry getShuffleEntry(InputStream input) throws IOException {
    byte[] position = getFixedLengthPrefixedByteArray(input);
    byte[] key = getFixedLengthPrefixedByteArray(input);
    byte[] skey = getFixedLengthPrefixedByteArray(input);
    byte[] value = getFixedLengthPrefixedByteArray(input);
    return new ShuffleEntry(position, key, skey, value);
  }

  /**
   * Extracts a length-prefix-encoded byte array from a given InputStream.
   *
   * @param input stream to read from
   * @return parsed byte array
   */
  static byte[] getFixedLengthPrefixedByteArray(InputStream input)
      throws IOException {
    DataInputStream dataInputStream = new DataInputStream(input);
    int length = dataInputStream.readInt();
    if (length < 0) {
      throw new IOException("invalid length: " + length);
    }
    byte[] data = new byte[(int) length];
    ByteStreams.readFully(dataInputStream, data);
    return data;
  }
}
