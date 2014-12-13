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

import static com.google.api.client.util.Preconditions.checkNotNull;

import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * ChunkingShuffleEntryWriter buffers ShuffleEntries and writes them
 * in batches to a shuffle dataset using a given writer.
 */
@NotThreadSafe
final class ChunkingShuffleEntryWriter implements ShuffleEntryWriter {
  // Approximate maximum size of a chunk in bytes.
  private static final int MAX_CHUNK_SIZE = 1 << 20;

  private static final byte[] EMPTY_BYTES = new byte[0];

  private ByteArrayOutputStream chunk = new ByteArrayOutputStream();

  private final ShuffleWriter writer;

  /**
   * @param writer used to write chunks created by this writer
   */
  public ChunkingShuffleEntryWriter(ShuffleWriter writer) {
    this.writer = checkNotNull(writer);
  }

  @Override
  public long put(ShuffleEntry entry) throws IOException {
    if (chunk.size() >= MAX_CHUNK_SIZE) {
      writeChunk();
    }

    DataOutputStream output = new DataOutputStream(chunk);
    return putFixedLengthPrefixedByteArray(entry.getKey(), output)
        + putFixedLengthPrefixedByteArray(entry.getSecondaryKey(), output)
        + putFixedLengthPrefixedByteArray(entry.getValue(), output);
  }

  @Override
  public void close() throws IOException {
    writeChunk();
    writer.close();
  }

  private void writeChunk() throws IOException {
    if (chunk.size() > 0) {
      writer.write(chunk.toByteArray());
      chunk.reset();
    }
  }

  static int putFixedLengthPrefixedByteArray(byte[] data,
                                             DataOutputStream output)
      throws IOException {
    if (data == null) {
      data = EMPTY_BYTES;
    }
    int bytesWritten = output.size();
    output.writeInt(data.length);
    output.write(data, 0, data.length);
    return output.size() - bytesWritten;
  }
}
