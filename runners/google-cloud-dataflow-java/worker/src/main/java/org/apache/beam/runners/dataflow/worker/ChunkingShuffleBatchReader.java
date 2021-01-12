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

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ByteArrayShufflePosition;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleBatchReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntry;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShufflePosition;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.checkerframework.checker.nullness.qual.Nullable;

/** ChunkingShuffleBatchReader reads data from a shuffle dataset using a ShuffleReader. */
final class ChunkingShuffleBatchReader implements ShuffleBatchReader {
  private ShuffleReader reader;
  private ExecutionStateTracker tracker;
  private ExecutionState readState;

  public ChunkingShuffleBatchReader(
      BatchModeExecutionContext executionContext,
      DataflowOperationContext operationContext,
      ShuffleReader reader) {
    this.reader = reader;
    this.readState = operationContext.newExecutionState("read-shuffle");
    this.tracker = executionContext.getExecutionStateTracker();
  }

  @Override
  public ShuffleBatchReader.Batch read(
      @Nullable ShufflePosition startShufflePosition, @Nullable ShufflePosition endShufflePosition)
      throws IOException {
    byte @Nullable [] startPosition = ByteArrayShufflePosition.getPosition(startShufflePosition);
    byte @Nullable [] endPosition = ByteArrayShufflePosition.getPosition(endShufflePosition);

    ShuffleReader.ReadChunkResult result;
    try (Closeable trackedReadState = tracker.enterState(readState)) {
      result = reader.readIncludingPosition(startPosition, endPosition);
    }
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(result.chunk));
    ArrayList<ShuffleEntry> entries = new ArrayList<>();
    while (input.available() > 0) {
      entries.add(getShuffleEntry(input));
    }
    return new Batch(
        entries,
        result.nextStartPosition == null
            ? null
            : ByteArrayShufflePosition.of(result.nextStartPosition));
  }

  /**
   * Extracts a ShuffleEntry by parsing bytes from a given InputStream.
   *
   * @param input stream to read from
   * @return parsed ShuffleEntry
   */
  static ShuffleEntry getShuffleEntry(DataInputStream input) throws IOException {
    byte[] position = getFixedLengthPrefixedByteArray(input);
    byte[] key = getFixedLengthPrefixedByteArray(input);
    byte[] skey = getFixedLengthPrefixedByteArray(input);
    byte[] value = getFixedLengthPrefixedByteArray(input);
    return new ShuffleEntry(ByteArrayShufflePosition.of(position), key, skey, value);
  }

  /**
   * Extracts a length-prefix-encoded byte array from a given InputStream.
   *
   * @param dataInputStream stream to read from
   * @return parsed byte array
   */
  static byte[] getFixedLengthPrefixedByteArray(DataInputStream dataInputStream)
      throws IOException {
    int length = dataInputStream.readInt();
    if (length < 0) {
      throw new IOException("invalid length: " + length);
    }
    byte[] data = new byte[length];
    ByteStreams.readFully(dataInputStream, data);
    return data;
  }
}
