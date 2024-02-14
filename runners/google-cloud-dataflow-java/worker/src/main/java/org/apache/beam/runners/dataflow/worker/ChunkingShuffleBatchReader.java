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
import java.util.ArrayList;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ByteArrayShufflePosition;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleBatchReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntry;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShufflePosition;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
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
    ByteArrayReader input = new ByteArrayReader(result.chunk);
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
   * @param chunk chunk to read from
   * @return parsed ShuffleEntry
   */
  private ShuffleEntry getShuffleEntry(ByteArrayReader chunk) throws IOException {
    ByteString position = getFixedLengthPrefixedByteArray(chunk);
    ByteString key = getFixedLengthPrefixedByteArray(chunk);
    ByteString skey = getFixedLengthPrefixedByteArray(chunk);
    ByteString value = getFixedLengthPrefixedByteArray(chunk);

    return new ShuffleEntry(ByteArrayShufflePosition.of(position), key, skey, value);
  }

  /**
   * Extracts a length-prefix-encoded byte array from a given InputStream.
   *
   * @param chunk chunk to read from
   * @return parsed byte array
   */
  private static ByteString getFixedLengthPrefixedByteArray(ByteArrayReader chunk)
      throws IOException {
    int length = chunk.readInt();
    if (length < 0) {
      throw new IOException("invalid length: " + length);
    }
    return chunk.read(length);
  }
}
