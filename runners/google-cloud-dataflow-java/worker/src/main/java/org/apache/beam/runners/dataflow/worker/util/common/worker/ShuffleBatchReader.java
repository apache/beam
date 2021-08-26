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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import java.io.IOException;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * ShuffleBatchReader provides an interface for reading a batch of key/value entries from a shuffle
 * dataset.
 */
public interface ShuffleBatchReader {
  /** The result returned by #read. */
  public static class Batch {
    public final List<ShuffleEntry> entries;
    public final @Nullable ShufflePosition nextStartPosition;

    public Batch(List<ShuffleEntry> entries, @Nullable ShufflePosition nextStartPosition) {
      this.entries = entries;
      this.nextStartPosition = nextStartPosition;
    }
  }

  /**
   * Reads a batch of data from a shuffle dataset.
   *
   * @param startPosition encodes the initial key from where to read. This parameter may be null,
   *     indicating that the read should start with the first key in the dataset.
   * @param endPosition encodes the key "just past" the end of the range to be read; keys up to
   *     endPosition will be returned, but keys equal to or greater than endPosition will not. This
   *     parameter may be null, indicating that the read should end just past the last key in the
   *     dataset (that is, the last key in the dataset will be included in the read, as long as that
   *     key is greater than or equal to startPosition).
   * @return the first {@link Batch} of entries
   */
  public Batch read(@Nullable ShufflePosition startPosition, @Nullable ShufflePosition endPosition)
      throws IOException;
}
