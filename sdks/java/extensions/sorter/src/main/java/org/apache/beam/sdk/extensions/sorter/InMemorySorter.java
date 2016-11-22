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

package org.apache.beam.sdk.extensions.sorter;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.primitives.UnsignedBytes;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.apache.beam.sdk.values.KV;

/**
 * Sorts {@code <key, value>} pairs in memory. Based on the configured size of the memory buffer,
 * will reject additional pairs.
 */
class InMemorySorter implements Sorter {
  /** {@code Options} contains configuration of the sorter. */
  public static class Options implements Serializable {
    private int memoryMB = 100;

    /** Sets the size of the memory buffer in megabytes. */
    public void setMemoryMB(int memoryMB) {
      checkArgument(memoryMB > 0, "memoryMB must be greater than zero");
      this.memoryMB = memoryMB;
    }

    /** Returns the configured size of the memory buffer. */
    public int getMemoryMB() {
      return memoryMB;
    }
  }

  /** The comparator to use to sort the records by key. */
  private static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();

  /** How many bytes per word in the running JVM. Assumes 64 bit/8 bytes if unknown. */
  private static final int NUM_BYTES_PER_WORD = getNumBytesPerWord();

  /**
   * Estimate of memory overhead per KV record in bytes not including memory associated with keys
   * and values.
   *
   * <ul>
   *   <li> Object reference within {@link ArrayList} (1 word),
   *   <li> A {@link KV} (2 words),
   *   <li> Two byte arrays (2 words for array lengths),
   *   <li> Per-object overhead (JVM-specific, guessing 2 words * 3 objects)
   * </ul>
   */
  private static final int RECORD_MEMORY_OVERHEAD_ESTIMATE = 11 * NUM_BYTES_PER_WORD;

  /** Maximum size of the buffer in bytes. */
  private int maxBufferSize;

  /** Current number of stored bytes. Including estimated overhead bytes. */
  private int numBytes;

  /** Whether sort has been called. */
  private boolean sortCalled;

  /** The stored records to be sorted. */
  private ArrayList<KV<byte[], byte[]>> records = new ArrayList<KV<byte[], byte[]>>();

  /** Private constructor. */
  private InMemorySorter(Options options) {
    maxBufferSize = options.getMemoryMB() * 1024 * 1024;
  }

  /** Create a new sorter from provided options. */
  public static InMemorySorter create(Options options) {
    return new InMemorySorter(options);
  }

  @Override
  public void add(KV<byte[], byte[]> record) {
    checkState(addIfRoom(record), "No space remaining for in memory sorting");
  }

  /** Adds the record is there is room and returns true. Otherwise returns false. */
  public boolean addIfRoom(KV<byte[], byte[]> record) {
    checkState(!sortCalled, "Records can only be added before sort()");

    int recordBytes = estimateRecordBytes(record);
    if (roomInBuffer(numBytes + recordBytes, records.size() + 1)) {
      records.add(record);
      numBytes += recordBytes;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Iterable<KV<byte[], byte[]>> sort() {
    checkState(!sortCalled, "sort() can only be called once.");

    sortCalled = true;

    Comparator<KV<byte[], byte[]>> kvComparator =
        new Comparator<KV<byte[], byte[]>>() {

          @Override
          public int compare(KV<byte[], byte[]> o1, KV<byte[], byte[]> o2) {
            return COMPARATOR.compare(o1.getKey(), o2.getKey());
          }
        };
    Collections.sort(records, kvComparator);
    return Collections.unmodifiableList(records);
  }

  /**
   * Estimate the number of additional bytes required to store this record. Including the key, the
   * value and any overhead for objects and references.
   */
  private int estimateRecordBytes(KV<byte[], byte[]> record) {
    return RECORD_MEMORY_OVERHEAD_ESTIMATE + record.getKey().length + record.getValue().length;
  }

  /**
   * Check whether we have room to store the provided total number of bytes and total number of
   * records.
   */
  private boolean roomInBuffer(int numBytes, int numRecords) {
    // Collections.sort may allocate up to n/2 extra object references.
    // Also, ArrayList grows by a factor of 1.5x, so there might be up to n/2 null object
    // references in the backing array.
    // And finally, in Java 7, Collections.sort performs a defensive copy to an array in case the
    // input list is a LinkedList.
    // So assume we need an additional overhead of two words per record in the worst case
    return (numBytes + (numRecords * NUM_BYTES_PER_WORD * 2)) < maxBufferSize;
  }

  /**
   * Returns the number of bytes in a word according to the JVM. Defaults to 8 for 64 bit if answer
   * unknown.
   */
  private static int getNumBytesPerWord() {
    String bitsPerWord = System.getProperty("sun.arch.data.model");

    try {
      return Integer.parseInt(bitsPerWord) / 8;
    } catch (Exception e) {
      // Can't determine whether 32 or 64 bit, so assume 64
      return 8;
    }
  }
}
