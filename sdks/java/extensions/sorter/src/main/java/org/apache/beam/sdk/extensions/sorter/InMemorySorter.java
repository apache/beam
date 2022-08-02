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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sorts {@code <key, value>} pairs in memory. Based on the configured size of the memory buffer,
 * will reject additional pairs.
 */
class InMemorySorter implements Sorter {

  private static final Logger LOG = LoggerFactory.getLogger(InMemorySorter.class);

  private static final String ARCH_DATA_MODEL_PROPERTY = "sun.arch.data.model";

  private static final long DEFAULT_BYTES_PER_WORD = 8;

  /** {@code Options} contains configuration of the sorter. */
  public static class Options implements Serializable {
    private long memoryMB = 100;

    /** Sets the size of the memory buffer in megabytes. */
    public void setMemoryMB(long memoryMB) {
      checkArgument(memoryMB > 0, "memoryMB must be greater than zero");
      this.memoryMB = memoryMB;
    }

    /** Returns the configured size of the memory buffer. */
    public long getMemoryMB() {
      return memoryMB;
    }
  }

  /** The comparator to use to sort the records by key. */
  private static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();

  /** How many bytes per word in the running JVM. Assumes 64 bit/8 bytes if unknown. */
  private static final long NUM_BYTES_PER_WORD = getNumBytesPerWord();

  /**
   * Estimate of memory overhead per KV record in bytes not including memory associated with keys
   * and values.
   *
   * <ul>
   *   <li>Object reference within {@link ArrayList} (1 word),
   *   <li>A {@link KV} (2 words),
   *   <li>Two byte arrays (2 words for array lengths),
   *   <li>Per-object overhead (JVM-specific, guessing 2 words * 3 objects)
   * </ul>
   */
  private static final long RECORD_MEMORY_OVERHEAD_ESTIMATE = 11 * NUM_BYTES_PER_WORD;

  /** Maximum size of the buffer in bytes. */
  private final long maxBufferSize;

  /** Current number of stored bytes. Including estimated overhead bytes. */
  private long numBytes;

  /** Whether sort has been called. */
  private boolean sortCalled;

  /** The stored records to be sorted. */
  private final ArrayList<KV<byte[], byte[]>> records = new ArrayList<>();

  /** Private constructor. */
  private InMemorySorter(Options options) {
    maxBufferSize = options.getMemoryMB() * 1024L * 1024L;
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

    long recordBytes = estimateRecordBytes(record);
    if (roomInBuffer(numBytes + recordBytes, records.size() + 1L)) {
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
        (o1, o2) -> COMPARATOR.compare(o1.getKey(), o2.getKey());
    records.sort(kvComparator);
    return Collections.unmodifiableList(records);
  }

  /**
   * Estimate the number of additional bytes required to store this record. Including the key, the
   * value and any overhead for objects and references.
   */
  private long estimateRecordBytes(KV<byte[], byte[]> record) {
    return RECORD_MEMORY_OVERHEAD_ESTIMATE + record.getKey().length + record.getValue().length;
  }

  /**
   * Check whether we have room to store the provided total number of bytes and total number of
   * records.
   */
  private boolean roomInBuffer(long numBytes, long numRecords) {
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
  private static long getNumBytesPerWord() {
    String bitsPerWordProperty = System.getProperty(ARCH_DATA_MODEL_PROPERTY);
    if (bitsPerWordProperty == null) {
      LOG.warn(
          "System property {} not set; assuming {} bits per word",
          ARCH_DATA_MODEL_PROPERTY,
          DEFAULT_BYTES_PER_WORD);
      return DEFAULT_BYTES_PER_WORD;
    }

    Long bitsPerWord;
    try {
      bitsPerWord = Long.parseLong(bitsPerWordProperty);
    } catch (NumberFormatException e) {
      LOG.warn(
          "System property {} (\"{}\") could not be parsed; assuming {} bits per word",
          ARCH_DATA_MODEL_PROPERTY,
          bitsPerWordProperty,
          DEFAULT_BYTES_PER_WORD);
      return DEFAULT_BYTES_PER_WORD;
    }

    return bitsPerWord / 8;
  }
}
