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

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.values.KV;

/**
 * {@link Sorter} that will use in memory sorting until the values can't fit into memory and will
 * then fall back to external sorting.
 */
public class BufferedExternalSorter implements Sorter {
  /** Contains configuration for the sorter. */
  public static class Options implements Serializable {
    private String tempLocation = "/tmp";
    private int memoryMB = 100;

    /** Sets the path to a temporary location where the sorter writes intermediate files. */
    public Options setTempLocation(String tempLocation) {
      checkArgument(
          !tempLocation.startsWith("gs://"),
          "BufferedExternalSorter does not support GCS temporary location");

      this.tempLocation = tempLocation;
      return this;
    }

    /** Returns the configured temporary location. */
    public String getTempLocation() {
      return tempLocation;
    }

    /**
     * Sets the size of the memory buffer in megabytes. This controls both the buffer for initial in
     * memory sorting and the buffer used when external sorting. Must be greater than zero.
     */
    public Options setMemoryMB(int memoryMB) {
      checkArgument(memoryMB > 0, "memoryMB must be greater than zero");
      this.memoryMB = memoryMB;
      return this;
    }

    /** Returns the configured size of the memory buffer. */
    public int getMemoryMB() {
      return memoryMB;
    }
  }

  private ExternalSorter externalSorter;
  private InMemorySorter inMemorySorter;

  boolean inMemorySorterFull;

  BufferedExternalSorter(ExternalSorter externalSorter, InMemorySorter inMemorySorter) {
    this.externalSorter = externalSorter;
    this.inMemorySorter = inMemorySorter;
  }

  public static BufferedExternalSorter create(Options options) {
    ExternalSorter.Options externalSorterOptions = new ExternalSorter.Options();
    externalSorterOptions.setMemoryMB(options.getMemoryMB());
    externalSorterOptions.setTempLocation(options.getTempLocation());

    InMemorySorter.Options inMemorySorterOptions = new InMemorySorter.Options();
    inMemorySorterOptions.setMemoryMB(options.getMemoryMB());

    return new BufferedExternalSorter(
        ExternalSorter.create(externalSorterOptions), InMemorySorter.create(inMemorySorterOptions));
  }

  @Override
  public void add(KV<byte[], byte[]> record) throws IOException {
    if (!inMemorySorterFull) {
      if (inMemorySorter.addIfRoom(record)) {
        return;
      } else {
        // Flushing contents of in memory sorter to external sorter so we can rely on external
        // from here on out
        inMemorySorterFull = true;
        transferToExternalSorter();
      }
    }

    // In memory sorter is full, so put in external sorter instead
    externalSorter.add(record);
  }

  /**
   * Transfers all of the records loaded so far into the in memory sorter over to the external
   * sorter.
   */
  private void transferToExternalSorter() throws IOException {
    for (KV<byte[], byte[]> record : inMemorySorter.sort()) {
      externalSorter.add(record);
    }
    // Allow in memory sorter and its contents to be garbage collected
    inMemorySorter = null;
  }

  @Override
  public Iterable<KV<byte[], byte[]>> sort() throws IOException {
    if (!inMemorySorterFull) {
      return inMemorySorter.sort();
    } else {
      return externalSorter.sort();
    }
  }
}
