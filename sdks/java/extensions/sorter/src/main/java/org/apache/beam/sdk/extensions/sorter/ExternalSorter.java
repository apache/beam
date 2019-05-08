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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import org.apache.beam.sdk.values.KV;

/** Does an external sort of the provided values. */
class ExternalSorter implements Sorter {
  private final Options options;

  /** Whether {@link #sort()} was already called. */
  private boolean sortCalled = false;

  /** Sorter used to sort the input. */
  private ExternalFileSorter sorter;

  private boolean initialized = false;

  /** {@link Options} contains configuration of the sorter. */
  public static class Options implements Serializable {
    private String tempLocation = "/tmp";
    private int memoryMB = 100;

    /** Sets the path to a temporary location where the sorter writes intermediate files. */
    public Options setTempLocation(String tempLocation) {
      if (tempLocation.startsWith("gs://")) {
        throw new IllegalArgumentException("Sorter doesn't support GCS temporary location.");
      }

      this.tempLocation = tempLocation;
      return this;
    }

    /** Returns the configured temporary location. */
    public String getTempLocation() {
      return tempLocation;
    }

    /**
     * Sets the size of the memory buffer in megabytes. Must be greater than zero and less than
     * 2048.
     */
    public Options setMemoryMB(int memoryMB) {
      checkArgument(memoryMB > 0, "memoryMB must be greater than zero");
      // Hadoop's external sort stores the number of available memory bytes in an int, this prevents
      // integer overflow
      checkArgument(memoryMB < 2048, "memoryMB must be less than 2048");
      this.memoryMB = memoryMB;
      return this;
    }

    /** Returns the configured size of the memory buffer. */
    public int getMemoryMB() {
      return memoryMB;
    }
  }

  /** Returns a {@link Sorter} configured with the given {@link Options}. */
  public static ExternalSorter create(Options options) {
    return new ExternalSorter(options);
  }

  @Override
  public void add(KV<byte[], byte[]> record) throws IOException {
    checkState(!sortCalled, "Records can only be added before sort()");

    initSorter();

    sorter.add(record.getKey(), record.getValue());
  }

  @Override
  public Iterable<KV<byte[], byte[]>> sort() throws IOException {
    checkState(!sortCalled, "sort() can only be called once.");
    sortCalled = true;

    initSorter();

    return sorter.sort();
  }

  private ExternalSorter(Options options) {
    this.options = options;
  }

  /**
   * Initializes the sorter. Does some local file system setup, and is somewhat expensive (~20 ms on
   * local machine). Only executed when necessary.
   */
  private void initSorter() throws IOException {
    if (!initialized) {
      sorter =
          new ExternalFileSorter(
              Paths.get(options.getTempLocation()), (long) options.getMemoryMB() * 1024 * 1024);
      initialized = true;
    }
  }
}
