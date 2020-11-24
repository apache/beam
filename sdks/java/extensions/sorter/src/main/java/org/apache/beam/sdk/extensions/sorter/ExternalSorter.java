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

import java.io.Serializable;

/** Does an external sort of the provided values. */
public abstract class ExternalSorter implements Sorter {
  protected final Options options;

  /** {@link Options} contains configuration of the sorter. */
  public static class Options implements Serializable {
    private String tempLocation = "/tmp";
    private int memoryMB = 100;
    private SorterType sorterType = SorterType.HADOOP;

    /** Sorter type. */
    public enum SorterType {
      HADOOP,
      NATIVE
    }

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
      this.memoryMB = memoryMB;
      checkMemoryMB();
      return this;
    }

    /** Returns the configured size of the memory buffer. */
    public int getMemoryMB() {
      return memoryMB;
    }

    /** Sets the sorter type. */
    public Options setSorterType(SorterType sorterType) {
      this.sorterType = sorterType;
      checkMemoryMB();
      return this;
    }

    /** Returns the sorter type. */
    public SorterType getSorterType() {
      return sorterType;
    }

    private void checkMemoryMB() {
      checkArgument(memoryMB > 0, "memoryMB must be greater than zero");
      if (getSorterType() == SorterType.HADOOP) {
        // Hadoop's external sort stores the number of available memory bytes in an int, this
        // prevents overflow
        checkArgument(memoryMB < 2048, "memoryMB must be less than 2048 for Hadoop sorter");
      }
    }
  }

  /** Returns a {@link Sorter} configured with the given {@link Options}. */
  public static ExternalSorter create(Options options) {
    return options.getSorterType() == Options.SorterType.HADOOP
        ? HadoopExternalSorter.create(options)
        : NativeExternalSorter.create(options);
  }

  ExternalSorter(Options options) {
    this.options = options;
  }
}
