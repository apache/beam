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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.nio.file.Paths;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

/** Does an external sort of the provided values. */
class NativeExternalSorter extends ExternalSorter {

  /** Whether {@link #sort()} was already called. */
  private boolean sortCalled = false;

  /** Sorter used to sort the input. */
  private @MonotonicNonNull NativeFileSorter sorter = null;

  /** Returns a {@link Sorter} configured with the given {@link Options}. */
  public static NativeExternalSorter create(Options options) {
    return new NativeExternalSorter(options);
  }

  @Override
  public void add(KV<byte[], byte[]> record) throws IOException {
    checkState(!sortCalled, "Records can only be added before sort()");
    getSorter().add(record.getKey(), record.getValue());
  }

  @Override
  public Iterable<KV<byte[], byte[]>> sort() throws IOException {
    checkState(!sortCalled, "sort() can only be called once.");
    sortCalled = true;
    return getSorter().sort();
  }

  private NativeExternalSorter(Options options) {
    super(options);
  }

  /**
   * Initializes the sorter. Does some local file system setup, and is somewhat expensive (~20 ms on
   * local machine). Only executed when necessary.
   */
  private NativeFileSorter getSorter() throws IOException {
    if (sorter == null) {
      sorter =
          new NativeFileSorter(
              Paths.get(options.getTempLocation()), (long) options.getMemoryMB() * 1024 * 1024);
    }
    return sorter;
  }
}
