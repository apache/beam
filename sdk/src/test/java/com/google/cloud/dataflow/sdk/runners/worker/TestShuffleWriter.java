/*
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
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;

import java.util.ArrayList;
import java.util.List;

/**
 * A fake implementation of a ShuffleEntryWriter, for testing.
 */
public class TestShuffleWriter implements ShuffleEntryWriter {
  final List<ShuffleEntry> records = new ArrayList<>();
  final List<Long> sizes = new ArrayList<>();
  boolean closed = false;

  public TestShuffleWriter() { }

  @Override
  public long put(ShuffleEntry entry) {
    if (closed) {
      throw new AssertionError("shuffle writer already closed");
    }
    records.add(entry);

    long size = entry.length();
    sizes.add(size);
    return size;
  }

  @Override
  public void close() {
    if (closed) {
      throw new AssertionError("shuffle writer already closed");
    }
    closed = true;
  }

  /** Returns the key/value records that were written to this ShuffleWriter. */
  public List<ShuffleEntry> getRecords() {
    if (!closed) {
      throw new AssertionError("shuffle writer not closed");
    }
    return records;
  }

  /** Returns the sizes in bytes of the records that were written to this ShuffleWriter. */
  public List<Long> getSizes() {
    if (!closed) {
      throw new AssertionError("shuffle writer not closed");
    }
    return sizes;
  }
}
