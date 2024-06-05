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
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntry;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;

/** A fake implementation of a ShuffleEntryWriter, for testing. */
public class TestShuffleWriter implements ShuffleWriter {
  final List<ShuffleEntry> records = new ArrayList<>();
  final List<Long> sizes = new ArrayList<>();
  boolean closed = false;

  public TestShuffleWriter() {}

  @Override
  public void write(byte[] chunk) throws IOException {
    if (closed) {
      throw new AssertionError("shuffle writer already closed");
    }
    DataInputStream dais = new DataInputStream(new ByteArrayInputStream(chunk));
    while (dais.available() > 0) {
      byte[] key = new byte[dais.readInt()];
      dais.readFully(key);
      byte[] sortKey = new byte[dais.readInt()];
      dais.readFully(sortKey);
      byte[] value = new byte[dais.readInt()];
      dais.readFully(value);

      ShuffleEntry entry =
          new ShuffleEntry(
              ByteString.copyFrom(key), ByteString.copyFrom(sortKey), ByteString.copyFrom(value));
      records.add(entry);

      long size = entry.length();
      sizes.add(size);
    }
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
