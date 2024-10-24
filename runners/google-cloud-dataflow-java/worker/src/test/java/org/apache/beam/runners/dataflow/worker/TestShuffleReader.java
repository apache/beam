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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ByteArrayShufflePosition;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntry;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntryReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShufflePosition;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A fake implementation of a ShuffleEntryReader, for testing. */
public class TestShuffleReader implements ShuffleEntryReader {
  // Sorts by secondary key where an empty secondary key sorts before all other secondary keys.
  static final Comparator<ByteString> SHUFFLE_KEY_COMPARATOR =
      (o1, o2) -> {
        if (o1 == o2) {
          return 0;
        }
        if (o1 == null) {
          return -1;
        }
        if (o2 == null) {
          return 1;
        }
        return ByteString.unsignedLexicographicalComparator().compare(o1, o2);
      };

  final TreeMap<ByteString, TreeMap<ByteString, List<ShuffleEntry>>> records =
      new TreeMap<>(SHUFFLE_KEY_COMPARATOR);
  boolean closed = false;

  public TestShuffleReader() {}

  public void addEntry(String key, String secondaryKey, String value) {
    addEntry(
        new ShuffleEntry(
            ByteString.copyFromUtf8(key),
            ByteString.copyFromUtf8(secondaryKey),
            ByteString.copyFromUtf8(value)));
  }

  public void addEntry(ShuffleEntry entry) {
    TreeMap<ByteString, List<ShuffleEntry>> valuesBySecondaryKey = records.get(entry.getKey());
    if (valuesBySecondaryKey == null) {
      valuesBySecondaryKey = new TreeMap<>(SHUFFLE_KEY_COMPARATOR);
      records.put(entry.getKey(), valuesBySecondaryKey);
    }
    List<ShuffleEntry> values = valuesBySecondaryKey.get(entry.getSecondaryKey());
    if (values == null) {
      values = new ArrayList<>();
      valuesBySecondaryKey.put(entry.getSecondaryKey(), values);
    }
    values.add(entry);
  }

  public Iterator<ShuffleEntry> read() {
    return read((ByteString) null, null);
  }

  private ByteString toByteString(ShufflePosition pos) {
    byte[] posBytes = ByteArrayShufflePosition.getPosition(pos);
    if (posBytes == null) {
      return null;
    }
    return ByteString.copyFrom(posBytes);
  }

  @Override
  public Reiterator<ShuffleEntry> read(
      @Nullable ShufflePosition startPosition, @Nullable ShufflePosition endPosition) {
    if (closed) {
      throw new RuntimeException("Cannot read from a closed reader.");
    }
    return read(toByteString(startPosition), toByteString(endPosition));
  }

  public Reiterator<ShuffleEntry> read(@Nullable String startKey, @Nullable String endKey) {
    return read(
        startKey == null ? null : ByteString.copyFromUtf8(startKey),
        endKey == null ? null : ByteString.copyFromUtf8(endKey));
  }

  public Reiterator<ShuffleEntry> read(@Nullable ByteString startKey, @Nullable ByteString endKey) {
    List<ShuffleEntry> res = new ArrayList<>();
    for (ByteString key : records.keySet()) {
      if ((startKey == null || SHUFFLE_KEY_COMPARATOR.compare(startKey, key) <= 0)
          && (endKey == null || SHUFFLE_KEY_COMPARATOR.compare(key, endKey) < 0)) {
        TreeMap<ByteString, List<ShuffleEntry>> entriesBySecondaryKey = records.get(key);
        for (Map.Entry<ByteString, List<ShuffleEntry>> entries : entriesBySecondaryKey.entrySet()) {
          res.addAll(entries.getValue());
        }
      }
    }
    return new ListReiterator<>(res);
  }

  @Override
  public void close() {
    closed = true;
  }

  public boolean isClosed() {
    return closed;
  }

  private static class ListReiterator<T> implements Reiterator<T> {
    private final List<T> items;
    private int lastReturnedIndex = -1;

    private ListReiterator(List<T> items) {
      this.items = items;
    }

    @Override
    public Reiterator<T> copy() {
      ListReiterator<T> res = new ListReiterator<>(items);
      res.lastReturnedIndex = this.lastReturnedIndex;
      return res;
    }

    @Override
    public boolean hasNext() {
      return lastReturnedIndex < items.size() - 1;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      lastReturnedIndex++;
      return items.get(lastReturnedIndex);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
