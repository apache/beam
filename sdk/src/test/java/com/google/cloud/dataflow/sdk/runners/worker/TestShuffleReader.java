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

import com.google.cloud.dataflow.sdk.util.common.Reiterator;
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntryReader;
import com.google.cloud.dataflow.sdk.util.common.worker.ShufflePosition;
// TODO: Decide how we want to handle this Guava dependency.
import com.google.common.primitives.UnsignedBytes;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 * A fake implementation of a ShuffleEntryReader, for testing.
 */
public class TestShuffleReader implements ShuffleEntryReader {
  static final Comparator<byte[]> SHUFFLE_KEY_COMPARATOR =
      UnsignedBytes.lexicographicalComparator();
  final NavigableMap<byte[], List<ShuffleEntry>> records;

  public TestShuffleReader(NavigableMap<byte[], List<ShuffleEntry>> records) {
    this.records = records;
  }

  public TestShuffleReader() {
    this(new TreeMap<byte[], List<ShuffleEntry>>(SHUFFLE_KEY_COMPARATOR));
  }

  public void addEntry(String key, String value) {
    addEntry(key.getBytes(), value.getBytes());
  }

  public void addEntry(byte[] key, byte[] value) {
    addEntry(new ShuffleEntry(key, null, value));
  }

  public void addEntry(ShuffleEntry entry) {
    List<ShuffleEntry> values = records.get(entry.getKey());
    if (values == null) {
      values = new ArrayList<>();
      records.put(entry.getKey(), values);
    }
    values.add(entry);
  }

  public Iterator<ShuffleEntry> read() {
    return read((byte[]) null, (byte[]) null);
  }

  @Override
  public Reiterator<ShuffleEntry> read(ShufflePosition startPosition,
                                       ShufflePosition endPosition) {
    return read(ByteArrayShufflePosition.getPosition(startPosition),
                ByteArrayShufflePosition.getPosition(endPosition));
  }

  public Reiterator<ShuffleEntry> read(String startKey, String endKey) {
    return read(startKey == null ? null : startKey.getBytes(),
                endKey == null ? null : endKey.getBytes());
  }

  public Reiterator<ShuffleEntry>read(byte[] startKey, byte[] endKey) {
    return new ShuffleReaderIterator(startKey, endKey);
  }

  class ShuffleReaderIterator implements Reiterator<ShuffleEntry> {
    final Iterator<Map.Entry<byte[], List<ShuffleEntry>>> recordsIter;
    final byte[] startKey;
    final byte[] endKey;
    byte[] currentKey;
    Map.Entry<byte[], List<ShuffleEntry>> currentRecord;
    ListIterator<ShuffleEntry> currentValuesIter;

    public ShuffleReaderIterator(byte[] startKey, byte[] endKey) {
      this.recordsIter = records.entrySet().iterator();
      this.startKey = startKey;
      this.endKey = endKey;
      advanceKey();
    }

    private ShuffleReaderIterator(ShuffleReaderIterator it) {
      if (it.currentKey != null) {
        this.recordsIter =
            records.tailMap(it.currentKey, false).entrySet().iterator();
      } else {
        this.recordsIter = null;
      }
      this.startKey = it.startKey;
      this.endKey = it.endKey;
      this.currentKey = it.currentKey;
      this.currentRecord = it.currentRecord;
      if (it.currentValuesIter != null) {
        this.currentValuesIter =
            it.currentRecord.getValue().listIterator(
                it.currentValuesIter.nextIndex());
      } else {
        this.currentValuesIter = null;
      }
    }

    @Override
    public boolean hasNext() {
      return currentKey != null;
    }

    @Override
    public ShuffleEntry next() {
      if (currentKey == null) {
        throw new NoSuchElementException();
      }
      ShuffleEntry resultValue = currentValuesIter.next();
      Assert.assertTrue(Arrays.equals(currentKey, resultValue.getKey()));
      if (!currentValuesIter.hasNext()) {
        advanceKey();
      }
      return resultValue;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Reiterator<ShuffleEntry> copy() {
      return new ShuffleReaderIterator(this);
    }

    private void advanceKey() {
      while (recordsIter.hasNext()) {
        currentRecord = recordsIter.next();
        currentKey = currentRecord.getKey();
        if (startKey != null &&
            SHUFFLE_KEY_COMPARATOR.compare(currentKey, startKey) < 0) {
          // This key is before the start of the range.  Keep looking.
          continue;
        }
        if (endKey != null &&
            SHUFFLE_KEY_COMPARATOR.compare(currentKey, endKey) >= 0) {
          // This key is at or after the end of the range.  Stop looking.
          break;
        }
        // In range.
        currentValuesIter = currentRecord.getValue().listIterator();
        return;
      }
      currentKey = null;
      currentValuesIter = null;
    }
  }
}
