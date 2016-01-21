/*
 * Copyright (C) 2015 Google Inc.
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
  // Sorts by secondary key where an empty secondary key sorts before all other secondary keys.
  static final Comparator<byte[]> SHUFFLE_KEY_COMPARATOR = new Comparator<byte[]>() {

    @Override
    public int compare(byte[] o1, byte[] o2) {
      if (o1 == o2) {
        return 0;
      }
      if (o1 == null) {
        return -1;
      }
      if (o2 == null) {
        return 1;
      }
      return UnsignedBytes.lexicographicalComparator().compare(o1, o2);
    }
  };

  final NavigableMap<byte[], NavigableMap<byte[], List<ShuffleEntry>>> records;

  public TestShuffleReader() {
    this.records = new TreeMap<>(SHUFFLE_KEY_COMPARATOR);
  }

  public void addEntry(String key, String secondaryKey, String value) {
    addEntry(new ShuffleEntry(key.getBytes(), secondaryKey.getBytes(), value.getBytes()));
  }

  public void addEntry(ShuffleEntry entry) {
    NavigableMap<byte[], List<ShuffleEntry>> valuesBySecondaryKey = records.get(entry.getKey());
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
    final Iterator<Map.Entry<byte[], NavigableMap<byte[], List<ShuffleEntry>>>> recordsIter;
    final byte[] startKey;
    final byte[] endKey;
    byte[] currentKey;
    byte[] currentSecondaryKey;
    Map.Entry<byte[], NavigableMap<byte[], List<ShuffleEntry>>> currentRecord;
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
        this.currentSecondaryKey = it.currentSecondaryKey;
        this.currentValuesIter =
            it.currentRecord.getValue().get(currentSecondaryKey).listIterator(
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
        advanceSecondaryKey();
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

    private void advanceSecondaryKey() {
      NavigableMap<byte[], List<ShuffleEntry>> tailMap =
          currentRecord.getValue().tailMap(currentSecondaryKey, false /* do not include key */);
      if (tailMap.isEmpty()) {
        advanceKey();
      } else {
        currentSecondaryKey = tailMap.firstKey();
        currentValuesIter = tailMap.get(currentSecondaryKey).listIterator();
      }
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
        currentSecondaryKey = currentRecord.getValue().firstKey();
        currentValuesIter = currentRecord.getValue().get(currentSecondaryKey).listIterator();
        return;
      }
      currentKey = null;
      currentSecondaryKey = null;
      currentValuesIter = null;
    }
  }
}
