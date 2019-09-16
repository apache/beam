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
package org.apache.beam.runners.flink.translation.types;

import java.io.IOException;
import java.util.Arrays;
import org.apache.beam.sdk.coders.Coder;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

/**
 * Flink {@link org.apache.flink.api.common.typeutils.TypeComparator} for Beam values that have been
 * encoded to byte data by a {@link Coder}.
 */
public class EncodedValueComparator extends TypeComparator<byte[]> {

  /** For storing the Reference in encoded form. */
  private transient byte[] encodedReferenceKey;

  private final boolean ascending;

  public EncodedValueComparator(boolean ascending) {
    this.ascending = ascending;
  }

  @Override
  public int hash(byte[] record) {
    return Arrays.hashCode(record);
  }

  @Override
  public void setReference(byte[] toCompare) {
    this.encodedReferenceKey = toCompare;
  }

  @Override
  public boolean equalToReference(byte[] candidate) {
    if (encodedReferenceKey.length != candidate.length) {
      return false;
    }
    int len = candidate.length;
    for (int i = 0; i < len; i++) {
      if (encodedReferenceKey[i] != candidate[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int compareToReference(TypeComparator<byte[]> other) {
    // VERY IMPORTANT: compareToReference does not behave like Comparable.compare
    // the meaning of the return value is inverted.

    EncodedValueComparator otherEncodedValueComparator = (EncodedValueComparator) other;

    int len =
        Math.min(
            encodedReferenceKey.length, otherEncodedValueComparator.encodedReferenceKey.length);

    for (int i = 0; i < len; i++) {
      byte b1 = encodedReferenceKey[i];
      byte b2 = otherEncodedValueComparator.encodedReferenceKey[i];
      int result = (b1 < b2 ? -1 : (b1 == b2 ? 0 : 1));
      if (result != 0) {
        return ascending ? -result : result;
      }
    }
    int result =
        encodedReferenceKey.length - otherEncodedValueComparator.encodedReferenceKey.length;
    return ascending ? -result : result;
  }

  @Override
  public int compare(byte[] first, byte[] second) {
    int len = Math.min(first.length, second.length);
    for (int i = 0; i < len; i++) {
      byte b1 = first[i];
      byte b2 = second[i];
      int result = (b1 < b2 ? -1 : (b1 == b2 ? 0 : 1));
      if (result != 0) {
        return ascending ? result : -result;
      }
    }
    int result = first.length - second.length;
    return ascending ? result : -result;
  }

  @Override
  public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
      throws IOException {
    int lengthFirst = firstSource.readInt();
    int lengthSecond = secondSource.readInt();

    int len = Math.min(lengthFirst, lengthSecond);
    for (int i = 0; i < len; i++) {
      byte b1 = firstSource.readByte();
      byte b2 = secondSource.readByte();
      int result = (b1 < b2 ? -1 : (b1 == b2 ? 0 : 1));
      if (result != 0) {
        return ascending ? result : -result;
      }
    }

    int result = lengthFirst - lengthSecond;
    return ascending ? result : -result;
  }

  @Override
  public boolean supportsNormalizedKey() {
    // disabled because this seems to not work with some coders,
    // such as the AvroCoder
    return false;
  }

  @Override
  public boolean supportsSerializationWithKeyNormalization() {
    return false;
  }

  @Override
  public int getNormalizeKeyLen() {
    return Integer.MAX_VALUE;
  }

  @Override
  public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
    return true;
  }

  @Override
  public void putNormalizedKey(byte[] record, MemorySegment target, int offset, int numBytes) {
    final int limit = offset + numBytes;

    target.put(offset, record, 0, Math.min(numBytes, record.length));

    offset += record.length;

    while (offset < limit) {
      target.put(offset++, (byte) 0);
    }
  }

  @Override
  public void writeWithKeyNormalization(byte[] record, DataOutputView target) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] readWithKeyDenormalization(byte[] reuse, DataInputView source) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean invertNormalizedKey() {
    return !ascending;
  }

  @Override
  public TypeComparator<byte[]> duplicate() {
    return new EncodedValueComparator(ascending);
  }

  @Override
  public int extractKeys(Object record, Object[] target, int index) {
    target[index] = record;
    return 1;
  }

  @Override
  public TypeComparator[] getFlatComparators() {
    return new TypeComparator[] {this.duplicate()};
  }
}
