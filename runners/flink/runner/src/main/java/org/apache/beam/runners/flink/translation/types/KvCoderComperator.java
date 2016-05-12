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

import org.apache.beam.runners.flink.translation.wrappers.DataInputViewWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Flink {@link TypeComparator} for {@link KvCoder}. We have a special comparator
 * for {@link KV} that always compares on the key only.
 */
public class KvCoderComperator <K, V> extends TypeComparator<WindowedValue<KV<K, V>>> {
  
  private final WindowedValue.WindowedValueCoder<KV<K, V>> coder;
  private final Coder<K> keyCoder;

  // We use these for internal encoding/decoding for creating copies and comparing
  // serialized forms using a Coder
  private transient InspectableByteArrayOutputStream buffer1;
  private transient InspectableByteArrayOutputStream buffer2;

  // For storing the Reference in encoded form
  private transient InspectableByteArrayOutputStream referenceBuffer;


  // For deserializing the key
  private transient DataInputViewWrapper inputWrapper;

  public KvCoderComperator(WindowedValue.WindowedValueCoder<KV<K, V>> coder) {
    this.coder = coder;
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) coder.getValueCoder();
    this.keyCoder = kvCoder.getKeyCoder();

    buffer1 = new InspectableByteArrayOutputStream();
    buffer2 = new InspectableByteArrayOutputStream();
    referenceBuffer = new InspectableByteArrayOutputStream();

    inputWrapper = new DataInputViewWrapper(null);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();

    buffer1 = new InspectableByteArrayOutputStream();
    buffer2 = new InspectableByteArrayOutputStream();
    referenceBuffer = new InspectableByteArrayOutputStream();

    inputWrapper = new DataInputViewWrapper(null);
  }

  @Override
  public int hash(WindowedValue<KV<K, V>> record) {
    K key = record.getValue().getKey();
    if (key != null) {
      return key.hashCode();
    } else {
      return 0;
    }
  }

  @Override
  public void setReference(WindowedValue<KV<K, V>> toCompare) {
    referenceBuffer.reset();
    try {
      keyCoder.encode(toCompare.getValue().getKey(), referenceBuffer, Coder.Context.OUTER);
    } catch (IOException e) {
      throw new RuntimeException("Could not set reference " + toCompare + ": " + e);
    }
  }

  @Override
  public boolean equalToReference(WindowedValue<KV<K, V>> candidate) {
    try {
      buffer2.reset();
      keyCoder.encode(candidate.getValue().getKey(), buffer2, Coder.Context.OUTER);
      byte[] arr = referenceBuffer.getBuffer();
      byte[] arrOther = buffer2.getBuffer();
      if (referenceBuffer.size() != buffer2.size()) {
        return false;
      }
      int len = buffer2.size();
      for (int i = 0; i < len; i++) {
        if (arr[i] != arrOther[i]) {
          return false;
        }
      }
      return true;
    } catch (IOException e) {
      throw new RuntimeException("Could not compare reference.", e);
    }
  }

  @Override
  public int compareToReference(TypeComparator<WindowedValue<KV<K, V>>> other) {
    InspectableByteArrayOutputStream otherReferenceBuffer =
        ((KvCoderComperator<K, V>) other).referenceBuffer;

    byte[] arr = referenceBuffer.getBuffer();
    byte[] arrOther = otherReferenceBuffer.getBuffer();
    if (referenceBuffer.size() != otherReferenceBuffer.size()) {
      return referenceBuffer.size() - otherReferenceBuffer.size();
    }
    int len = referenceBuffer.size();
    for (int i = 0; i < len; i++) {
      if (arr[i] != arrOther[i]) {
        return arr[i] - arrOther[i];
      }
    }
    return 0;
  }


  @Override
  public int compare(WindowedValue<KV<K, V>> first, WindowedValue<KV<K, V>> second) {
    try {
      buffer1.reset();
      buffer2.reset();
      keyCoder.encode(first.getValue().getKey(), buffer1, Coder.Context.OUTER);
      keyCoder.encode(second.getValue().getKey(), buffer2, Coder.Context.OUTER);
      byte[] arr = buffer1.getBuffer();
      byte[] arrOther = buffer2.getBuffer();
      if (buffer1.size() != buffer2.size()) {
        return buffer1.size() - buffer2.size();
      }
      int len = buffer1.size();
      for (int i = 0; i < len; i++) {
        if (arr[i] != arrOther[i]) {
          return arr[i] - arrOther[i];
        }
      }
      return 0;
    } catch (IOException e) {
      throw new RuntimeException("Could not compare reference.", e);
    }
  }

  @Override
  public int compareSerialized(
      DataInputView firstSource,
      DataInputView secondSource) throws IOException {
    inputWrapper.setInputView(firstSource);
    WindowedValue<KV<K, V>> first = coder.decode(inputWrapper, Coder.Context.NESTED);
    inputWrapper.setInputView(secondSource);
    WindowedValue<KV<K, V>> second = coder.decode(inputWrapper, Coder.Context.NESTED);
    return compare(first, second);
  }

  @Override
  public boolean supportsNormalizedKey() {
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
  public void putNormalizedKey(
      WindowedValue<KV<K, V>> record,
      MemorySegment target,
      int offset,
      int numBytes) {

    buffer1.reset();
    try {
      keyCoder.encode(record.getValue().getKey(), buffer1, Coder.Context.NESTED);
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not serializer " + record + " using coder " + coder + ": " + e);
    }
    final byte[] data = buffer1.getBuffer();
    final int limit = offset + numBytes;

    int numBytesPut = Math.min(numBytes, buffer1.size());

    target.put(offset, data, 0, numBytesPut);

    offset += numBytesPut;

    while (offset < limit) {
      target.put(offset++, (byte) 0);
    }
  }

  @Override
  public void writeWithKeyNormalization(
      WindowedValue<KV<K, V>> record,
      DataOutputView target) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public WindowedValue<KV<K, V>> readWithKeyDenormalization(
      WindowedValue<KV<K, V>> reuse,
      DataInputView source) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean invertNormalizedKey() {
    return false;
  }

  @Override
  public TypeComparator<WindowedValue<KV<K, V>>> duplicate() {
    return new KvCoderComperator<>(coder);
  }

  @Override
  public int extractKeys(Object record, Object[] target, int index) {
    WindowedValue<KV<K, V>> kv = (WindowedValue<KV<K, V>>) record;
    K k = kv.getValue().getKey();
    target[index] = k;
    return 1;
  }

  @Override
  public TypeComparator[] getFlatComparators() {
    return new TypeComparator[] {new CoderComparator<>(keyCoder)};
  }
}
