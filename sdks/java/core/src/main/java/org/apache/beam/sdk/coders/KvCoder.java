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
package org.apache.beam.sdk.coders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@code KvCoder} encodes {@link KV}s.
 *
 * @param <K> the type of the keys of the KVs being transcoded
 * @param <V> the type of the values of the KVs being transcoded
 */
public class KvCoder<K extends @Nullable Object, V extends @Nullable Object>
    extends StructuredCoder<KV<K, V>> {
  public static <K, V> KvCoder<K, V> of(Coder<K> keyCoder, Coder<V> valueCoder) {
    return new KvCoder<>(keyCoder, valueCoder);
  }

  public Coder<K> getKeyCoder() {
    return keyCoder;
  }

  public Coder<V> getValueCoder() {
    return valueCoder;
  }

  /////////////////////////////////////////////////////////////////////////////

  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;

  private KvCoder(Coder<K> keyCoder, Coder<V> valueCoder) {
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
  }

  @Override
  public void encode(KV<K, V> kv, OutputStream outStream) throws IOException, CoderException {
    encode(kv, outStream, Context.NESTED);
  }

  @Override
  public void encode(KV<K, V> kv, OutputStream outStream, Context context)
      throws IOException, CoderException {
    if (kv == null) {
      throw new CoderException("cannot encode a null KV");
    }
    keyCoder.encode(kv.getKey(), outStream);
    valueCoder.encode(kv.getValue(), outStream, context);
  }

  @Override
  public KV<K, V> decode(InputStream inStream) throws IOException, CoderException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public KV<K, V> decode(InputStream inStream, Context context) throws IOException, CoderException {
    K key = keyCoder.decode(inStream);
    V value = valueCoder.decode(inStream, context);
    return KV.of(key, value);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(keyCoder, valueCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(this, "Key coder must be deterministic", getKeyCoder());
    verifyDeterministic(this, "Value coder must be deterministic", getValueCoder());
  }

  @Override
  public boolean consistentWithEquals() {
    return keyCoder.consistentWithEquals() && valueCoder.consistentWithEquals();
  }

  @Override
  public Object structuralValue(KV<K, V> kv) {
    if (consistentWithEquals()) {
      return kv;
    } else {
      return KV.of(
          getKeyCoder().structuralValue(kv.getKey()),
          getValueCoder().structuralValue(kv.getValue()));
    }
  }

  /** Returns whether both keyCoder and valueCoder are considered not expensive. */
  @Override
  public boolean isRegisterByteSizeObserverCheap(KV<K, V> kv) {
    return keyCoder.isRegisterByteSizeObserverCheap(kv.getKey())
        && valueCoder.isRegisterByteSizeObserverCheap(kv.getValue());
  }

  /** Notifies ElementByteSizeObserver about the byte size of the encoded value using this coder. */
  @Override
  public void registerByteSizeObserver(KV<K, V> kv, ElementByteSizeObserver observer)
      throws Exception {
    if (kv == null) {
      throw new CoderException("cannot encode a null KV");
    }
    keyCoder.registerByteSizeObserver(kv.getKey(), observer);
    valueCoder.registerByteSizeObserver(kv.getValue(), observer);
  }

  @Override
  public TypeDescriptor<KV<K, V>> getEncodedTypeDescriptor() {
    return new TypeDescriptor<KV<K, V>>() {}.where(
            new TypeParameter<K>() {}, keyCoder.getEncodedTypeDescriptor())
        .where(new TypeParameter<V>() {}, valueCoder.getEncodedTypeDescriptor());
  }
}
