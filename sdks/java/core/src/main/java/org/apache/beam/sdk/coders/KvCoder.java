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

import static org.apache.beam.sdk.util.Structs.addBoolean;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

/**
 * A {@code KvCoder} encodes {@link KV}s.
 *
 * @param <K> the type of the keys of the KVs being transcoded
 * @param <V> the type of the values of the KVs being transcoded
 */
public class KvCoder<K, V> extends StandardCoder<KV<K, V>> {
  public static <K, V> KvCoder<K, V> of(Coder<K> keyCoder,
                                        Coder<V> valueCoder) {
    return new KvCoder<>(keyCoder, valueCoder);
  }

  @JsonCreator
  public static KvCoder<?, ?> of(
      @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
      List<Coder<?>> components) {
    checkArgument(components.size() == 2, "Expecting 2 components, got %s", components.size());
    return of(components.get(0), components.get(1));
  }

  public static <K, V> List<Object> getInstanceComponents(
      KV<K, V> exampleValue) {
    return Arrays.asList(
        exampleValue.getKey(),
        exampleValue.getValue());
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
  public void encode(KV<K, V> kv, OutputStream outStream, Context context)
      throws IOException, CoderException  {
    if (kv == null) {
      throw new CoderException("cannot encode a null KV");
    }
    Context nestedContext = context.nested();
    keyCoder.encode(kv.getKey(), outStream, nestedContext);
    valueCoder.encode(kv.getValue(), outStream, nestedContext);
  }

  @Override
  public KV<K, V> decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    Context nestedContext = context.nested();
    K key = keyCoder.decode(inStream, nestedContext);
    V value = valueCoder.decode(inStream, nestedContext);
    return KV.of(key, value);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(keyCoder, valueCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic("Key coder must be deterministic", getKeyCoder());
    verifyDeterministic("Value coder must be deterministic", getValueCoder());
  }

  @Override
  public boolean consistentWithEquals() {
    return keyCoder.consistentWithEquals() && valueCoder.consistentWithEquals();
  }

  @Override
  public Object structuralValue(KV<K, V> kv) throws Exception {
    if (consistentWithEquals()) {
      return kv;
    } else {
      return KV.of(getKeyCoder().structuralValue(kv.getKey()),
                   getValueCoder().structuralValue(kv.getValue()));
    }
  }

  @Override
  public CloudObject asCloudObject() {
    CloudObject result = super.asCloudObject();
    addBoolean(result, PropertyNames.IS_PAIR_LIKE, true);
    return result;
  }

  /**
   * Returns whether both keyCoder and valueCoder are considered not expensive.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(KV<K, V> kv, Context context) {
    return keyCoder.isRegisterByteSizeObserverCheap(kv.getKey(),
                                                    context.nested())
        && valueCoder.isRegisterByteSizeObserverCheap(kv.getValue(),
                                                      context.nested());
  }

  /**
   * Notifies ElementByteSizeObserver about the byte size of the
   * encoded value using this coder.
   */
  @Override
  public void registerByteSizeObserver(
      KV<K, V> kv, ElementByteSizeObserver observer, Context context)
      throws Exception {
    if (kv == null) {
      throw new CoderException("cannot encode a null KV");
    }
    keyCoder.registerByteSizeObserver(
        kv.getKey(), observer, context.nested());
    valueCoder.registerByteSizeObserver(
        kv.getValue(), observer, context.nested());
  }
}
