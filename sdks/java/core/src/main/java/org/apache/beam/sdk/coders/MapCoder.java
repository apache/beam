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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

import com.google.common.collect.Maps;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A {@link Coder} for {@link Map Maps} that encodes them according to provided
 * coders for keys and values.
 *
 * @param <K> the type of the keys of the KVs being transcoded
 * @param <V> the type of the values of the KVs being transcoded
 */
public class MapCoder<K, V> extends StandardCoder<Map<K, V>> {
  /**
   * Produces a MapCoder with the given keyCoder and valueCoder.
   */
  public static <K, V> MapCoder<K, V> of(
      Coder<K> keyCoder,
      Coder<V> valueCoder) {
    return new MapCoder<>(keyCoder, valueCoder);
  }

  @JsonCreator
  public static MapCoder<?, ?> of(
      @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
      List<Coder<?>> components) {
    checkArgument(components.size() == 2, "Expecting 2 components, got %s", components.size());
    return of((Coder<?>) components.get(0), (Coder<?>) components.get(1));
  }

  /**
   * Returns the key and value for an arbitrary element of this map,
   * if it is non-empty, otherwise returns {@code null}.
   */
   public static <K, V> List<Object> getInstanceComponents(
       Map<K, V> exampleValue) {
     for (Map.Entry<K, V> entry : exampleValue.entrySet()) {
       return Arrays.asList(entry.getKey(), entry.getValue());
     }
     return null;
   }

  public Coder<K> getKeyCoder() {
    return keyCoder;
  }

  public Coder<V> getValueCoder() {
    return valueCoder;
  }

  /////////////////////////////////////////////////////////////////////////////

  Coder<K> keyCoder;
  Coder<V> valueCoder;

  MapCoder(Coder<K> keyCoder, Coder<V> valueCoder) {
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
  }

  @Override
  public void encode(
      Map<K, V> map,
      OutputStream outStream,
      Context context)
      throws IOException, CoderException  {
    if (map == null) {
      throw new CoderException("cannot encode a null Map");
    }
    DataOutputStream dataOutStream = new DataOutputStream(outStream);
    dataOutStream.writeInt(map.size());
    for (Entry<K, V> entry : map.entrySet()) {
      keyCoder.encode(entry.getKey(), outStream, context.nested());
      valueCoder.encode(entry.getValue(), outStream, context.nested());
    }
    dataOutStream.flush();
  }

  @Override
  public Map<K, V> decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    DataInputStream dataInStream = new DataInputStream(inStream);
    int size = dataInStream.readInt();
    Map<K, V> retval = Maps.newHashMapWithExpectedSize(size);
    for (int i = 0; i < size; ++i) {
      K key = keyCoder.decode(inStream, context.nested());
      V value = valueCoder.decode(inStream, context.nested());
      retval.put(key, value);
    }
    return retval;
  }

  /**
   * {@inheritDoc}
   *
   * @return a {@link List} containing the key coder at index 0 at the and value coder at index 1.
   */
  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(keyCoder, valueCoder);
  }

  /**
   * {@inheritDoc}
   *
   * @throws NonDeterministicException always. Not all maps have a deterministic encoding.
   * For example, {@code HashMap} comparison does not depend on element order, so
   * two {@code HashMap} instances may be equal but produce different encodings.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this,
        "Ordering of entries in a Map may be non-deterministic.");
  }

  @Override
  public void registerByteSizeObserver(
      Map<K, V> map, ElementByteSizeObserver observer, Context context)
      throws Exception {
    observer.update(4L);
    for (Entry<K, V> entry : map.entrySet()) {
      keyCoder.registerByteSizeObserver(
          entry.getKey(), observer, context.nested());
      valueCoder.registerByteSizeObserver(
          entry.getValue(), observer, context.nested());
    }
  }
}
