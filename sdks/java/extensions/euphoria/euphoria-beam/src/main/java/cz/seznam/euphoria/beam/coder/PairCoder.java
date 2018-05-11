/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.beam.coder;

import cz.seznam.euphoria.core.client.util.Pair;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

public class PairCoder<K, V> extends StructuredCoder<Pair<K, V>> {

  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;

  private PairCoder(Coder<K> keyCoder, Coder<V> valueCoder) {
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
  }

  public static <K, V> PairCoder<K, V> of(Coder<K> keyCoder, Coder<V> valueCoder) {
    return new PairCoder<>(keyCoder, valueCoder);
  }

  @Override
  public void encode(Pair<K, V> value, OutputStream outStream) throws IOException {
    keyCoder.encode(value.getFirst(), outStream);
    valueCoder.encode(value.getSecond(), outStream);
  }

  @Override
  public Pair<K, V> decode(InputStream inStream) throws IOException {
    return Pair.of(keyCoder.decode(inStream), valueCoder.decode(inStream));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(keyCoder, valueCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(this, "Key coder must be deterministic", keyCoder);
    verifyDeterministic(this, "Value coder must be deterministic", valueCoder);
  }

  @Override
  public boolean consistentWithEquals() {
    return keyCoder.consistentWithEquals() && valueCoder.consistentWithEquals();
  }

  @Override
  public Object structuralValue(Pair<K, V> pair) {
    if (consistentWithEquals()) {
      return pair;
    }
    return Pair.of(
        keyCoder.structuralValue(pair.getFirst()), valueCoder.structuralValue(pair.getSecond()));
  }

  @Override
  public boolean isRegisterByteSizeObserverCheap(Pair<K, V> pair) {
    return keyCoder.isRegisterByteSizeObserverCheap(pair.getFirst())
        && valueCoder.isRegisterByteSizeObserverCheap(pair.getSecond());
  }

  @Override
  public void registerByteSizeObserver(Pair<K, V> pair, ElementByteSizeObserver observer)
      throws Exception {
    if (pair == null) {
      throw new CoderException("Can not encode a null pair");
    }
    keyCoder.registerByteSizeObserver(pair.getFirst(), observer);
    valueCoder.registerByteSizeObserver(pair.getSecond(), observer);
  }

  @Override
  public TypeDescriptor<Pair<K, V>> getEncodedTypeDescriptor() {
    return new TypeDescriptor<Pair<K, V>>() {}.where(
            new TypeParameter<K>() {}, keyCoder.getEncodedTypeDescriptor())
        .where(new TypeParameter<V>() {}, valueCoder.getEncodedTypeDescriptor());
  }
}
