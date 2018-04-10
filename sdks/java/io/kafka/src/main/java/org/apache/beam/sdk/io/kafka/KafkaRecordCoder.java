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
package org.apache.beam.sdk.io.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.values.KV;

/**
 * {@link Coder} for {@link KafkaRecord}.
 */
public class KafkaRecordCoder<K, V> extends StructuredCoder<KafkaRecord<K, V>> {

  private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
  private static final VarLongCoder longCoder = VarLongCoder.of();
  private static final VarIntCoder intCoder = VarIntCoder.of();

  private final KvCoder<K, V> kvCoder;

  public static <K, V> KafkaRecordCoder<K, V> of(Coder<K> keyCoder, Coder<V> valueCoder) {
    return new KafkaRecordCoder<>(keyCoder, valueCoder);
  }

  public KafkaRecordCoder(Coder<K> keyCoder, Coder<V> valueCoder) {
    this.kvCoder = KvCoder.of(keyCoder, valueCoder);
  }

  @Override
  public void encode(KafkaRecord<K, V> value, OutputStream outStream) throws IOException {
    stringCoder.encode(value.getTopic(), outStream);
    intCoder.encode(value.getPartition(), outStream);
    longCoder.encode(value.getOffset(), outStream);
    longCoder.encode(value.getTimestamp(), outStream);
    intCoder.encode(value.getTimestampType().ordinal(), outStream);
    kvCoder.encode(value.getKV(), outStream);
  }

  @Override
  public KafkaRecord<K, V> decode(InputStream inStream) throws IOException {
    return new KafkaRecord<>(
        stringCoder.decode(inStream),
        intCoder.decode(inStream),
        longCoder.decode(inStream),
        longCoder.decode(inStream),
        KafkaTimestampType.forOrdinal(intCoder.decode(inStream)),
        kvCoder.decode(inStream));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return kvCoder.getCoderArguments();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    kvCoder.verifyDeterministic();
  }

  @Override
  public boolean isRegisterByteSizeObserverCheap(KafkaRecord<K, V> value) {
    return kvCoder.isRegisterByteSizeObserverCheap(value.getKV());
    //TODO : do we have to implement getEncodedSize()?
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object structuralValue(KafkaRecord<K, V> value) {
    if (consistentWithEquals()) {
      return value;
    } else {
      return new KafkaRecord<>(
          value.getTopic(),
          value.getPartition(),
          value.getOffset(),
          value.getTimestamp(),
          value.getTimestampType(),
          (KV<Object, Object>) kvCoder.structuralValue(value.getKV()));
    }
  }

  @Override
  public boolean consistentWithEquals() {
    return kvCoder.consistentWithEquals();
  }
}
