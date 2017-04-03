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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.values.KV;

/**
 * {@link Coder} for {@link KafkaRecord}.
 */
public class KafkaRecordCoder<K, V> extends StandardCoder<KafkaRecord<K, V>> {

  private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
  private static final VarLongCoder longCoder = VarLongCoder.of();
  private static final VarIntCoder intCoder = VarIntCoder.of();

  private final KvCoder<K, V> kvCoder;

  @JsonCreator
  public static KafkaRecordCoder<?, ?> of(@JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
                                          List<Coder<?>> components) {
    KvCoder<?, ?> kvCoder = KvCoder.of(components);
    return of(kvCoder.getKeyCoder(), kvCoder.getValueCoder());
  }

  public static <K, V> KafkaRecordCoder<K, V> of(Coder<K> keyCoder, Coder<V> valueCoder) {
    return new KafkaRecordCoder<K, V>(keyCoder, valueCoder);
  }

  public KafkaRecordCoder(Coder<K> keyCoder, Coder<V> valueCoder) {
    this.kvCoder = KvCoder.of(keyCoder, valueCoder);
  }

  @Override
  public void encode(KafkaRecord<K, V> value, OutputStream outStream, Context context)
                         throws CoderException, IOException {
    Context nested = context.nested();
    stringCoder.encode(value.getTopic(), outStream, nested);
    intCoder.encode(value.getPartition(), outStream, nested);
    longCoder.encode(value.getOffset(), outStream, nested);
    longCoder.encode(value.getTimestamp(), outStream, nested);
    kvCoder.encode(value.getKV(), outStream, context);
  }

  @Override
  public KafkaRecord<K, V> decode(InputStream inStream, Context context)
                                      throws CoderException, IOException {
    Context nested = context.nested();
    return new KafkaRecord<K, V>(
        stringCoder.decode(inStream, nested),
        intCoder.decode(inStream, nested),
        longCoder.decode(inStream, nested),
        longCoder.decode(inStream, nested),
        kvCoder.decode(inStream, context));
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
  public boolean isRegisterByteSizeObserverCheap(KafkaRecord<K, V> value, Context context) {
    return kvCoder.isRegisterByteSizeObserverCheap(value.getKV(), context);
    //TODO : do we have to implement getEncodedSize()?
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object structuralValue(KafkaRecord<K, V> value) throws Exception {
    if (consistentWithEquals()) {
      return value;
    } else {
      return new KafkaRecord<Object, Object>(
          value.getTopic(),
          value.getPartition(),
          value.getOffset(),
          value.getTimestamp(),
          (KV<Object, Object>) kvCoder.structuralValue(value.getKV()));
    }
  }

  @Override
  public boolean consistentWithEquals() {
    return kvCoder.consistentWithEquals();
  }
}
