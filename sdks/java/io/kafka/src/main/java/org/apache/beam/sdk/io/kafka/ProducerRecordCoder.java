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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/** {@link Coder} for {@link ProducerRecord}. */
public class ProducerRecordCoder<K, V> extends StructuredCoder<ProducerRecord<K, V>> {
  private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
  private static final VarLongCoder longCoder = VarLongCoder.of();
  private static final VarIntCoder intCoder = VarIntCoder.of();
  private static final IterableCoder headerCoder =
      IterableCoder.of(KvCoder.of(stringCoder, ByteArrayCoder.of()));

  private final KvCoder<K, V> kvCoder;

  public static <K, V> ProducerRecordCoder<K, V> of(Coder<K> keyCoder, Coder<V> valueCoder) {
    return new ProducerRecordCoder<>(keyCoder, valueCoder);
  }

  public ProducerRecordCoder(Coder<K> keyCoder, Coder<V> valueCoder) {
    this.kvCoder = KvCoder.of(keyCoder, valueCoder);
  }

  @Override
  public void encode(ProducerRecord<K, V> value, OutputStream outStream) throws IOException {
    stringCoder.encode(value.topic(), outStream);
    intCoder.encode(value.partition() != null ? value.partition() : -1, outStream);
    longCoder.encode(value.timestamp() != null ? value.timestamp() : Long.MAX_VALUE, outStream);
    headerCoder.encode(toIterable(value), outStream);
    kvCoder.encode(KV.of(value.key(), value.value()), outStream);
  }

  @Override
  public ProducerRecord<K, V> decode(InputStream inStream) throws IOException {
    String topic = stringCoder.decode(inStream);
    Integer partition = intCoder.decode(inStream);
    if (partition == -1) {
      partition = null;
    }

    Long timestamp = longCoder.decode(inStream);
    if (timestamp == Long.MAX_VALUE) {
      timestamp = null;
    }

    Headers headers = (Headers) toHeaders(headerCoder.decode(inStream));
    KV<K, V> kv = kvCoder.decode(inStream);
    if (ConsumerSpEL.hasHeaders()) {
      return new ProducerRecord<>(topic, partition, timestamp, kv.getKey(), kv.getValue(), headers);
    }
    return new ProducerRecord<>(topic, partition, timestamp, kv.getKey(), kv.getValue());
  }

  private Object toHeaders(Iterable<KV<String, byte[]>> records) {
    if (!ConsumerSpEL.hasHeaders()) {
      return null;
    }

    // ConsumerRecord is used to simply create a list of headers
    ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("", 0, 0L, "", "");
    records.forEach(kv -> consumerRecord.headers().add(kv.getKey(), kv.getValue()));
    return consumerRecord.headers();
  }

  private Iterable<KV<String, byte[]>> toIterable(ProducerRecord record) {
    if (!ConsumerSpEL.hasHeaders()) {
      return Collections.emptyList();
    }
    List<KV<String, byte[]>> vals = new ArrayList<>();
    for (Header header : record.headers()) {
      vals.add(KV.of(header.key(), header.value()));
    }
    return vals;
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
  public boolean isRegisterByteSizeObserverCheap(ProducerRecord<K, V> value) {
    return kvCoder.isRegisterByteSizeObserverCheap(KV.of(value.key(), value.value()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object structuralValue(ProducerRecord<K, V> value) {
    if (consistentWithEquals()) {
      return value;
    } else {
      if (!ConsumerSpEL.hasHeaders()) {
        return new ProducerRecord<>(
            value.topic(), value.partition(), value.timestamp(), value.key(), value.value());
      } else {
        return new ProducerRecord<>(
            value.topic(),
            value.partition(),
            value.timestamp(),
            value.key(),
            value.value(),
            value.headers());
      }
    }
  }

  @Override
  public boolean consistentWithEquals() {
    return kvCoder.consistentWithEquals();
  }
}
