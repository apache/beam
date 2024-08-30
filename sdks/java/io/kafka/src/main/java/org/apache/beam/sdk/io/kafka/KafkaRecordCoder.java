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
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.checkerframework.checker.nullness.qual.Nullable;

/** {@link Coder} for {@link KafkaRecord}. */
public class KafkaRecordCoder<K, V> extends StructuredCoder<KafkaRecord<K, V>> {

  private static final Coder<String> stringCoder = StringUtf8Coder.of();
  private static final Coder<Long> longCoder = VarLongCoder.of();
  private static final Coder<Integer> intCoder = VarIntCoder.of();
  private static final Coder<Iterable<KV<String, byte[]>>> headerCoder =
      IterableCoder.of(KvCoder.of(stringCoder, NullableCoder.of(ByteArrayCoder.of())));

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
    headerCoder.encode(toIterable(value), outStream);
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
        (Headers) toHeaders(headerCoder.decode(inStream)),
        kvCoder.decode(inStream));
  }

  private @Nullable Object toHeaders(Iterable<KV<String, byte[]>> records) {
    if (!ConsumerSpEL.hasHeaders()) {
      return null;
    }

    // ConsumerRecord is used to simply create a list of headers
    ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("", 0, 0L, "", "");
    records.forEach(kv -> consumerRecord.headers().add(kv.getKey(), kv.getValue()));
    return consumerRecord.headers();
  }

  private Iterable<KV<String, byte[]>> toIterable(KafkaRecord<K, V> record) {
    if (!ConsumerSpEL.hasHeaders()) {
      return Collections.emptyList();
    }

    List<KV<String, byte[]>> vals = new ArrayList<>();
    if (record.getHeaders() != null) {
      for (Header header : record.getHeaders()) {
        vals.add(KV.of(header.key(), header.value()));
      }
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
  public boolean isRegisterByteSizeObserverCheap(KafkaRecord<K, V> value) {
    return kvCoder.isRegisterByteSizeObserverCheap(value.getKV());
    // TODO : do we have to implement getEncodedSize()?
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
          !ConsumerSpEL.hasHeaders() ? null : value.getHeaders(),
          (KV<Object, Object>) kvCoder.structuralValue(value.getKV()));
    }
  }

  @Override
  public boolean consistentWithEquals() {
    return kvCoder.consistentWithEquals();
  }
}
