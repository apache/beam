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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.BooleanCoder;
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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/** {@link Coder} for {@link KafkaRecord}. */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class NullableKeyKafkaRecordCoder<K, V> extends StructuredCoder<KafkaRecord<K, V>> {

  private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
  private static final VarLongCoder longCoder = VarLongCoder.of();
  private static final VarIntCoder intCoder = VarIntCoder.of();
  private static final IterableCoder headerCoder =
      IterableCoder.of(KvCoder.of(stringCoder, ByteArrayCoder.of()));
  private static final BooleanCoder nullKeyBooleanCoder = BooleanCoder.of();
  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;

  public static <K, V> NullableKeyKafkaRecordCoder<K, V> of(
      Coder<K> keyCoder, Coder<V> valueCoder) {
    return new NullableKeyKafkaRecordCoder<>(keyCoder, valueCoder);
  }

  public NullableKeyKafkaRecordCoder(Coder<K> keyCoder, Coder<V> valueCoder) {
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
  }

  @Override
  public void encode(KafkaRecord<K, V> value, OutputStream outStream) throws IOException {
    stringCoder.encode(value.getTopic(), outStream);
    intCoder.encode(value.getPartition(), outStream);
    longCoder.encode(value.getOffset(), outStream);
    longCoder.encode(value.getTimestamp(), outStream);
    intCoder.encode(value.getTimestampType().ordinal(), outStream);
    headerCoder.encode(toIterable(value), outStream);
    if (value.getKV().getKey() != null) {
      nullKeyBooleanCoder.encode(false, outStream);
      keyCoder.encode(value.getKV().getKey(), outStream);
    } else {
      nullKeyBooleanCoder.encode(true, outStream);
    }
    valueCoder.encode(value.getKV().getValue(), outStream);
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
        nullKeyBooleanCoder.decode(inStream) ? null : keyCoder.decode(inStream),
        valueCoder.decode(inStream));
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

  private Iterable<KV<String, byte[]>> toIterable(KafkaRecord record) {
    if (!ConsumerSpEL.hasHeaders()) {
      return Collections.emptyList();
    }

    List<KV<String, byte[]>> vals = new ArrayList<>();
    for (Header header : record.getHeaders()) {
      vals.add(KV.of(header.key(), header.value()));
    }
    return vals;
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(keyCoder, valueCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(this, "Key must be deterministic", keyCoder);
    verifyDeterministic(this, "Value must be deterministic", valueCoder);
  }

  @Override
  public boolean isRegisterByteSizeObserverCheap(KafkaRecord<K, V> value) {
    return keyCoder.isRegisterByteSizeObserverCheap(value.getKV().getKey())
        && valueCoder.isRegisterByteSizeObserverCheap(value.getKV().getValue());
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
          value.getKV().getKey(),
          value.getKV().getValue());
    }
  }

  @Override
  public boolean consistentWithEquals() {
    return keyCoder.consistentWithEquals() && valueCoder.consistentWithEquals();
  }
}
