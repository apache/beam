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
package org.apache.beam.runners.kafka.streams.translation;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Re-keys a {@code KV}-valued stream by the Beam key so Kafka Streams shuffles by it.
 *
 * <p>This is not GroupByKey-specific: any transform that needs the values of a key co-located on
 * one partition uses it — GroupByKey today, and stateful ParDo later. For a data record it sets the
 * Kafka record key to the encoded Beam key (taken from the {@code KV}), so the downstream
 * repartition sink co-locates every value of a key. Watermark reports are forwarded unchanged — the
 * {@link GroupByKeyBroadcastPartitioner} fans them out to all partitions so every downstream task
 * can fire.
 */
class ShuffleByKeyProcessor
    implements Processor<byte[], KStreamsPayload<?>, byte[], KStreamsPayload<?>> {

  private final Coder<Object> keyCoder;
  private @Nullable ProcessorContext<byte[], KStreamsPayload<?>> context;

  ShuffleByKeyProcessor(Coder<Object> keyCoder) {
    this.keyCoder = keyCoder;
  }

  @Override
  public void init(ProcessorContext<byte[], KStreamsPayload<?>> context) {
    this.context = context;
  }

  @Override
  public void process(Record<byte[], KStreamsPayload<?>> record) {
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = checkInitialized(context);
    KStreamsPayload<?> payload = record.value();
    if (payload.isData()) {
      Object element = payload.getData().getValue();
      if (element == null) {
        throw new IllegalStateException("shuffle data element must not be null");
      }
      Object key = ((KV<?, ?>) element).getKey();
      if (key == null) {
        throw new IllegalStateException("shuffle key must not be null");
      }
      byte[] encodedKey;
      try {
        encodedKey = CoderUtils.encodeToByteArray(keyCoder, key);
      } catch (CoderException e) {
        throw new RuntimeException("Failed to encode shuffle key", e);
      }
      ctx.forward(record.withKey(encodedKey));
    } else {
      // Watermark report: forward as-is; the sink's partitioner broadcasts it to all partitions.
      ctx.forward(record);
    }
  }

  private static <T> T checkInitialized(@Nullable T value) {
    if (value == null) {
      throw new IllegalStateException("ShuffleByKeyProcessor used before init()");
    }
    return value;
  }
}
