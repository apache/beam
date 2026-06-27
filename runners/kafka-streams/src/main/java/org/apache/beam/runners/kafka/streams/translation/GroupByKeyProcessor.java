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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/** Processor for GroupByKey. */
class GroupByKeyProcessor
    implements Processor<
        byte[],
        KStreamsPayload<KV<Object, Object>>,
        byte[],
        KStreamsPayload<KV<Object, Iterable<Object>>>> {

  private final String stateStoreName;
  private final String transformId;
  private final Coder<List<WindowedValue<KV<Object, Object>>>> listCoder;

  private ProcessorContext<byte[], KStreamsPayload<KV<Object, Iterable<Object>>>> context;
  private KeyValueStore<byte[], byte[]> stateStore;

  GroupByKeyProcessor(
      String stateStoreName,
      String transformId,
      Coder<WindowedValue<KV<Object, Object>>> inputCoder) {
    this.stateStoreName = stateStoreName;
    this.transformId = transformId;
    this.listCoder = ListCoder.of(inputCoder);
  }

  @Override
  public void init(
      ProcessorContext<byte[], KStreamsPayload<KV<Object, Iterable<Object>>>> context) {
    this.context = context;
    this.stateStore = context.getStateStore(stateStoreName);
  }

  @Override
  public void process(Record<byte[], KStreamsPayload<KV<Object, Object>>> record) {
    KStreamsPayload<KV<Object, Object>> payload = record.value();

    if (payload.isData()) {
      byte[] keyBytes = record.key();
      byte[] existingBytes = stateStore.get(keyBytes);
      List<WindowedValue<KV<Object, Object>>> list;
      if (existingBytes == null) {
        list = new ArrayList<>();
      } else {
        try {
          list = listCoder.decode(new ByteArrayInputStream(existingBytes));
        } catch (IOException e) {
          throw new RuntimeException("Failed to decode buffered GroupByKey state", e);
        }
      }
      list.add(payload.getData());
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      try {
        listCoder.encode(list, os);
      } catch (IOException e) {
        throw new RuntimeException("Failed to encode buffered GroupByKey state", e);
      }
      stateStore.put(keyBytes, os.toByteArray());
    } else {
      WatermarkPayload watermark = payload.asWatermark();
      if (watermark.getWatermarkMillis() == BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
        try (KeyValueIterator<byte[], byte[]> iterator = stateStore.all()) {
          while (iterator.hasNext()) {
            org.apache.kafka.streams.KeyValue<byte[], byte[]> kv = iterator.next();
            List<WindowedValue<KV<Object, Object>>> buffered;
            try {
              buffered = listCoder.decode(new ByteArrayInputStream(kv.value));
            } catch (IOException e) {
              throw new RuntimeException("Failed to decode buffered GroupByKey state on emit", e);
            }
            if (!buffered.isEmpty()) {
              List<Object> values = new ArrayList<>();
              for (WindowedValue<KV<Object, Object>> wv : buffered) {
                values.add(wv.getValue().getValue());
              }
              Object key = buffered.get(0).getValue().getKey();
              WindowedValue<KV<Object, Iterable<Object>>> outWv =
                  WindowedValues.valueInGlobalWindow(KV.of(key, values));
              context.forward(
                  new Record<>(kv.key, KStreamsPayload.data(outWv), record.timestamp()));
            }
          }
        }
        // Since we fired everything for the global window, we can optionally clear the store here.
        // But the pipeline is finishing.

        // Forward the watermark downstream
        context.forward(
            new Record<>(
                record.key(),
                KStreamsPayload.watermark(
                    watermark.getWatermarkMillis(),
                    watermark.getSourcePartition(),
                    watermark.getTotalSourcePartitions()),
                record.timestamp()));
      }
    }
  }
}
