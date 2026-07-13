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

import java.util.Set;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Kafka Streams {@link Processor} implementing Beam's {@code Flatten} primitive ({@code
 * beam:transform:flatten:v1}): the union of N input PCollections into one output PCollection.
 *
 * <p><b>Data</b> records are forwarded straight through unchanged — the merge of the N parents'
 * data streams <em>is</em> the flatten.
 *
 * <p><b>Watermark</b> reports are where Flatten does real work, and it owns its output watermark
 * the same way GroupByKey does: it runs a {@link WatermarkManager} over its inputs, forwards its
 * own watermark only when the {@code min()} across them advances, and stamps that as a single
 * source ({@code 0 of 1}) to its downstream. This holds the output watermark back until
 * <em>every</em> input branch has reported, so a downstream GroupByKey does not fire before all
 * flattened branches are drained.
 *
 * <p>The {@link WatermarkAggregator} tells the input branches apart by the transform id each
 * branch's producer stamps on its watermark (Kafka Streams does not tell a processor which parent
 * forwarded a record). Each producer stamps its own identity regardless of who consumes it, so a
 * PCollection feeding several Flattens reports one identity and every Flatten still waits only for
 * the upstream transforms it expects — the set handed to it at construction from the pipeline
 * graph.
 */
class FlattenProcessor
    implements Processor<byte[], KStreamsPayload<?>, byte[], KStreamsPayload<?>> {

  // This transform's own id, stamped on every watermark it forwards downstream.
  private final String transformId;
  // Computes the output watermark as min() over the upstream transforms' reports, holding until
  // every partition of every expected upstream transform has reported (see WatermarkAggregator).
  private final WatermarkAggregator watermarkAggregator;
  // The last watermark actually forwarded downstream, so we only forward when it advances.
  private Instant lastForwardedWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  private @Nullable ProcessorContext<byte[], KStreamsPayload<?>> context;

  /**
   * @param transformId this Flatten's own transform id, stamped on the watermarks it emits
   * @param upstreamTransformIds the producers of this Flatten's input PCollections (known from the
   *     pipeline graph), whose reports the {@link WatermarkAggregator} waits for
   */
  FlattenProcessor(String transformId, Set<String> upstreamTransformIds) {
    this.transformId = transformId;
    this.watermarkAggregator = new WatermarkAggregator(upstreamTransformIds);
  }

  @Override
  public void init(ProcessorContext<byte[], KStreamsPayload<?>> context) {
    this.context = context;
  }

  @Override
  public void process(Record<byte[], KStreamsPayload<?>> record) {
    KStreamsPayload<?> payload = record.value();
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = checkInitialized(context);
    if (!payload.isWatermark()) {
      // Data: the union of the parents' data streams is the flatten — forward unchanged.
      ctx.forward(record);
      return;
    }
    watermarkAggregator.observe(payload.asWatermark());
    Instant advanced = watermarkAggregator.advance();
    if (advanced.isAfter(lastForwardedWatermark)) {
      lastForwardedWatermark = advanced;
      // Stamped with this Flatten's own transform id; Flatten is a single instance for now, so the
      // report is for its only partition (0 of 1).
      ctx.forward(
          new Record<byte[], KStreamsPayload<?>>(
              record.key(),
              KStreamsPayload.watermark(advanced.getMillis(), transformId, 0, 1),
              record.timestamp()));
    }
  }

  private static ProcessorContext<byte[], KStreamsPayload<?>> checkInitialized(
      @Nullable ProcessorContext<byte[], KStreamsPayload<?>> context) {
    if (context == null) {
      throw new IllegalStateException("FlattenProcessor used before init()");
    }
    return context;
  }
}
