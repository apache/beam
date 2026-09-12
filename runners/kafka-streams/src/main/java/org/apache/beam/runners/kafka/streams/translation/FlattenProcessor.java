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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams {@link Processor} implementing Beam's {@code Flatten} primitive: the union of N
 * input PCollections into one.
 *
 * <p>Data records pass straight through — merging the parents' streams is the flatten. The work is
 * in the watermark, which Flatten owns as GroupByKey does: a {@link WatermarkAggregator} over its
 * inputs, forwarding its own watermark only when the minimum across them advances and stamping it
 * as a single source. That holds the output back until every branch has reported, so a downstream
 * GroupByKey cannot fire before all branches are drained.
 *
 * <p>Branches are told apart by the transform id each producer stamps, since Kafka Streams does not
 * say which parent forwarded a record. A producer stamps its own identity regardless of who
 * consumes it, so a PCollection feeding several Flattens reports one identity and each Flatten
 * still waits only for the upstream transforms handed to it at construction.
 */
class FlattenProcessor
    implements Processor<byte[], KStreamsPayload<?>, byte[], KStreamsPayload<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(FlattenProcessor.class);

  // This transform's own id, stamped on every watermark it forwards downstream.
  private final String transformId;
  // Computes the output watermark as min() over the upstream transforms' reports, holding until
  // every partition of every expected upstream transform has reported (see WatermarkAggregator).
  private final WatermarkAggregator watermarkAggregator;
  // The last watermark actually forwarded downstream, so we only forward when it advances.
  private Instant lastForwardedWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  // Reports this Flatten as finished once every branch it merges has gone terminal.
  private final TerminationReporter terminationReporter;

  private @Nullable ProcessorContext<byte[], KStreamsPayload<?>> context;

  /**
   * @param transformId this Flatten's own transform id, stamped on the watermarks it emits
   * @param upstreamTransformIds the producers of this Flatten's input PCollections (known from the
   *     pipeline graph), whose reports the {@link WatermarkAggregator} waits for
   */
  FlattenProcessor(
      String transformId, Set<String> upstreamTransformIds, TerminationTracker terminationTracker) {
    this.transformId = transformId;
    this.watermarkAggregator = new WatermarkAggregator(upstreamTransformIds);
    this.terminationReporter = new TerminationReporter(terminationTracker, transformId);
  }

  @Override
  public void init(ProcessorContext<byte[], KStreamsPayload<?>> context) {
    this.context = context;
    terminationReporter.init(context);
  }

  @Override
  public void close() {
    terminationReporter.close();
  }

  @Override
  public void process(Record<byte[], KStreamsPayload<?>> record) {
    KStreamsPayload<?> payload = record.value();
    if (payload == null) {
      // A topic feeding the runner can always be written to from outside (or carry a tombstone),
      // so recover from the obvious error instead of crashing the task: warn and drop.
      LOG.warn(
          "Flatten {} dropping record with null payload (external write or tombstone)",
          transformId);
      return;
    }
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
      // Labelled as the only source a consumer will see; a shuffle downstream relabels it.
      ctx.forward(
          new Record<byte[], KStreamsPayload<?>>(
              record.key(),
              KStreamsPayload.watermark(advanced.getMillis(), transformId, 0, 1),
              record.timestamp()));
      terminationReporter.watermarkEmitted(ctx, advanced.getMillis());
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
