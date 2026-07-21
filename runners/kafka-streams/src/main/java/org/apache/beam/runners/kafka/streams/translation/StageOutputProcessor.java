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

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One output port of a multi-output {@link ExecutableStageProcessor}: a relay node that stands in
 * as the producer of one of the stage's output PCollections.
 *
 * <p>A Kafka Streams processor forwards a record either to all of its children or to one named
 * child, but downstream transforms are wired to a <em>producer node</em> by PCollection id, and
 * that node's identity has to be known when the stage is translated — before the downstream
 * transforms are. So each output PCollection of a multi-output stage gets its own relay: the stage
 * routes each harness output to the matching relay by name, and downstream transforms wire to the
 * relay. (A single-output stage needs none of this and forwards directly.)
 *
 * <p>The relay forwards data records unchanged, and re-stamps the watermark with its own transform
 * id so a downstream watermark aggregator, which expects to hear from this relay, sees a consistent
 * source. The stage is a single instance and its watermark is already the monotonic aggregate over
 * its input, so the relay only relabels it — there is exactly one upstream partition to relay, no
 * aggregation to do.
 */
class StageOutputProcessor
    implements Processor<byte[], KStreamsPayload<?>, byte[], KStreamsPayload<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(StageOutputProcessor.class);

  private final String transformId;
  private @Nullable ProcessorContext<byte[], KStreamsPayload<?>> context;

  StageOutputProcessor(String transformId) {
    this.transformId = transformId;
  }

  @Override
  public void init(ProcessorContext<byte[], KStreamsPayload<?>> context) {
    this.context = context;
  }

  @Override
  public void process(Record<byte[], KStreamsPayload<?>> record) {
    KStreamsPayload<?> payload = record.value();
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = context;
    if (ctx == null) {
      throw new IllegalStateException("StageOutputProcessor used before init()");
    }
    if (payload == null) {
      LOG.warn(
          "Stage output {} dropping record with null payload (external write or tombstone)",
          transformId);
      return;
    }
    if (!payload.isWatermark()) {
      // Data for this output: forward unchanged.
      ctx.forward(record);
      return;
    }
    // Re-stamp the stage's watermark with this output port's own id. Single upstream instance, so
    // its value is already monotonic; just relabel it as source 0 of 1.
    WatermarkPayload report = payload.asWatermark();
    ctx.forward(
        new Record<byte[], KStreamsPayload<?>>(
            record.key(),
            KStreamsPayload.watermark(report.getWatermarkMillis(), transformId, 0, 1),
            record.timestamp()));
  }
}
