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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Re-keys a {@code KV}-valued stream by the Beam key so Kafka Streams shuffles by it.
 *
 * <p>This is not GroupByKey-specific: any transform that needs the values of a key co-located on
 * one partition uses it — GroupByKey today, and stateful ParDo later. For a data record it sets the
 * Kafka record key to the encoded Beam key (taken from the {@code KV}), so the downstream
 * repartition sink co-locates every value of a key.
 *
 * <p>Watermark reports are relabelled here with the reporting instance's real partition identity.
 * This is the point at which a report stops being delivered in-process and starts crossing a topic:
 * upstream of it a transform forwards to its fused children, which see exactly one instance of it,
 * so the report names a single source. The {@link GroupByKeyBroadcastPartitioner} on the sink below
 * fans each report out to <em>every</em> partition, so a downstream task instead sees a report from
 * every instance of the upstream transform, and has to be able to tell them apart to know when it
 * has heard from all of them. The transform id is left alone, so the report still names the
 * transform that produced it.
 */
class ShuffleByKeyProcessor
    implements Processor<byte[], KStreamsPayload<?>, byte[], KStreamsPayload<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleByKeyProcessor.class);

  private final Coder<Object> keyCoder;

  /** How many instances the transform being shuffled runs as, and which one this is. */
  private final int upstreamPartitionCount;

  private int upstreamPartition;

  // Reports this shuffle as finished once it has written the terminal watermark to the repartition
  // topic. The downstream side reading that topic reports separately, which is why the pipeline
  // waits for every processor rather than the first.
  private final TerminationReporter terminationReporter;

  private @Nullable ProcessorContext<byte[], KStreamsPayload<?>> context;

  ShuffleByKeyProcessor(
      Coder<Object> keyCoder,
      int upstreamPartitionCount,
      String nodeName,
      TerminationTracker terminationTracker) {
    this.keyCoder = keyCoder;
    this.upstreamPartitionCount = upstreamPartitionCount;
    this.terminationReporter = new TerminationReporter(terminationTracker, nodeName);
  }

  @Override
  public void init(ProcessorContext<byte[], KStreamsPayload<?>> context) {
    this.context = context;
    // This processor runs in the upstream transform's task, so the task's partition is the
    // identity of the instance whose reports it is forwarding.
    this.upstreamPartition = context.taskId().partition();
    terminationReporter.init(context);
  }

  @Override
  public void close() {
    terminationReporter.close();
  }

  @Override
  public void process(Record<byte[], KStreamsPayload<?>> record) {
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = checkInitialized(context);
    KStreamsPayload<?> payload = record.value();
    if (payload == null) {
      // A topic feeding the runner can always be written to from outside (or carry a tombstone),
      // so recover from the obvious error instead of crashing the task: warn and drop.
      LOG.warn("Shuffle dropping record with null payload (external write or tombstone)");
      return;
    }
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
      WatermarkPayload report = payload.asWatermark();
      ctx.forward(
          new Record<byte[], KStreamsPayload<?>>(
              record.key(),
              KStreamsPayload.watermark(
                  report.getWatermarkMillis(),
                  report.getTransformId(),
                  upstreamPartition,
                  upstreamPartitionCount),
              record.timestamp()));
      terminationReporter.watermarkEmitted(ctx, report.getWatermarkMillis());
    }
  }

  private static <T> T checkInitialized(@Nullable T value) {
    if (value == null) {
      throw new IllegalStateException("ShuffleByKeyProcessor used before init()");
    }
    return value;
  }
}
