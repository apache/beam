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

import java.util.Objects;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Sum-type envelope flowing between Kafka Streams processors in the Beam Kafka Streams runner.
 *
 * <p>Every record value emitted by a runner-introduced processor is one of:
 *
 * <ul>
 *   <li>A {@link #isData() data} element wrapping a {@link WindowedValue}, or
 *   <li>A {@link #isWatermark() watermark} report carrying an event-time milliseconds value plus
 *       the in-band coordination fields (source partition and total source partition count) the
 *       downstream {@link WatermarkManager} needs.
 * </ul>
 *
 * <p>The envelope lets a single Kafka Streams output channel carry both Beam data and the watermark
 * / synchronization primitives that Kafka Streams does not natively support. Future control
 * messages (e.g. the {@code (epoch, assigned_partitions)} propagation from design doc §5) can be
 * added here as additional variants.
 *
 * <p>This class is intentionally in-JVM only for now; serialization across topic boundaries
 * (repartition or sink topics) will be introduced when the first translator that emits to a topic
 * lands, at which point a corresponding Kafka {@link org.apache.kafka.common.serialization.Serde}
 * will be added.
 *
 * @param <T> element type carried by data variants
 */
public final class KStreamsPayload<T> {

  private enum Kind {
    DATA,
    WATERMARK
  }

  private final Kind kind;
  private final @Nullable WindowedValue<T> data;
  private final long watermarkMillis;
  private final String transformId;
  private final int sourcePartition;
  private final int totalSourcePartitions;

  private KStreamsPayload(
      Kind kind,
      @Nullable WindowedValue<T> data,
      long watermarkMillis,
      String transformId,
      int sourcePartition,
      int totalSourcePartitions) {
    this.kind = kind;
    this.data = data;
    this.watermarkMillis = watermarkMillis;
    this.transformId = transformId;
    this.sourcePartition = sourcePartition;
    this.totalSourcePartitions = totalSourcePartitions;
  }

  /** Returns a data payload wrapping the given {@link WindowedValue}. */
  public static <T> KStreamsPayload<T> data(WindowedValue<T> value) {
    return new KStreamsPayload<>(Kind.DATA, value, 0L, "", 0, 0);
  }

  /**
   * Returns a watermark report payload: the event-time milliseconds together with the in-band
   * coordination fields a downstream watermark aggregator needs — which transform produced the
   * report ({@code transformId}, stamped by the producer without regard to who consumes it), which
   * of that transform's partitions this report is for, and how many partitions that transform has
   * in total.
   */
  public static <T> KStreamsPayload<T> watermark(
      long watermarkMillis, String transformId, int sourcePartition, int totalSourcePartitions) {
    Preconditions.checkArgument(
        transformId != null && !transformId.isEmpty(), "transformId must be non-empty");
    Preconditions.checkArgument(
        totalSourcePartitions > 0,
        "totalSourcePartitions must be positive: %s",
        totalSourcePartitions);
    Preconditions.checkArgument(
        sourcePartition >= 0 && sourcePartition < totalSourcePartitions,
        "sourcePartition %s out of range for totalSourcePartitions %s",
        sourcePartition,
        totalSourcePartitions);
    return new KStreamsPayload<>(
        Kind.WATERMARK, null, watermarkMillis, transformId, sourcePartition, totalSourcePartitions);
  }

  public boolean isData() {
    return kind == Kind.DATA;
  }

  public boolean isWatermark() {
    return kind == Kind.WATERMARK;
  }

  /**
   * Returns the wrapped data element. Caller must check {@link #isData()} first; calling this on a
   * watermark payload throws.
   */
  public WindowedValue<T> getData() {
    if (kind != Kind.DATA || data == null) {
      throw new IllegalStateException("Payload is not a data element: kind=" + kind);
    }
    return data;
  }

  /**
   * Narrows this payload to its {@link WatermarkPayload} view, through which the watermark report
   * fields are read. Caller must check {@link #isWatermark()} first; calling this on a data payload
   * throws.
   */
  public WatermarkPayload asWatermark() {
    Preconditions.checkState(isWatermark(), "Payload is not a watermark: kind=%s", kind);
    return new WatermarkView();
  }

  /** {@link WatermarkPayload} view backed by this payload's fields. */
  private final class WatermarkView implements WatermarkPayload {
    @Override
    public long getWatermarkMillis() {
      return watermarkMillis;
    }

    @Override
    public String getTransformId() {
      return transformId;
    }

    @Override
    public int getSourcePartition() {
      return sourcePartition;
    }

    @Override
    public int getTotalSourcePartitions() {
      return totalSourcePartitions;
    }
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KStreamsPayload)) {
      return false;
    }
    KStreamsPayload<?> that = (KStreamsPayload<?>) o;
    return kind == that.kind
        && watermarkMillis == that.watermarkMillis
        && transformId.equals(that.transformId)
        && sourcePartition == that.sourcePartition
        && totalSourcePartitions == that.totalSourcePartitions
        && Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        kind, data, watermarkMillis, transformId, sourcePartition, totalSourcePartitions);
  }

  @Override
  public String toString() {
    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this).add("kind", kind);
    if (kind == Kind.DATA) {
      helper.add("data", data);
    } else {
      helper
          .add("watermarkMillis", watermarkMillis)
          .add("transformId", transformId)
          .add("sourcePartition", sourcePartition)
          .add("totalSourcePartitions", totalSourcePartitions);
    }
    return helper.toString();
  }
}
