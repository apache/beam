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
package org.apache.beam.runners.spark.translation.streaming;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder.SparkWatermarks;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.ConstantInputDStream;
import org.joda.time.Instant;
import scala.Option;

/**
 * Relays SDF self-checkpoint residuals across micro-batches on the driver.
 *
 * <p>Residuals returned by the SDK harness in one micro-batch are collected via {@link
 * #onBatchResiduals}, held until their requested resume time, and re-emitted as encoded stage input
 * by {@link ResidualInputDStream} in a later micro-batch. Elements stay in their coder-encoded byte
 * form on this path; decoding happens on the executors. The relay also advances the {@link
 * GlobalWatermarkHolder} watermark for its stream from the residuals' output watermarks, so
 * downstream event-time windows fire correctly.
 *
 * <p>State lives in a static driver-side registry (like {@link GlobalWatermarkHolder}) so DStream
 * checkpointing never needs to serialize it. Residuals are lost on driver failure; the portable
 * streaming path has no driver recovery.
 *
 * <p>This relay reports a watermark per micro-batch, while an impulse reports once. A {@link
 * org.apache.beam.sdk.transforms.GroupByKey} whose inputs are flattened from both therefore sees
 * two sources with different synchronized processing times, which {@code
 * SparkTimerInternals#forStreamFromSources} rejects. Grouping an SDF's output on its own is
 * unaffected, since this relay replaces the stage's stream sources.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SdfResidualRelay {

  private static final Map<String, SdfResidualRelay> RELAYS = new ConcurrentHashMap<>();

  // Residuals waiting for their requested resume time; guarded by this.
  private final List<ScheduledResidual> pending = new ArrayList<>();
  // Residuals taken by a generated batch, keyed by batch time, until that batch reports back.
  private final Map<Long, List<ScheduledResidual>> inFlight = new HashMap<>();
  // The stage's original inputs; this stage can never be ahead of them.
  private final List<Integer> upstreamSourceIds;
  private final long batchDurationMillis;
  private Instant highWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private boolean seenResidual = false;
  // Set during translation, read from the streaming scheduler threads.
  private volatile int sourceId = -1;

  private SdfResidualRelay(List<Integer> upstreamSourceIds, long batchDurationMillis) {
    this.upstreamSourceIds = new ArrayList<>(upstreamSourceIds);
    this.batchDurationMillis = batchDurationMillis;
  }

  /** Builds a registry key that is unique across jobs sharing a job server JVM. */
  public static String relayId(String jobId, String transformId) {
    return jobId + "/" + transformId;
  }

  public static SdfResidualRelay register(
      String relayId, List<Integer> upstreamSourceIds, long batchDurationMillis) {
    SdfResidualRelay relay = new SdfResidualRelay(upstreamSourceIds, batchDurationMillis);
    if (RELAYS.putIfAbsent(relayId, relay) != null) {
      throw new IllegalStateException("Duplicate SDF residual relay registration: " + relayId);
    }
    return relay;
  }

  /** Drops every relay belonging to a finished job. */
  public static void unregisterJob(String jobId) {
    RELAYS.keySet().removeIf(relayId -> relayId.startsWith(jobId + "/"));
  }

  /** Feeds the residuals of a completed micro-batch into the relay. */
  public static void onBatchResiduals(
      String relayId, List<byte[]> serializedResiduals, long batchTimeMillis) {
    SdfResidualRelay relay = RELAYS.get(relayId);
    if (relay != null) {
      relay.onBatch(serializedResiduals, batchTimeMillis);
    }
  }

  public void setSourceId(int sourceId) {
    this.sourceId = sourceId;
  }

  private synchronized void onBatch(List<byte[]> serializedResiduals, long batchTimeMillis) {
    inFlight.remove(batchTimeMillis);
    long now = System.currentTimeMillis();
    for (byte[] serializedResidual : serializedResiduals) {
      try {
        DelayedBundleApplication residual = DelayedBundleApplication.parseFrom(serializedResidual);
        if (residual.getApplication().getElement().isEmpty()) {
          continue;
        }
        long delayMillis =
            residual.hasRequestedTimeDelay()
                ? residual.getRequestedTimeDelay().getSeconds() * 1000
                    + residual.getRequestedTimeDelay().getNanos() / 1_000_000
                : 0;
        pending.add(
            new ScheduledResidual(
                now + delayMillis,
                residual.getApplication().getElement().toByteArray(),
                residualWatermark(residual)));
        seenResidual = true;
      } catch (Exception e) {
        throw new RuntimeException("Failed to parse SDF residual", e);
      }
    }
    advanceWatermark(batchTimeMillis);
  }

  // The source watermark a residual promises for its future output; absent means unknown (hold).
  private static Instant residualWatermark(DelayedBundleApplication residual) {
    if (residual.getApplication().getOutputWatermarksMap().isEmpty()) {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }
    long watermark = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
    for (org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.Timestamp outputWatermark :
        residual.getApplication().getOutputWatermarksMap().values()) {
      watermark = Math.min(watermark, outputWatermark.getSeconds() * 1000);
    }
    return Instant.ofEpochMilli(watermark);
  }

  private void advanceWatermark(long batchTimeMillis) {
    Instant newHigh;
    if (pending.isEmpty() && inFlight.isEmpty()) {
      // The SDF completed if it ever ran; otherwise hold until the first residual arrives.
      newHigh = seenResidual ? BoundedWindow.TIMESTAMP_MAX_VALUE : highWatermark;
    } else {
      newHigh = BoundedWindow.TIMESTAMP_MAX_VALUE;
      for (ScheduledResidual scheduled : pending) {
        newHigh = earlier(newHigh, scheduled.watermark);
      }
      for (List<ScheduledResidual> taken : inFlight.values()) {
        for (ScheduledResidual scheduled : taken) {
          newHigh = earlier(newHigh, scheduled.watermark);
        }
      }
    }
    // This stage's output can never be ahead of the input still to come.
    newHigh = earlier(newHigh, upstreamHighWatermark());
    if (newHigh.isBefore(highWatermark)) {
      newHigh = highWatermark;
    }
    GlobalWatermarkHolder.add(
        sourceId, new SparkWatermarks(highWatermark, newHigh, new Instant(batchTimeMillis)));
    highWatermark = newHigh;
  }

  // Slowest watermark among the upstreams still reporting. A source drops out of the holder once it
  // stops reporting, which is how an impulse behaves after its single emission, so an absent
  // upstream constrains nothing and the residual holds stay in charge.
  private Instant upstreamHighWatermark() {
    Map<Integer, SparkWatermarks> committed =
        upstreamSourceIds.isEmpty() ? null : GlobalWatermarkHolder.get(batchDurationMillis);
    Instant high = BoundedWindow.TIMESTAMP_MAX_VALUE;
    if (committed == null) {
      return high;
    }
    for (Integer upstreamSourceId : upstreamSourceIds) {
      SparkWatermarks upstream = committed.get(upstreamSourceId);
      if (upstream != null) {
        high = earlier(high, upstream.getHighWatermark());
      }
    }
    return high;
  }

  private static Instant earlier(Instant a, Instant b) {
    return a.isBefore(b) ? a : b;
  }

  private synchronized List<byte[]> takeDue(long validTimeMillis) {
    List<byte[]> due = new ArrayList<>();
    List<ScheduledResidual> taken = new ArrayList<>();
    Iterator<ScheduledResidual> iterator = pending.iterator();
    while (iterator.hasNext()) {
      ScheduledResidual scheduled = iterator.next();
      if (scheduled.dueMillis <= validTimeMillis) {
        due.add(scheduled.elementBytes);
        taken.add(scheduled);
        iterator.remove();
      }
    }
    if (!taken.isEmpty()) {
      inFlight.merge(
          validTimeMillis,
          taken,
          (existing, added) -> {
            existing.addAll(added);
            return existing;
          });
    }
    return due;
  }

  private static class ScheduledResidual {
    private final long dueMillis;
    private final byte[] elementBytes;
    private final Instant watermark;

    ScheduledResidual(long dueMillis, byte[] elementBytes, Instant watermark) {
      this.dueMillis = dueMillis;
      this.elementBytes = elementBytes;
      this.watermark = watermark;
    }
  }

  /** Input stream emitting the encoded residuals due for resumption at each micro-batch. */
  public static class ResidualInputDStream extends ConstantInputDStream<byte[]> {

    private final String relayId;

    public ResidualInputDStream(StreamingContext ssc, String relayId) {
      super(ssc, emptyRdd(ssc), JavaSparkContext$.MODULE$.fakeClassTag());
      this.relayId = relayId;
    }

    private static RDD<byte[]> emptyRdd(StreamingContext ssc) {
      return ssc.sparkContext().emptyRDD(JavaSparkContext$.MODULE$.fakeClassTag());
    }

    @Override
    public Option<RDD<byte[]>> compute(Time validTime) {
      SdfResidualRelay relay = RELAYS.get(relayId);
      if (relay == null) {
        return Option.apply(emptyRdd(context()));
      }
      List<byte[]> due = relay.takeDue(validTime.milliseconds());
      if (due.isEmpty()) {
        return Option.apply(emptyRdd(context()));
      }
      JavaSparkContext jsc = JavaSparkContext.fromSparkContext(context().sparkContext());
      return Option.apply(jsc.parallelize(due).rdd());
    }
  }
}
