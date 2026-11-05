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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The CDC sink's commit stage: commits each {@code (destination, window)}'s merged writer outputs
 * (represented as {@link ShardDeltaFiles}) as a single Iceberg snapshot, in ascending window-end
 * order. Re-keys by destination, gathers all shards per {@code (dest, window)}, captures the window
 * end, then re-windows into the global window for the stateful {@link OrderedCommitFn}.
 *
 * <p>Each commit writes the window's end millis to the snapshot summary as an idempotency token,
 * keyed by the sink's unique {@code sinkId}. The committer recovers it by scanning snapshot
 * ancestry: once on first touch of a destination, and again on every commit fire. Any window whose
 * end is at or below the recovered token has already been committed, so it is skipped.
 */
class CommitDeltas
    extends PTransform<PCollection<ShardDeltaFiles>, PCollection<KV<String, SnapshotInfo>>> {

  // test-only attributes
  @VisibleForTesting static volatile @Nullable Runnable preCommitHookForTest = null;
  @VisibleForTesting static volatile @Nullable BiConsumer<Long, List<Long>> onFireForTest = null;

  private final IcebergCatalogConfig catalogConfig;
  private final String sinkId;
  private final Map<String, String> snapshotProperties;
  private final long heartbeatMillis;

  /** The expansion's runId. */
  private final String runId;

  private OrderedCommitFn.Clock clock = System::currentTimeMillis;

  @VisibleForTesting
  CommitDeltas(IcebergCatalogConfig catalogConfig, String sinkId) {
    this(catalogConfig, sinkId, null, null);
  }

  @VisibleForTesting
  CommitDeltas(
      IcebergCatalogConfig catalogConfig,
      String sinkId,
      @Nullable Map<String, String> snapshotProperties,
      @Nullable Long tokenHeartbeatMillis) {
    this(
        catalogConfig,
        sinkId,
        snapshotProperties,
        tokenHeartbeatMillis,
        UUID.randomUUID().toString());
  }

  CommitDeltas(
      IcebergCatalogConfig catalogConfig,
      String sinkId,
      @Nullable Map<String, String> snapshotProperties,
      @Nullable Long tokenHeartbeatMillis,
      String runId) {
    this.catalogConfig = catalogConfig;
    this.sinkId = sinkId;
    this.snapshotProperties =
        snapshotProperties == null ? Collections.emptyMap() : snapshotProperties;
    this.heartbeatMillis = tokenHeartbeatMillis == null ? 0L : tokenHeartbeatMillis;
    this.runId = runId;
  }

  /** Overrides the committer's clock to test skew deterministically. */
  @VisibleForTesting
  CommitDeltas withClockForTest(OrderedCommitFn.Clock clock) {
    this.clock = clock;
    return this;
  }

  @Override
  public PCollection<KV<String, SnapshotInfo>> expand(PCollection<ShardDeltaFiles> input) {
    boolean streaming = input.isBounded() == PCollection.IsBounded.UNBOUNDED;
    return input
        .apply("KeyByDestination", WithKeys.of(ShardDeltaFiles::getTableIdentifierString))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), ShardDeltaFiles.coder()))
        // One element per (dest, window): every shard's output for the pair.
        // Late panes are handled upstream by SplitLateData so no need to handle again here.
        .apply("GatherShardsPerWindow", GroupByKey.create())
        .apply("CaptureWindowEnd", ParDo.of(new CaptureWindowEndFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), windowedCommitCoder()))
        .apply("ToGlobalWindow", Window.into(new GlobalWindows()))
        .apply(
            "OrderedCommit",
            ParDo.of(
                new OrderedCommitFn(
                    catalogConfig,
                    sinkId,
                    runId,
                    snapshotProperties,
                    heartbeatMillis,
                    clock,
                    streaming)))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), snapshotInfoCoder()));
  }

  private static Coder<SnapshotInfo> snapshotInfoCoder() {
    try {
      return SchemaRegistry.createDefault().getSchemaCoder(SnapshotInfo.class);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException("Could not build a coder for SnapshotInfo.", e);
    }
  }

  static Coder<WindowedCommit> windowedCommitCoder() {
    try {
      return SchemaRegistry.createDefault().getSchemaCoder(WindowedCommit.class);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException("Could not build a coder for WindowedCommit.", e);
    }
  }

  /**
   * One {@code (destination, window)}'s merged writer outputs, tagged with the window's end.
   *
   * <p>The window end is a deterministic {@code FixedWindows} boundary and doubles as the
   * restart-safe idempotency token. A window whose end is at or below the recovered
   * committed-through token is skipped.
   */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  abstract static class WindowedCommit {
    @SchemaFieldNumber("0")
    public abstract long getWindowEndMs();

    @SchemaFieldNumber("1")
    public abstract List<ShardDeltaFiles> getFiles();

    public static WindowedCommit of(long windowEndMs, List<ShardDeltaFiles> files) {
      return new AutoValue_CommitDeltas_WindowedCommit(windowEndMs, files);
    }
  }

  /** Folds a {@code (dest, window)}'s shard outputs into one {@link WindowedCommit}. */
  static class CaptureWindowEndFn
      extends DoFn<KV<String, Iterable<ShardDeltaFiles>>, KV<String, WindowedCommit>> {
    @ProcessElement
    public void process(
        @Element KV<String, Iterable<ShardDeltaFiles>> element,
        BoundedWindow window,
        OutputReceiver<KV<String, WindowedCommit>> out) {
      long windowEndMs = window.maxTimestamp().getMillis();
      List<ShardDeltaFiles> files = Lists.newArrayList(element.getValue());
      out.outputWithTimestamp(
          KV.of(element.getKey(), WindowedCommit.of(windowEndMs, files)), window.maxTimestamp());
    }
  }
}
