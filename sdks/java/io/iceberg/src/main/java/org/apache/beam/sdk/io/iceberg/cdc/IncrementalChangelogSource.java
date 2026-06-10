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
package org.apache.beam.sdk.io.iceberg.cdc;

import static org.apache.beam.sdk.io.iceberg.cdc.ChangelogScanner.LARGE_BIDIRECTIONAL_TASKS;
import static org.apache.beam.sdk.io.iceberg.cdc.ChangelogScanner.SMALL_BIDIRECTIONAL_TASKS;
import static org.apache.beam.sdk.io.iceberg.cdc.ChangelogScanner.UNIDIRECTIONAL_TASKS;
import static org.apache.beam.sdk.io.iceberg.cdc.ResolveChanges.DELETES;
import static org.apache.beam.sdk.io.iceberg.cdc.ResolveChanges.INSERTS;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.ReadUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * An Iceberg source that incrementally reads a table's changelogs, processing one snapshot at a
 * time.
 *
 * <p>Each snapshot is resolved independently. For a given primary key, this source emits the <b>net
 * change</b> the snapshot produces, and not its intermediate states.
 *
 * <p>Implications: if a writer batches several transitions for the same PK into one snapshot (for
 * example {@code A → B, then B → C}), only the endpoints survive. The intermediate {@code B} is
 * dropped. A round-trip within a single snapshot (for example {@code A → B → A}) is also dropped.
 *
 * <p>The streaming path uses {@link WatchForSnapshotsSdf} for proper per-snapshot watermarks. The
 * bounded path creates the snapshot range up front.
 */
public class IncrementalChangelogSource extends PTransform<PBegin, PCollection<Row>> {
  private final IcebergScanConfig scanConfig;

  public IncrementalChangelogSource(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
  }

  @Override
  public PCollection<Row> expand(PBegin input) {
    // emit one SnapshotInfo per element, with element timestamp -> snapshot commit time.
    PCollection<Long> snapshots =
        MoreObjects.firstNonNull(scanConfig.getStreaming(), false)
            ? unboundedSnapshots(input)
            : boundedSnapshots(input);

    // process one snapshot at a time and produce batches of changelog scan tasks.
    // tasks are emitted to three outputs:
    // 1. unidirectional tasks: we know these won't have any updates
    // 2. small bidirectional tasks: these may contain an update, but the batch is small enough to
    // resolve in-memory
    // 2. large bidirectional tasks: may contain an update, but are too large for in-memory
    // resolution. will
    //    need to run these output rows through a CoGBK
    PCollectionTuple changelogTasks =
        snapshots.apply(
            "Create Changelog Tasks",
            ParDo.of(new ChangelogScanner(scanConfig))
                .withOutputTags(
                    UNIDIRECTIONAL_TASKS,
                    TupleTagList.of(LARGE_BIDIRECTIONAL_TASKS).and(SMALL_BIDIRECTIONAL_TASKS)));
    KvCoder<ChangelogDescriptor, List<SerializableChangelogTask>> tasksCoder =
        ChangelogScanner.coder(scanConfig.rowIdBeamSchema());
    changelogTasks.get(UNIDIRECTIONAL_TASKS).setCoder(tasksCoder);
    changelogTasks.get(SMALL_BIDIRECTIONAL_TASKS).setCoder(tasksCoder);
    changelogTasks.get(LARGE_BIDIRECTIONAL_TASKS).setCoder(tasksCoder);

    Schema projectedRowSchema =
        IcebergUtils.icebergSchemaToBeamSchema(scanConfig.getProjectedSchema());
    Schema outputRowSchema = CdcOutputUtils.outputSchema(scanConfig, projectedRowSchema);

    // reads UNIDIRECTIONAL and BIDIRECTIONAL tags and produces rows.
    ReadFromChangelogs.Output outputRows = changelogTasks.apply(new ReadFromChangelogs(scanConfig));

    // Small overlapping groups get resolved entirely in memory with no shuffle.
    PCollection<Row> smallBidirectionalCdcRows =
        changelogTasks
            .get(SMALL_BIDIRECTIONAL_TASKS)
            .apply("Redistribute Small Bidirectional Changes", Redistribute.arbitrarily())
            .apply("Resolve Locally", ParDo.of(new LocalResolveDoFn(scanConfig)))
            .setRowSchema(outputRowSchema);

    // BIDIRECTIONAL records go through a CoGBK and ResolveChanges
    // We window locally using a custom WindowFn based on the snapsot's commit time. Each snapshot
    // exists in its own window.
    // We re-window the resolved output back to GlobalWindows before the final Flatten
    // to align with the other branches.
    Window<KV<CdcRowDescriptor, Row>> keyedWindowing =
        Window.<KV<CdcRowDescriptor, Row>>into(new SnapshotWindowFn())
            .triggering(AfterWatermark.pastEndOfWindow())
            .withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes();
    PCollection<KV<CdcRowDescriptor, Row>> keyedInserts =
        outputRows.biDirectionalInserts().apply("Window Inserts", keyedWindowing);
    PCollection<KV<CdcRowDescriptor, Row>> keyedDeletes =
        outputRows.biDirectionalDeletes().apply("Window Deletes", keyedWindowing);
    PCollection<Row> biDirectionalCdcRows =
        KeyedPCollectionTuple.of(INSERTS, keyedInserts)
            .and(DELETES, keyedDeletes)
            .apply("CoGroupBy Primary Key", CoGroupByKey.create())
            .apply("Resolve Delete-Insert Pairs", ParDo.of(new ResolveChanges(scanConfig)))
            .setRowSchema(outputRowSchema)
            .apply(
                "Re-window to Global",
                Window.<Row>into(new GlobalWindows())
                    .triggering(DefaultTrigger.of())
                    .discardingFiredPanes());

    // Merge all three paths into a single output. All three are in GlobalWindows.
    PCollection<Row> merged =
        PCollectionList.of(outputRows.uniDirectionalRows())
            .and(smallBidirectionalCdcRows)
            .and(biDirectionalCdcRows)
            .apply(Flatten.pCollections());

    // If the user configures a watermark column, restamp each record by
    // that column's value. Output watermark then advances per-record rather than per-snapshot.
    @Nullable String watermarkColumn = scanConfig.getWatermarkColumn();
    if (watermarkColumn != null) {
      merged =
          merged.apply(
              "Apply Watermark Column",
              ParDo.of(
                  new ApplyWatermarkColumn(
                      watermarkColumn, scanConfig.getWatermarkColumnTimeUnit())));
    }

    return merged.setRowSchema(outputRowSchema);
  }

  /**
   * Continuously watches the Iceberg table for new snapshots via {@link WatchForSnapshotsSdf} and
   * emits per snapshot.
   */
  private PCollection<Long> unboundedSnapshots(PBegin input) {
    return input
        .apply("Impulse", Create.of(""))
        .apply("Watch for Snapshots", ParDo.of(new WatchForSnapshotsSdf(scanConfig)));
  }

  /**
   * Reads the full snapshot range up front and emits each snapshot individually, each carrying its
   * own commit time as the element timestamp.
   */
  private PCollection<Long> boundedSnapshots(PBegin input) {
    Table table =
        scanConfig
            .getCatalogConfig()
            .catalog()
            .loadTable(TableIdentifier.parse(scanConfig.getTableIdentifier()));
    checkStateNotNull(
        table.currentSnapshot(),
        "Table %s does not have any snapshots to read from.",
        scanConfig.getTableIdentifier());

    @Nullable Long from = ReadUtils.getFromSnapshotExclusive(table, scanConfig);
    long to =
        MoreObjects.firstNonNull(
            ReadUtils.getToSnapshot(table, scanConfig), table.currentSnapshot().snapshotId());
    List<TimestampedValue<Long>> timestamped =
        ReadUtils.snapshotsBetween(table, scanConfig.getTableIdentifier(), from, to).stream()
            .map(
                s ->
                    TimestampedValue.of(
                        s.getSnapshotId(), Instant.ofEpochMilli(s.getTimestampMillis())))
            .collect(Collectors.toList());
    return input.apply("Create Snapshot Range", Create.timestamped(timestamped));
  }
}
