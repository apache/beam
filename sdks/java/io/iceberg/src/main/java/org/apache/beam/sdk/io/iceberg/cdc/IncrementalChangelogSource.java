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

import static org.apache.beam.sdk.io.iceberg.cdc.ChangelogScanner.MIXED_CHANGES;
import static org.apache.beam.sdk.io.iceberg.cdc.ChangelogScanner.UNIFORM_CHANGES;
import static org.apache.beam.sdk.io.iceberg.cdc.ReadFromChangelogs.KEYED_DELETES;
import static org.apache.beam.sdk.io.iceberg.cdc.ReadFromChangelogs.KEYED_INSERTS;
import static org.apache.beam.sdk.io.iceberg.cdc.ReadFromChangelogs.UNIFORM_ROWS;
import static org.apache.beam.sdk.io.iceberg.cdc.ReconcileChanges.DELETES;
import static org.apache.beam.sdk.io.iceberg.cdc.ReconcileChanges.INSERTS;

import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.IncrementalScanSource;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
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

public class IncrementalChangelogSource extends IncrementalScanSource {
  public IncrementalChangelogSource(IcebergScanConfig scanConfig) {
    super(scanConfig);
  }

  @Override
  public PCollection<Row> expand(PBegin input) {
    Table table =
        scanConfig
            .getCatalogConfig()
            .catalog()
            .loadTable(TableIdentifier.parse(scanConfig.getTableIdentifier()));

    PCollection<KV<String, List<SnapshotInfo>>> snapshots =
        MoreObjects.firstNonNull(scanConfig.getStreaming(), false)
            ? unboundedSnapshots(input)
            : boundedSnapshots(input, table);

    // scan each interval of snapshots and create groups of changelog tasks
    PCollectionTuple changelogTasks =
        snapshots
            .apply(Redistribute.byKey())
            .apply(
                "Create Changelog Tasks",
                ParDo.of(new ChangelogScanner(scanConfig))
                    .withOutputTags(UNIFORM_CHANGES, TupleTagList.of(MIXED_CHANGES)));

    // for changelog ordinal groups that have UNIFORM changes (i.e. all deletes, or all inserts),
    // take the fast approach of just reading and emitting CDC records.
    PCollection<Row> fastPathCdcRows =
        processUniformChanges(
            changelogTasks.get(UNIFORM_CHANGES).setCoder(ChangelogScanner.OUTPUT_CODER));

    // changelog ordinal groups that have MIXED changes (i.e. some deletes and some inserts)
    // will need extra processing to identify any updates
    PCollection<Row> slowPathCdcRows =
        processMixedChanges(
            changelogTasks.get(MIXED_CHANGES).setCoder(ChangelogScanner.OUTPUT_CODER));

    // Merge UNIFORM and MIXED outputs
    return PCollectionList.of(fastPathCdcRows).and(slowPathCdcRows).apply(Flatten.pCollections());
  }

  private PCollection<Row> processUniformChanges(
      PCollection<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> uniformChangelogs) {
    return uniformChangelogs
        .apply(Redistribute.arbitrarily())
        .apply(
            "Read Uniform Changes",
            ParDo.of(ReadFromChangelogs.of(scanConfig))
                .withOutputTags(UNIFORM_ROWS, TupleTagList.empty()))
        .get(UNIFORM_ROWS)
        .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(scanConfig.getProjectedSchema()));
  }

  private PCollection<Row> processMixedChanges(
      PCollection<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> mixedChangelogs) {
    PCollectionTuple mixedCdcKeyedRows =
        mixedChangelogs
            .apply(Redistribute.arbitrarily())
            .apply(
                "Read Mixed Changes",
                ParDo.of(ReadFromChangelogs.withKeyedOutput(scanConfig))
                    .withOutputTags(KEYED_INSERTS, TupleTagList.of(KEYED_DELETES)));

    // prior to CoGBK, set a windowing strategy to maintain the earliest timestamp in the window
    Window<KV<Row, TimestampedValue<Row>>> windowingStrategy =
        Window.<KV<Row, TimestampedValue<Row>>>into(new GlobalWindows())
            .withTimestampCombiner(TimestampCombiner.EARLIEST);

    // preserve the element's timestamp by moving it into the value
    KvCoder<Row, Row> keyedOutputCoder = ReadFromChangelogs.keyedOutputCoder(scanConfig);
    PCollection<KV<Row, TimestampedValue<Row>>> keyedInsertsWithTimestamps =
        mixedCdcKeyedRows
            .get(KEYED_INSERTS)
            .setCoder(keyedOutputCoder)
            .apply(Reify.timestampsInValue())
            .apply(windowingStrategy);
    PCollection<KV<Row, TimestampedValue<Row>>> keyedDeletesWithTimestamps =
        mixedCdcKeyedRows
            .get(KEYED_DELETES)
            .setCoder(keyedOutputCoder)
            .apply(Reify.timestampsInValue())
            .apply(windowingStrategy);

    // CoGroup by record ID and emit any (DELETE + INSERT) pairs as updates: (UPDATE_BEFORE,
    // UPDATE_AFTER)
    return KeyedPCollectionTuple.of(INSERTS, keyedInsertsWithTimestamps)
        .and(DELETES, keyedDeletesWithTimestamps)
        .apply(CoGroupByKey.create())
        .apply("Reconcile Inserts and Deletes", ParDo.of(new ReconcileChanges()))
        .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(scanConfig.getProjectedSchema()));
  }
}
