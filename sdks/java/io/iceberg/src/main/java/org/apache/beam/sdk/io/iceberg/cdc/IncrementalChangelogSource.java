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

import static org.apache.beam.sdk.io.iceberg.cdc.ChangelogScanner.BIDIRECTIONAL_CHANGES;
import static org.apache.beam.sdk.io.iceberg.cdc.ChangelogScanner.UNIDIRECTIONAL_CHANGES;
import static org.apache.beam.sdk.io.iceberg.cdc.ReconcileChanges.DELETES;
import static org.apache.beam.sdk.io.iceberg.cdc.ReconcileChanges.INSERTS;

import java.util.List;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.IncrementalScanSource;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * An Iceberg source that incrementally reads a table's changelogs using range(s) of table
 * snapshots. The bounded source creates a single range, while the unbounded implementation
 * continuously polls for new snapshots at the specified interval.
 */
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
                    .withOutputTags(
                        UNIDIRECTIONAL_CHANGES, TupleTagList.of(BIDIRECTIONAL_CHANGES)));
    changelogTasks.get(UNIDIRECTIONAL_CHANGES).setCoder(ChangelogScanner.OUTPUT_CODER);
    changelogTasks.get(BIDIRECTIONAL_CHANGES).setCoder(ChangelogScanner.OUTPUT_CODER);

    // process changelog tasks and output rows
    ReadFromChangelogs.CdcOutput outputRows =
        changelogTasks.apply(new ReadFromChangelogs(scanConfig));

    // compare bi-directional rows to identify potential updates
    PCollection<Row> biDirectionalCdcRows =
        KeyedPCollectionTuple.of(INSERTS, outputRows.keyedInserts())
            .and(DELETES, outputRows.keyedDeletes())
            .apply("CoGroupBy Primary Key", CoGroupByKey.create())
            .apply("Reconcile Updates-Inserts-Deletes", ParDo.of(new ReconcileChanges()))
            .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(scanConfig.getProjectedSchema()));

    // Merge uni-directional and bi-directional outputs
    return PCollectionList.of(outputRows.uniDirectionalRows())
        .and(biDirectionalCdcRows)
        .apply(Flatten.pCollections());
  }
}
