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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.util.UUID;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

class WriteToDestinations extends PTransform<PCollection<KV<String, Row>>, IcebergWriteResult> {

  // Used for auto-sharding in streaming. Limits number of records per batch/file
  private static final int FILE_TRIGGERING_RECORD_COUNT = 500_000;
  // Used for auto-sharding in streaming. Limits total byte size per batch/file
  public static final int FILE_TRIGGERING_BYTE_COUNT = 1 << 30; // 1GiB
  static final int DEFAULT_NUM_FILE_SHARDS = 0;
  private final IcebergCatalogConfig catalogConfig;
  private final DynamicDestinations dynamicDestinations;
  private final @Nullable Duration triggeringFrequency;
  private final String filePrefix;

  WriteToDestinations(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      @Nullable Duration triggeringFrequency) {
    this.dynamicDestinations = dynamicDestinations;
    this.catalogConfig = catalogConfig;
    this.triggeringFrequency = triggeringFrequency;
    // single unique prefix per write transform
    this.filePrefix = UUID.randomUUID().toString();
  }

  @Override
  public IcebergWriteResult expand(PCollection<KV<String, Row>> input) {
    // Write records to files
    PCollection<FileWriteResult> writtenFiles =
        input.isBounded().equals(PCollection.IsBounded.UNBOUNDED)
            ? writeTriggered(input)
            : writeUntriggered(input);

    // Commit files to tables
    PCollection<KV<String, SnapshotInfo>> snapshots =
        writtenFiles.apply(new AppendFilesToTables(catalogConfig));

    return new IcebergWriteResult(input.getPipeline(), snapshots);
  }

  private PCollection<FileWriteResult> writeTriggered(PCollection<KV<String, Row>> input) {
    checkArgumentNotNull(
        triggeringFrequency, "Streaming pipelines must set a triggering frequency.");

    // Group records into batches to avoid writing thousands of small files
    PCollection<KV<ShardedKey<String>, Iterable<Row>>> groupedRecords =
        input
            .apply("WindowIntoGlobal", Window.into(new GlobalWindows()))
            // We rely on GroupIntoBatches to group and parallelize records properly,
            // respecting our thresholds for number of records and bytes per batch.
            // Each output batch will be written to a file.
            .apply(
                GroupIntoBatches.<String, Row>ofSize(FILE_TRIGGERING_RECORD_COUNT)
                    .withByteSize(FILE_TRIGGERING_BYTE_COUNT)
                    .withMaxBufferingDuration(checkArgumentNotNull(triggeringFrequency))
                    .withShardedKey())
            .setCoder(
                KvCoder.of(
                    org.apache.beam.sdk.util.ShardedKey.Coder.of(StringUtf8Coder.of()),
                    IterableCoder.of(RowCoder.of(dynamicDestinations.getDataSchema()))));

    return groupedRecords
        .apply(
            "WriteGroupedRows",
            new WriteGroupedRowsToFiles(catalogConfig, dynamicDestinations, filePrefix))
        // Respect user's triggering frequency before committing snapshots
        .apply(
            "ApplyUserTrigger",
            Window.<FileWriteResult>into(new GlobalWindows())
                .triggering(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(checkArgumentNotNull(triggeringFrequency))))
                .discardingFiredPanes());
  }

  private PCollection<FileWriteResult> writeUntriggered(PCollection<KV<String, Row>> input) {
    Preconditions.checkArgument(
        triggeringFrequency == null,
        "Triggering frequency is only applicable for streaming pipelines.");

    // First, attempt to write directly to files without shuffling. If there are
    // too many distinct destinations in a single bundle, the remaining
    // elements will be emitted to take the "slow path" that involves a shuffle
    WriteUngroupedRowsToFiles.Result writeUngroupedResult =
        input.apply(
            "Fast-path write rows",
            new WriteUngroupedRowsToFiles(catalogConfig, dynamicDestinations, filePrefix));

    // Then write the rest by shuffling on the destination
    PCollection<FileWriteResult> writeGroupedResult =
        writeUngroupedResult
            .getSpilledRows()
            .apply("Group spilled rows by destination shard", GroupByKey.create())
            .apply(
                "Write remaining rows to files",
                new WriteGroupedRowsToFiles(catalogConfig, dynamicDestinations, filePrefix));

    return PCollectionList.of(writeUngroupedResult.getWrittenFiles())
        .and(writeGroupedResult)
        .apply("Flatten Written Files ", Flatten.pCollections());
  }
}
