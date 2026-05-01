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
import static org.apache.beam.sdk.values.TypeDescriptors.iterables;
import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.rows;

import java.util.UUID;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

class WriteToPartitions extends PTransform<PCollection<KV<Row, Row>>, IcebergWriteResult> {
  private static final long DEFAULT_BYTES_PER_FILE = (1L << 29); // 512mb
  private final IcebergCatalogConfig catalogConfig;
  private final DynamicDestinations dynamicDestinations;
  private final @Nullable Duration triggeringFrequency;
  private final String filePrefix;
  private final boolean autoSharding;

  WriteToPartitions(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      @Nullable Duration triggeringFrequency,
      boolean autoSharding) {
    this.dynamicDestinations = dynamicDestinations;
    this.catalogConfig = catalogConfig;
    this.triggeringFrequency = triggeringFrequency;
    // single unique prefix per write transform
    this.filePrefix = UUID.randomUUID().toString();
    this.autoSharding = autoSharding;
  }

  private PCollection<KV<Row, Iterable<Row>>> groupByPartition(PCollection<KV<Row, Row>> input) {
    RowCoder destinationCoder = RowCoder.of(AssignDestinationsAndPartitions.OUTPUT_SCHEMA);
    RowCoder dataCoder = RowCoder.of(dynamicDestinations.getDataSchema());

    GroupIntoBatches<Row, Row> groupIntoPartitions =
        GroupIntoBatches.ofByteSize(DEFAULT_BYTES_PER_FILE);
    if (IcebergUtils.isUnbounded(input) && triggeringFrequency != null) {
      groupIntoPartitions = groupIntoPartitions.withMaxBufferingDuration(triggeringFrequency);
    }

    if (autoSharding) {
      return input
          .apply(groupIntoPartitions.withShardedKey())
          .setCoder(
              KvCoder.of(
                  org.apache.beam.sdk.util.ShardedKey.Coder.of(destinationCoder),
                  IterableCoder.of(dataCoder)))
          .apply(
              "DropShardId",
              MapElements.into(kvs(rows(), iterables(rows())))
                  .via(kv -> KV.of(kv.getKey().getKey(), kv.getValue())))
          .setCoder(KvCoder.of(destinationCoder, IterableCoder.of(dataCoder)));
    } else {
      return input
          .apply(groupIntoPartitions)
          .setCoder(KvCoder.of(destinationCoder, IterableCoder.of(dataCoder)));
    }
  }

  @Override
  public IcebergWriteResult expand(PCollection<KV<Row, Row>> input) {
    PCollection<KV<Row, Iterable<Row>>> groupedRows = groupByPartition(input);

    PCollection<FileWriteResult> writtenFiles =
        groupedRows.apply(
            new WritePartitionedRowsToFiles(catalogConfig, dynamicDestinations, filePrefix));

    if (IcebergUtils.isUnbounded(input) && triggeringFrequency != null) {
      writtenFiles =
          writtenFiles.apply(
              "ApplyUserTrigger",
              Window.<FileWriteResult>into(new GlobalWindows())
                  .triggering(
                      Repeatedly.forever(
                          AfterProcessingTime.pastFirstElementInPane()
                              .plusDelayOf(checkArgumentNotNull(triggeringFrequency))))
                  .discardingFiredPanes());
    }

    // Commit files to tables
    PCollection<KV<String, SnapshotInfo>> snapshots =
        writtenFiles.apply(new AppendFilesToTables(catalogConfig, filePrefix));

    return new IcebergWriteResult(input.getPipeline(), snapshots);
  }
}
