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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

class WriteToDestinations extends PTransform<PCollection<KV<String, Row>>, IcebergWriteResult> {

  // Used for auto-sharding in streaming. Limits number of records per batch/file
  private static final int FILE_TRIGGERING_RECORD_COUNT = 500_000;
  // Used for auto-sharding in streaming. Limits total byte size per batch/file
  public static final int FILE_TRIGGERING_BYTE_COUNT = 1 << 30; // 1GiB
  private static final long DEFAULT_MAX_BYTES_PER_FILE = (1L << 29); // 512mb
  private final IcebergCatalogConfig catalogConfig;
  private final DynamicDestinations dynamicDestinations;
  private final @Nullable Duration triggeringFrequency;
  private final String filePrefix;
  private final @Nullable Integer directWriteByteLimit;

  WriteToDestinations(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      @Nullable Duration triggeringFrequency,
      @Nullable Integer directWriteByteLimit) {
    this.dynamicDestinations = dynamicDestinations;
    this.catalogConfig = catalogConfig;
    this.triggeringFrequency = triggeringFrequency;
    this.directWriteByteLimit = directWriteByteLimit;
    // single unique prefix per write transform
    this.filePrefix = UUID.randomUUID().toString();
  }

  @Override
  public IcebergWriteResult expand(PCollection<KV<String, Row>> input) {
    // Write records to files
    PCollection<FileWriteResult> writtenFiles;
    if (IcebergUtils.isUnbounded(input)) {
      writtenFiles =
          IcebergUtils.validDirectWriteLimit(directWriteByteLimit)
              ? writeTriggeredWithBundleLifting(input)
              : writeTriggered(input);
    } else {
      writtenFiles = writeUntriggered(input);
    }

    // Commit files to tables
    PCollection<KV<String, SnapshotInfo>> snapshots =
        writtenFiles.apply(new AppendFilesToTables(catalogConfig, filePrefix));

    return new IcebergWriteResult(input.getPipeline(), snapshots);
  }

  private PCollection<FileWriteResult> groupAndWriteRecords(PCollection<KV<String, Row>> input) {
    // We rely on GroupIntoBatches to group and parallelize records properly,
    // respecting our thresholds for number of records and bytes per batch.
    // Each output batch will be written to a file.
    PCollection<KV<ShardedKey<String>, Iterable<Row>>> groupedRecords =
        input
            .apply(
                GroupIntoBatches.<String, Row>ofSize(FILE_TRIGGERING_RECORD_COUNT)
                    .withByteSize(FILE_TRIGGERING_BYTE_COUNT)
                    .withMaxBufferingDuration(checkArgumentNotNull(triggeringFrequency))
                    .withShardedKey())
            .setCoder(
                KvCoder.of(
                    org.apache.beam.sdk.util.ShardedKey.Coder.of(StringUtf8Coder.of()),
                    IterableCoder.of(RowCoder.of(dynamicDestinations.getDataSchema()))));

    return groupedRecords.apply(
        "WriteGroupedRows",
        new WriteGroupedRowsToFiles(
            catalogConfig, dynamicDestinations, filePrefix, DEFAULT_MAX_BYTES_PER_FILE));
  }

  private PCollection<FileWriteResult> applyUserTriggering(PCollection<FileWriteResult> input) {
    return input.apply(
        "ApplyUserTrigger",
        Window.<FileWriteResult>into(new GlobalWindows())
            .triggering(
                Repeatedly.forever(
                    AfterProcessingTime.pastFirstElementInPane()
                        .plusDelayOf(checkArgumentNotNull(triggeringFrequency))))
            .discardingFiredPanes());
  }

  private PCollection<FileWriteResult> writeTriggeredWithBundleLifting(
      PCollection<KV<String, Row>> input) {
    checkArgumentNotNull(
        triggeringFrequency, "Streaming pipelines must set a triggering frequency.");
    checkArgumentNotNull(
        directWriteByteLimit, "Must set non-null directWriteByteLimit for bundle lifting.");

    final TupleTag<KV<String, Row>> groupedRecordsTag = new TupleTag<>("small_batches");
    final TupleTag<KV<String, Row>> directRecordsTag = new TupleTag<>("large_batches");

    input = input.apply("WindowIntoGlobal", Window.into(new GlobalWindows()));
    PCollectionTuple bundleOutputs =
        input.apply(
            BundleLifter.of(
                groupedRecordsTag, directRecordsTag, directWriteByteLimit, new RowSizer()));

    PCollection<KV<String, Row>> smallBatches =
        bundleOutputs
            .get(groupedRecordsTag)
            .setCoder(
                KvCoder.of(StringUtf8Coder.of(), RowCoder.of(dynamicDestinations.getDataSchema())));
    PCollection<KV<String, Row>> largeBatches =
        bundleOutputs
            .get(directRecordsTag)
            .setCoder(
                KvCoder.of(StringUtf8Coder.of(), RowCoder.of(dynamicDestinations.getDataSchema())));

    PCollection<FileWriteResult> directFileWrites =
        largeBatches.apply(
            "WriteDirectRowsToFiles",
            new WriteDirectRowsToFiles(
                catalogConfig, dynamicDestinations, filePrefix, DEFAULT_MAX_BYTES_PER_FILE));

    PCollection<FileWriteResult> groupedFileWrites = groupAndWriteRecords(smallBatches);

    PCollection<FileWriteResult> allFileWrites =
        PCollectionList.of(groupedFileWrites)
            .and(directFileWrites)
            .apply(Flatten.<FileWriteResult>pCollections());

    return applyUserTriggering(allFileWrites);
  }

  private PCollection<FileWriteResult> writeTriggered(PCollection<KV<String, Row>> input) {
    checkArgumentNotNull(
        triggeringFrequency, "Streaming pipelines must set a triggering frequency.");
    input = input.apply("WindowIntoGlobal", Window.into(new GlobalWindows()));
    PCollection<FileWriteResult> files = groupAndWriteRecords(input);
    return applyUserTriggering(files);
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
            new WriteUngroupedRowsToFiles(
                catalogConfig, dynamicDestinations, filePrefix, DEFAULT_MAX_BYTES_PER_FILE));

    // Then write the rest by shuffling on the destination
    PCollection<FileWriteResult> writeGroupedResult =
        writeUngroupedResult
            .getSpilledRows()
            .apply("Group spilled rows by destination shard", GroupByKey.create())
            .apply(
                "Write remaining rows to files",
                new WriteGroupedRowsToFiles(
                    catalogConfig, dynamicDestinations, filePrefix, DEFAULT_MAX_BYTES_PER_FILE));

    return PCollectionList.of(writeUngroupedResult.getWrittenFiles())
        .and(writeGroupedResult)
        .apply("Flatten Written Files", Flatten.pCollections());
  }

  /**
   * A SerializableFunction to estimate the byte size of a Row for bundling purposes. This is a
   * heuristic that avoids the high cost of encoding each row with a Coder.
   */
  private static class RowSizer implements SerializableFunction<KV<String, Row>, Integer> {
    @Override
    public Integer apply(KV<String, Row> element) {
      return estimateRowSize(element.getValue());
    }

    private int estimateRowSize(Row row) {
      if (row == null) {
        return 0;
      }
      int size = 0;
      for (Object value : row.getValues()) {
        size += estimateObjectSize(value);
      }
      return size;
    }

    private int estimateObjectSize(@Nullable Object value) {
      if (value == null) {
        return 0;
      }
      if (value instanceof String) {
        return ((String) value).getBytes(UTF_8).length;
      } else if (value instanceof byte[]) {
        return ((byte[]) value).length;
      } else if (value instanceof Row) {
        return estimateRowSize((Row) value);
      } else if (value instanceof List) {
        int listSize = 0;
        for (Object item : (List) value) {
          listSize += estimateObjectSize(item);
        }
        return listSize;
      } else if (value instanceof Map) {
        int mapSize = 0;
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          mapSize += estimateObjectSize(entry.getKey()) + estimateObjectSize(entry.getValue());
        }
        return mapSize;
      } else {
        return 8; // Approximation for other fields
      }
    }
  }
}
