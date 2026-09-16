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

import static org.apache.beam.sdk.io.iceberg.IcebergUtils.beamRowToIcebergRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.io.iceberg.DynamicDestinations;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.SerializableDeleteFile;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Stage 3 of the CDC sink: consumes each complete sorted {@code (destination, shard, window)} group
 * from {@link CommitWindows}, collapses each primary key to its final state through a {@link
 * RecordDeltaTaskWriter}, and emits one {@link ShardDeltaFiles} with the serialized file metadata.
 */
final class WriteDeltas
    extends PTransform<
        PCollection<KV<DestinationShard, Iterable<KV<byte[], CdcRecord>>>>,
        PCollection<ShardDeltaFiles>> {

  private final IcebergCatalogConfig catalogConfig;
  private final CdcWriteConfig config;
  private final DynamicDestinations destinations;
  private final String runId;

  WriteDeltas(
      IcebergCatalogConfig catalogConfig,
      CdcWriteConfig config,
      DynamicDestinations destinations,
      String runId) {
    this.catalogConfig = catalogConfig;
    this.config = config;
    this.destinations = destinations;
    this.runId = runId;
  }

  @Override
  public PCollection<ShardDeltaFiles> expand(
      PCollection<KV<DestinationShard, Iterable<KV<byte[], CdcRecord>>>> input) {
    return input
        .apply(
            "WriteDeltas",
            ParDo.of(
                new WriteDeltasFn(
                    new TableSetup(catalogConfig, config, destinations, runId),
                    config,
                    runId,
                    destinations.getDataSchema())))
        .setCoder(ShardDeltaFiles.coder());
  }

  /**
   * Writes one {@link RecordDeltaTaskWriter} per input group and emits the serialized {@link
   * ShardDeltaFiles}. On any failure {@link RecordDeltaTaskWriter#abort()} deletes the group's
   * written files.
   */
  @VisibleForTesting
  static final class WriteDeltasFn
      extends DoFn<KV<DestinationShard, Iterable<KV<byte[], CdcRecord>>>, ShardDeltaFiles> {

    private final TableSetup tableSetup;
    private final CdcWriteConfig config;
    private final String runId;
    private final Schema dataSchema;

    WriteDeltasFn(TableSetup tableSetup, CdcWriteConfig config, String runId, Schema dataSchema) {
      this.tableSetup = tableSetup;
      this.config = config;
      this.runId = runId;
      this.dataSchema = dataSchema;
    }

    @ProcessElement
    public void process(
        @Element KV<DestinationShard, Iterable<KV<byte[], CdcRecord>>> group,
        BoundedWindow window,
        OutputReceiver<ShardDeltaFiles> out)
        throws IOException {
      String destString = group.getKey().getDestination();
      int shardId = group.getKey().getShard();
      TableSetup.Dest dest = tableSetup.get(destString, dataSchema);
      Table table = dest.table();
      PartitionSpec spec = dest.spec();

      FileFormat dataFormat = RecordDeltaTaskWriter.dataFileFormat(table);
      FileFormat deleteFormat = RecordDeltaTaskWriter.deleteFileFormat(table, dataFormat);
      long targetFileSize =
          PropertyUtil.propertyAsLong(
              table.properties(),
              TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
              TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

      OutputFileFactory fileFactory =
          OutputFileFactory.builderFor(table, shardId, /* taskId= */ 0)
              .format(dataFormat)
              .defaultSpec(spec)
              .operationId(
                  runId + "-" + window.maxTimestamp().getMillis() + "-" + UUID.randomUUID())
              .build();
      RecordDeltaTaskWriter writer =
          RecordDeltaTaskWriter.create(
              table,
              spec,
              dest.equalityFieldIds(),
              config.getUpsert(),
              targetFileSize,
              fileFactory,
              dataFormat,
              deleteFormat);

      org.apache.iceberg.Schema tableSchema = table.schema();
      long minSeq = Long.MAX_VALUE;
      long maxSeq = Long.MIN_VALUE;
      try {
        // Sorted upstream by the pk-prefixed sort key: the collapse writer relies on each key's
        // records arriving contiguous, in (seq, kind) order, and reads the key's pk prefix to
        // find block boundaries.
        for (KV<byte[], CdcRecord> keyed : group.getValue()) {
          CdcRecord record = keyed.getValue();
          writer.write(
              keyed.getKey(),
              beamRowToIcebergRecord(tableSchema, record.getData()),
              record.getKind());
          minSeq = Math.min(minSeq, record.getSequenceNumber());
          maxSeq = Math.max(maxSeq, record.getSequenceNumber());
        }

        WriteResult result = writer.complete();
        if (result.dataFiles().length == 0 && result.deleteFiles().length == 0) {
          return; // Empty group: emit nothing.
        }
        out.output(serialize(destString, table, result, minSeq, maxSeq));
      } catch (Throwable t) {
        // Delete this group's written files; preserve the root cause if abort also fails.
        try {
          writer.abort();
        } catch (Exception abortEx) {
          t.addSuppressed(abortEx);
        }
        throw t;
      }
    }
  }

  /** Serializes {@code result}'s files and returns the {@link ShardDeltaFiles} carrying them. */
  static ShardDeltaFiles serialize(
      String destString, Table table, WriteResult result, long minSeq, long maxSeq) {
    Map<Integer, PartitionSpec> specs = table.specs();
    List<SerializableDataFile> dataFiles = new ArrayList<>(result.dataFiles().length);
    for (DataFile dataFile : result.dataFiles()) {
      dataFiles.add(SerializableDataFile.from(dataFile, specs));
    }
    List<SerializableDeleteFile> deleteFiles = new ArrayList<>(result.deleteFiles().length);
    for (DeleteFile deleteFile : result.deleteFiles()) {
      deleteFiles.add(SerializableDeleteFile.from(deleteFile, specs));
    }
    return ShardDeltaFiles.builder()
        .setTableIdentifierString(destString)
        .setDataFiles(dataFiles)
        .setDeleteFiles(deleteFiles)
        .setMinSequenceNumber(minSeq)
        .setMaxSequenceNumber(maxSeq)
        .build();
  }
}
