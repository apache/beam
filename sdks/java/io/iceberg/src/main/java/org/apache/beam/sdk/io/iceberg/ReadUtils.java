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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.iceberg.util.SnapshotUtil.ancestorsOf;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.IcebergIO.ReadRows.StartingStrategy;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.ParquetReader;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class for source operations. */
public class ReadUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ReadUtils.class);

  // default is 8MB. keep this low to avoid overwhelming memory
  static final int MAX_FILE_BUFFER_SIZE = 1 << 20; // 1MB
  private static final Collection<String> READ_PROPERTIES_TO_REMOVE =
      Sets.newHashSet(
          "parquet.read.filter",
          "parquet.private.read.filter.predicate",
          "parquet.read.support.class",
          "parquet.crypto.factory.class");
  static final String OPERATION = "operation";
  static final String RECORD = "record";
  static final String DEFAULT_WATERMARK_TIME_UNIT = TimeUnit.MICROSECONDS.name();

  /** Extracts {@link Row}s after a CDC streaming read. */
  public static PTransform<PCollection<Row>, PCollection<Row>> extractRecords() {
    return new ExtractRecords();
  }

  public static Schema outputCdcSchema(Schema tableSchema) {
    return Schema.builder()
        .addRowField(RECORD, tableSchema)
        .addNullableStringField(OPERATION)
        .build();
  }

  public static Schema outputCdcSchema(org.apache.iceberg.Schema tableSchema) {
    return outputCdcSchema(IcebergUtils.icebergSchemaToBeamSchema(tableSchema));
  }

  static ParquetReader<Record> createReader(FileScanTask task, Table table) {
    String filePath = task.file().path().toString();
    InputFile inputFile;
    try (FileIO io = table.io()) {
      EncryptedInputFile encryptedInput =
          EncryptedFiles.encryptedInput(io.newInputFile(filePath), task.file().keyMetadata());
      inputFile = table.encryption().decrypt(encryptedInput);
    }
    Map<Integer, ?> idToConstants =
        ReadUtils.constantsMap(task, IdentityPartitionConverters::convertConstant, table.schema());

    ParquetReadOptions.Builder optionsBuilder;
    if (inputFile instanceof HadoopInputFile) {
      // remove read properties already set that may conflict with this read
      Configuration conf = new Configuration(((HadoopInputFile) inputFile).getConf());
      for (String property : READ_PROPERTIES_TO_REMOVE) {
        conf.unset(property);
      }
      optionsBuilder = HadoopReadOptions.builder(conf);
    } else {
      optionsBuilder = ParquetReadOptions.builder();
    }
    optionsBuilder =
        optionsBuilder
            .withRange(task.start(), task.start() + task.length())
            .withMaxAllocationInBytes(MAX_FILE_BUFFER_SIZE);

    @Nullable String nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping mapping =
        nameMapping != null ? NameMappingParser.fromJson(nameMapping) : NameMapping.empty();

    return new ParquetReader<>(
        inputFile,
        table.schema(),
        optionsBuilder.build(),
        fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema, idToConstants),
        mapping,
        task.residual(),
        false,
        true);
  }

  static Map<Integer, ?> constantsMap(
      FileScanTask task,
      BiFunction<Type, Object, Object> converter,
      org.apache.iceberg.Schema schema) {
    PartitionSpec spec = task.spec();
    Set<Integer> idColumns = spec.identitySourceIds();
    org.apache.iceberg.Schema partitionSchema = TypeUtil.select(schema, idColumns);
    boolean projectsIdentityPartitionColumns = !partitionSchema.columns().isEmpty();

    if (projectsIdentityPartitionColumns) {
      return PartitionUtil.constantsMap(task, converter);
    } else {
      return Collections.emptyMap();
    }
  }

  static @Nullable Long getFromSnapshotExclusive(Table table, IcebergScanConfig scanConfig) {
    @Nullable StartingStrategy startingStrategy = scanConfig.getStartingStrategy();
    boolean isStreaming = MoreObjects.firstNonNull(scanConfig.getStreaming(), false);
    if (startingStrategy == null) {
      startingStrategy = isStreaming ? StartingStrategy.LATEST : StartingStrategy.EARLIEST;
    }

    // 1. fetch from from_snapshot
    @Nullable Long fromSnapshot = scanConfig.getFromSnapshotInclusive();
    // 2. fetch from from_timestamp
    @Nullable Long fromTimestamp = scanConfig.getFromTimestamp();
    if (fromTimestamp != null) {
      fromSnapshot = SnapshotUtil.oldestAncestorAfter(table, fromTimestamp).snapshotId();
    }
    // 3. get current snapshot if starting_strategy is LATEST
    if (fromSnapshot == null && startingStrategy.equals(StartingStrategy.LATEST)) {
      fromSnapshot = table.currentSnapshot().snapshotId();
    }
    // incremental append scan can only be configured with an *exclusive* starting snapshot,
    // so we need to provide this snapshot's parent id.
    if (fromSnapshot != null) {
      fromSnapshot = table.snapshot(fromSnapshot).parentId();
    }

    // 4. if snapshot is still null, the scan will default to the oldest snapshot, i.e. EARLIEST
    return fromSnapshot;
  }

  static @Nullable Long getToSnapshot(Table table, IcebergScanConfig scanConfig) {
    // 1. fetch from to_snapshot
    @Nullable Long toSnapshot = scanConfig.getToSnapshot();
    // 2. fetch from to_timestamp
    @Nullable Long toTimestamp = scanConfig.getToTimestamp();
    if (toTimestamp != null) {
      toSnapshot = SnapshotUtil.snapshotIdAsOfTime(table, toTimestamp);
    }

    return toSnapshot;
  }

  /**
   * Returns a list of snapshots in the range (fromSnapshotId, toSnapshotId], ordered
   * chronologically.
   */
  static List<SnapshotInfo> snapshotsBetween(
      Table table, String tableIdentifier, @Nullable Long fromSnapshotId, long toSnapshotId) {
    long from = MoreObjects.firstNonNull(fromSnapshotId, -1L);
    @SuppressWarnings("return")
    List<SnapshotInfo> snapshotIds =
        Lists.newArrayList(
                Lists.newArrayList(
                    ancestorsOf(
                        toSnapshotId,
                        snapshotId -> snapshotId != from ? table.snapshot(snapshotId) : null)))
            .stream()
            .map(s -> SnapshotInfo.fromSnapshot(s, tableIdentifier))
            .sorted(Comparator.comparingLong(SnapshotInfo::getSequenceNumber))
            .collect(Collectors.toList());

    return snapshotIds;
  }

  static @Nullable Long getLowerBoundTimestampMillis(
      FileScanTask fileScanTask, String watermarkColumnName, @Nullable String watermarkTimeUnit) {
    Types.NestedField watermarkColumn = fileScanTask.schema().findField(watermarkColumnName);
    int watermarkColumnId = watermarkColumn.fieldId();
    @Nullable Map<Integer, ByteBuffer> lowerBounds = fileScanTask.file().lowerBounds();

    if (lowerBounds != null && lowerBounds.containsKey(watermarkColumnId)) {
      TimeUnit timeUnit =
          TimeUnit.valueOf(
              MoreObjects.firstNonNull(watermarkTimeUnit, DEFAULT_WATERMARK_TIME_UNIT)
                  .toUpperCase());
      return timeUnit.toMillis(
          Conversions.fromByteBuffer(
              Types.LongType.get(), checkStateNotNull(lowerBounds.get(watermarkColumnId))));
    } else {
      LOG.warn(
          "Could not find statistics for watermark column '{}' in file '{}'.",
          watermarkColumnName,
          fileScanTask.file().path());
      return null;
    }
  }

  /**
   * Returns the output timestamp associated with this read task.
   *
   * <p>If a watermark column is not specified, we fall back on the snapshot's commit timestamp.
   *
   * <p>If a watermark column is specified, we attempt to fetch it from the file's stats. If that
   * information isn't available for whatever reason, we default to -inf.
   */
  static Instant getReadTaskTimestamp(ReadTask readTask, IcebergScanConfig scanConfig) {
    long millis;
    @Nullable String watermarkColumn = scanConfig.getWatermarkColumn();
    if (watermarkColumn != null) {
      millis =
          MoreObjects.firstNonNull(
              ReadUtils.getLowerBoundTimestampMillis(
                  readTask.getFileScanTask(), watermarkColumn, scanConfig.getWatermarkTimeUnit()),
              BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis());
      System.out.println("xxx using file millis " + millis);
    } else {
      millis = readTask.getSnapshotTimestampMillis();
      System.out.println("xxx using snapshot millis " + millis);
    }
    return Instant.ofEpochMilli(millis);
  }

  private static class ExtractRecords extends PTransform<PCollection<Row>, PCollection<Row>> {
    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
      Preconditions.checkArgument(
          input.getSchema().hasField(RECORD)
              && input.getSchema().getField(RECORD).getType().getTypeName().isCompositeType(),
          "PCollection schema must contain a \"%s\" field of type %s. Actual schema: %s",
          RECORD,
          Schema.TypeName.ROW,
          input.getSchema());
      Schema recordSchema =
          checkStateNotNull(input.getSchema().getField(RECORD).getType().getRowSchema());
      return input.apply(ParDo.of(new ExtractRecordsDoFn())).setRowSchema(recordSchema);
    }

    static class ExtractRecordsDoFn extends DoFn<Row, Row> {
      @ProcessElement
      public void process(@Element Row row, @Timestamp Instant timestamp, OutputReceiver<Row> out) {
        out.outputWithTimestamp(checkStateNotNull(row.getRow(RECORD)), timestamp);
      }
    }
  }
}
