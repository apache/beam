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

import static org.apache.iceberg.util.SnapshotUtil.ancestorsOf;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.IcebergIO.ReadRows.StartingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.ParquetReader;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Helper class for source operations. */
public class ReadUtils {
  // default is 8MB. keep this low to avoid overwhelming memory
  static final int MAX_FILE_BUFFER_SIZE = 1 << 18; // 256KB
  private static final Collection<String> READ_PROPERTIES_TO_REMOVE =
      Sets.newHashSet(
          "parquet.read.filter",
          "parquet.private.read.filter.predicate",
          "parquet.read.support.class",
          "parquet.crypto.factory.class");

  static ParquetReader<Record> createReader(FileScanTask task, Table table, Schema schema) {
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
        schema,
        optionsBuilder.build(),
        // TODO(ahmedabu98): Implement a Parquet-to-Beam Row reader, bypassing conversion to Iceberg
        // Record
        fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema, idToConstants),
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
      @Nullable Snapshot currentSnapshot = table.currentSnapshot();
      if (currentSnapshot != null) {
        fromSnapshot = currentSnapshot.snapshotId();
      }
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

  public static CloseableIterable<Record> maybeApplyFilter(
      CloseableIterable<Record> iterable, IcebergScanConfig scanConfig) {
    InternalRecordWrapper wrapper =
        new InternalRecordWrapper(scanConfig.getRequiredSchema().asStruct());
    Expression filter = scanConfig.getFilter();
    Evaluator evaluator = scanConfig.getEvaluator();
    if (filter != null && evaluator != null && filter.op() != Expression.Operation.TRUE) {
      return CloseableIterable.filter(iterable, record -> evaluator.eval(wrapper.wrap(record)));
    }
    return iterable;
  }
}
