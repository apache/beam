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
package org.apache.beam.sdk.io.iceberg.maintenance;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.util.StructLikeSet;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

/** Builds the {@link TaskWriter} for one rewrite subgroup. */
class WriterFactory {
  @VisibleForTesting static int maxOpenFanoutWriters = 100;
  // Number of output partitions opened while writing (one appender each).
  private static final Counter openFanoutWriters =
      Metrics.counter(WriterFactory.class, "openFanoutWriters");

  private final long targetFileSizeBytes;
  private final String operationId;
  private final long attemptId;
  private final int globalIndex;
  private final PartitionSpec outputSpec;
  private final FileFormat format;
  private final Map<String, String> writeProperties;
  private final boolean preserveRowLineage;
  private @MonotonicNonNull OutputFileFactory outputFileFactory;
  private @MonotonicNonNull Table table;

  /**
   * @param attemptId unique id minted per rewrite attempt.
   * @param globalIndex the rewrite group's global index.
   * @param outputSpec the partition spec to write the rewritten files with (the planner's chosen
   *     output spec, which may differ from the table's current default when {@code output-spec-id}
   *     is set or the spec has evolved).
   * @param writeProperties write properties that override the table's for the rewrite operation.
   * @param preserveRowLineage when true (v3 row-lineage tables), read and write each record's
   *     {@code _row_id} / {@code _last_updated_sequence_number} metadata columns to preserve
   *     lineage.
   */
  WriterFactory(
      FileFormat format,
      long targetFileSizeBytes,
      long attemptId,
      int globalIndex,
      String operationId,
      PartitionSpec outputSpec,
      Map<String, String> writeProperties,
      boolean preserveRowLineage) {
    this.format = format;
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.operationId = operationId;
    this.attemptId = attemptId;
    this.globalIndex = globalIndex;
    this.outputSpec = outputSpec;
    this.writeProperties = writeProperties;
    this.preserveRowLineage = preserveRowLineage;
  }

  void init(Table table) {
    if (outputFileFactory == null) {
      this.table = table;

      outputFileFactory =
          OutputFileFactory.builderFor(table, globalIndex, attemptId)
              .format(format)
              .ioSupplier(table::io)
              .defaultSpec(outputSpec)
              .operationId(operationId)
              .build();
    }
  }

  TaskWriter<Record> create() {
    Table table = checkStateNotNull(this.table);
    // Include metadata columns for v3 row-lineage tables
    Schema writeSchema =
        preserveRowLineage ? MetadataColumns.schemaWithRowLineage(table.schema()) : table.schema();
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(writeSchema, outputSpec);

    // User's rewrite write-property overrides table properties
    appenderFactory.setAll(table.properties());
    appenderFactory.setAll(writeProperties);

    if (outputSpec.isUnpartitioned()) {
      return new UnpartitionedWriter<>(
          outputSpec,
          format,
          appenderFactory,
          checkStateNotNull(outputFileFactory),
          table.io(),
          targetFileSizeBytes);
    } else {
      return new RecordPartitionedFanoutWriter(
          outputSpec,
          format,
          appenderFactory,
          checkStateNotNull(outputFileFactory),
          table.io(),
          targetFileSizeBytes,
          table.schema());
    }
  }

  private static class RecordPartitionedFanoutWriter extends PartitionedFanoutWriter<Record> {

    private final PartitionKey partitionKey;
    private final InternalRecordWrapper recordWrapper;
    // Distinct output partitions opened so far
    private final StructLikeSet openPartitions;

    RecordPartitionedFanoutWriter(
        PartitionSpec spec,
        FileFormat format,
        FileAppenderFactory<Record> appenderFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        long targetFileSize,
        Schema schema) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
      this.partitionKey = new PartitionKey(spec, schema);
      this.openPartitions = StructLikeSet.create(spec.partitionType());
      this.recordWrapper = new InternalRecordWrapper(schema.asStruct());
    }

    @Override
    protected PartitionKey partition(Record row) {
      // Track distinct partitions and cap simultaneously-open
      // appenders so a runaway fan-out fails fast with guidance instead of OOMing
      partitionKey.partition(recordWrapper.wrap(row));
      if (!openPartitions.contains(partitionKey)) {
        if (openPartitions.size() >= maxOpenFanoutWriters) {
          throw new IllegalStateException(
              String.format(
                  "Repartitioning compaction fanned out to more than %d simultaneously-open writers on one "
                      + "subgroup. Compact with the table's current spec (so each subgroup stays within one "
                      + "partition), or raise worker memory",
                  maxOpenFanoutWriters));
        }
        openPartitions.add(partitionKey.copy());
        openFanoutWriters.inc();
      }
      return partitionKey;
    }
  }
}
