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
package org.apache.beam.sdk.io.delta;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineException;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.hadoop.conf.Configuration;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A Splittable DoFn that processes {@link DeltaCDCReadTask} elements and reads Change Data Feed
 * files, converting rows to Beam Rows.
 */
@DoFn.BoundedPerElement
class DeltaCDCSourceDoFn extends DoFn<DeltaCDCReadTask, Row> {
  @Nullable Map<String, String> hadoopConfig;
  private transient @Nullable Engine engine;
  private transient @Nullable Configuration conf;

  public DeltaCDCSourceDoFn(@Nullable Map<String, String> hadoopConfig) {
    this.hadoopConfig = hadoopConfig;
  }

  private synchronized Configuration getConfiguration() {
    Configuration localConf = conf;
    if (localConf == null) {
      localConf = new Configuration();
      if (hadoopConfig != null) {
        for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
          localConf.set(entry.getKey(), entry.getValue());
        }
      }
      conf = localConf;
    }
    return localConf;
  }

  private List<Long> getRowGroupSizes(DeltaCDCReadTask task) {
    return task.getRowGroupSizes();
  }

  @GetInitialRestriction
  public OffsetRange getInitialRestriction(@Element DeltaCDCReadTask task) {
    List<Long> rowGroupSizes = getRowGroupSizes(task);
    return new OffsetRange(0L, rowGroupSizes.size());
  }

  @NewTracker
  public DeltaReadTaskTracker newTracker(
      @Restriction OffsetRange restriction, @Element DeltaCDCReadTask task) {
    return new DeltaReadTaskTracker(restriction, getRowGroupSizes(task));
  }

  @Setup
  public void setUp() {
    engine = DefaultEngine.create(getConfiguration());
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element DeltaCDCReadTask task,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<Row> out)
      throws Exception {

    Engine currentEngine = engine;
    if (currentEngine == null) {
      throw new IllegalArgumentException("Expected the engine to not be null");
    }

    SerializableRow originalScanStateRow = task.getScanStateRow();
    StructType logicalTableSchema = ScanStateRow.getLogicalSchema(originalScanStateRow);
    StructType physicalTableSchema = ScanStateRow.getPhysicalDataReadSchema(originalScanStateRow);

    StructType scanStateSchema = originalScanStateRow.getSchema();

    // 1. Build modified scanState and scanFile rows depending on whether we read a CDC file or ADD
    // file.
    io.delta.kernel.data.Row scanStateRow;
    StructType readPhysicalSchema;
    StructType readLogicalSchema;
    Schema beamSchema;

    if (task.isCDC()) {
      readLogicalSchema = appendCDFColumns(logicalTableSchema);
      readPhysicalSchema = appendCDFColumns(physicalTableSchema);
      beamSchema = DeltaIO.ReadRows.convertToBeamSchema(readLogicalSchema);

      HashMap<Integer, Object> valueMap = new HashMap<>();

      // Tracking row level lineage is not needed.
      Map<String, String> config =
          new HashMap<>(ScanStateRow.getConfiguration(originalScanStateRow));
      config.put("delta.enableRowTracking", "false");

      valueMap.put(
          scanStateSchema.indexOf("configuration"), VectorUtils.stringStringMapValue(config));
      valueMap.put(scanStateSchema.indexOf("logicalSchemaJson"), readLogicalSchema.toJson());
      valueMap.put(scanStateSchema.indexOf("physicalSchemaJson"), readPhysicalSchema.toJson());
      valueMap.put(
          scanStateSchema.indexOf("partitionColumns"),
          originalScanStateRow.getArray(scanStateSchema.indexOf("partitionColumns")));
      valueMap.put(
          scanStateSchema.indexOf("minReaderVersion"),
          originalScanStateRow.getInt(scanStateSchema.indexOf("minReaderVersion")));
      valueMap.put(
          scanStateSchema.indexOf("minWriterVersion"),
          originalScanStateRow.getInt(scanStateSchema.indexOf("minWriterVersion")));
      valueMap.put(
          scanStateSchema.indexOf("tablePath"),
          originalScanStateRow.getString(scanStateSchema.indexOf("tablePath")));

      scanStateRow = new ScanStateRow(valueMap);
    } else {
      // For ADD files, we read the table schema and append the CDF columns manually afterwards.
      //      readLogicalSchema = logicalTableSchema;
      readPhysicalSchema = physicalTableSchema;
      beamSchema = DeltaIO.ReadRows.convertToBeamSchema(appendCDFColumns(logicalTableSchema));
      scanStateRow = originalScanStateRow;
    }

    io.delta.kernel.data.Row scanFileRow =
        generateScanFileRow(task.getPath(), task.getPartitionValues());
    FileStatus fileStatus = FileStatus.of(task.getPath(), task.getSize(), task.getTimestamp());

    BeamParquetHandler parquetHandler =
        new BeamParquetHandler(getConfiguration(), currentEngine.getParquetHandler(), tracker);
    BeamEngine beamEngine = new BeamEngine(currentEngine, parquetHandler);

    long currentStartRgIndex = 0L;

    try (CloseableIterator<FileReadResult> fileReadResults =
        parquetHandler.readParquetFiles(
            Utils.singletonCloseableIterator(fileStatus),
            readPhysicalSchema,
            Optional.empty(),
            currentStartRgIndex)) {

      CloseableIterator<ColumnarBatch> physicalData =
          new CloseableIterator<ColumnarBatch>() {
            @Override
            public void close() throws java.io.IOException {}

            @Override
            public boolean hasNext() {
              return fileReadResults.hasNext();
            }

            @Override
            public ColumnarBatch next() {
              return fileReadResults.next().getData();
            }
          };

      try (CloseableIterator<FilteredColumnarBatch> logicalBatches =
          Scan.transformPhysicalData(beamEngine, scanStateRow, scanFileRow, physicalData)) {

        while (logicalBatches.hasNext()) {
          FilteredColumnarBatch batch = logicalBatches.next();
          ColumnarBatch logicalBatch = batch.getData();

          if (!task.isCDC()) {
            // For ADD files, we need to append the constant CDF columns:
            // _change_type = "insert", _commit_version = task.version, _commit_timestamp =
            // task.timestamp
            logicalBatch =
                appendConstantCDFColumns(
                    currentEngine, logicalBatch, task.getVersion(), task.getTimestamp());
          }

          try (CloseableIterator<io.delta.kernel.data.Row> logicalRows = logicalBatch.getRows()) {
            while (logicalRows.hasNext()) {
              io.delta.kernel.data.Row deltaRow = logicalRows.next();
              Row beamRow = DeltaSourceDoFn.toBeamRow(deltaRow, beamSchema);
              String changeType = beamRow.getString("_change_type");
              if (changeType == null) {
                throw new IllegalStateException("Field _change_type must not be null.");
              }
              ValueKind kind = getValueKind(changeType);
              out.builder(beamRow).setValueKind(kind).output();
            }
          }
        }
      }
    }

    return ProcessContinuation.stop();
  }

  private static ValueKind getValueKind(String changeType) {
    switch (changeType) {
      case "insert":
        return ValueKind.INSERT;
      case "delete":
        return ValueKind.DELETE;
      case "update_preimage":
        return ValueKind.UPDATE_BEFORE;
      case "update_postimage":
        return ValueKind.UPDATE_AFTER;
      default:
        throw new IllegalArgumentException("Unsupported change type: " + changeType);
    }
  }

  private static StructType appendCDFColumns(StructType schema) {
    return schema
        .add("_change_type", StringType.STRING, false)
        .add("_commit_version", LongType.LONG, false)
        .add("_commit_timestamp", TimestampType.TIMESTAMP, false);
  }

  private ColumnarBatch appendConstantCDFColumns(
      Engine engine, ColumnarBatch batch, long version, long timestamp) {
    StructType schemaForEval = batch.getSchema();

    ExpressionEvaluator changeTypeGenerator =
        wrapEngineException(
            () ->
                engine
                    .getExpressionHandler()
                    .getEvaluator(schemaForEval, Literal.ofString("insert"), StringType.STRING),
            "Get the expression evaluator for change type");

    ExpressionEvaluator commitVersionGenerator =
        wrapEngineException(
            () ->
                engine
                    .getExpressionHandler()
                    .getEvaluator(schemaForEval, Literal.ofLong(version), LongType.LONG),
            "Get the expression evaluator for commit version");

    ExpressionEvaluator commitTimestampGenerator =
        wrapEngineException(
            () ->
                engine
                    .getExpressionHandler()
                    // Microseconds since epoch is expected for TimestampType
                    .getEvaluator(
                        schemaForEval,
                        Literal.ofTimestamp(timestamp * 1000L),
                        TimestampType.TIMESTAMP),
            "Get the expression evaluator for commit timestamp");

    ColumnVector changeTypeVector =
        wrapEngineException(
            () -> changeTypeGenerator.eval(batch), "Evaluating change type expression");

    ColumnVector commitVersionVector =
        wrapEngineException(
            () -> commitVersionGenerator.eval(batch), "Evaluating commit version expression");

    ColumnVector commitTimestampVector =
        wrapEngineException(
            () -> commitTimestampGenerator.eval(batch), "Evaluating commit timestamp expression");

    int numCols = batch.getSchema().length();
    return batch
        .withNewColumn(
            numCols, new StructField("_change_type", StringType.STRING, false), changeTypeVector)
        .withNewColumn(
            numCols + 1,
            new StructField("_commit_version", LongType.LONG, false),
            commitVersionVector)
        .withNewColumn(
            numCols + 2,
            new StructField("_commit_timestamp", TimestampType.TIMESTAMP, false),
            commitTimestampVector);
  }

  @SuppressWarnings("nullness")
  private static io.delta.kernel.data.Row generateScanFileRow(
      String path, Map<String, String> partitionValues) {
    StructType addFileSchema =
        (StructType) InternalScanFileUtils.SCAN_FILE_SCHEMA.get("add").getDataType();
    MapValue partMapValue = VectorUtils.stringStringMapValue(partitionValues);

    Map<Integer, Object> addFileMap = new HashMap<>();
    addFileMap.put(addFileSchema.indexOf("path"), path);
    addFileMap.put(addFileSchema.indexOf("partitionValues"), partMapValue);
    addFileMap.put(addFileSchema.indexOf("size"), 0L);
    addFileMap.put(addFileSchema.indexOf("modificationTime"), 0L);
    addFileMap.put(addFileSchema.indexOf("dataChange"), true);
    addFileMap.put(addFileSchema.indexOf("deletionVector"), null);

    io.delta.kernel.data.Row addFile = new GenericRow(addFileSchema, addFileMap);

    StructType scanFileSchema = InternalScanFileUtils.SCAN_FILE_SCHEMA;
    Map<Integer, Object> scanFileMap = new HashMap<>();
    scanFileMap.put(scanFileSchema.indexOf("add"), addFile);
    scanFileMap.put(scanFileSchema.indexOf("tableRoot"), "/");

    return new GenericRow(scanFileSchema, scanFileMap);
  }
}
