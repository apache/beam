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

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

/** Utility class for writing test commits (appends and CDC actions) to Delta tables in tests. */
final class DeltaWriteTestUtils {

  private DeltaWriteTestUtils() {}

  private static final StructType CDC_ACTION_SCHEMA =
      new StructType()
          .add("path", StringType.STRING, false)
          .add("partitionValues", new MapType(StringType.STRING, StringType.STRING, false), false)
          .add("size", LongType.LONG, false)
          .add("dataChange", BooleanType.BOOLEAN, false);

  private static StructType getCustomSingleActionSchema() {
    StructType originalSchema = io.delta.kernel.internal.actions.SingleAction.FULL_SCHEMA;
    List<StructField> fields = new ArrayList<>();
    for (StructField field : originalSchema.fields()) {
      if (field.getName().equals("cdc")) {
        fields.add(new StructField("cdc", CDC_ACTION_SCHEMA, true));
      } else {
        fields.add(field);
      }
    }
    return new StructType(fields);
  }

  private static io.delta.kernel.data.Row createSingleAction(
      StructType customSingleActionSchema, String actionName, io.delta.kernel.data.Row actionRow) {
    Map<Integer, Object> values = new HashMap<>();
    values.put(customSingleActionSchema.indexOf(actionName), actionRow);
    return new GenericRow(customSingleActionSchema, values);
  }

  private static io.delta.kernel.data.Row createRemoveAction(
      StructType removeSchema, String path, long deletionTimestamp) {
    Map<Integer, Object> values = new HashMap<>();
    values.put(removeSchema.indexOf("path"), path);
    values.put(removeSchema.indexOf("deletionTimestamp"), deletionTimestamp);
    values.put(removeSchema.indexOf("dataChange"), true);
    values.put(removeSchema.indexOf("size"), 100L);
    return new GenericRow(removeSchema, values);
  }

  private static io.delta.kernel.data.Row createCdcAction(
      StructType cdcSchema, String path, long size) {
    Map<Integer, Object> values = new HashMap<>();
    values.put(cdcSchema.indexOf("path"), path);
    values.put(
        cdcSchema.indexOf("partitionValues"),
        io.delta.kernel.internal.util.VectorUtils.stringStringMapValue(Collections.emptyMap()));
    values.put(cdcSchema.indexOf("size"), size);
    values.put(cdcSchema.indexOf("dataChange"), true);
    return new GenericRow(cdcSchema, values);
  }

  private static ColumnVector createColumnVector(
      List<Row> rows, int fieldIndex, DataType dataType) {
    return new ColumnVector() {
      @Override
      public DataType getDataType() {
        return dataType;
      }

      @Override
      public int getSize() {
        return rows.size();
      }

      @Override
      public void close() {}

      @Override
      public boolean isNullAt(int rowId) {
        return rows.get(rowId).getValue(fieldIndex) == null;
      }

      @Override
      public boolean getBoolean(int rowId) {
        return rows.get(rowId).getBoolean(fieldIndex);
      }

      @Override
      public int getInt(int rowId) {
        return rows.get(rowId).getInt32(fieldIndex);
      }

      @Override
      public long getLong(int rowId) {
        if (dataType instanceof TimestampType) {
          Instant instant = rows.get(rowId).getDateTime(fieldIndex).toInstant();
          return instant.getMillis() * 1000L;
        }
        return rows.get(rowId).getInt64(fieldIndex);
      }

      @Override
      public String getString(int rowId) {
        return rows.get(rowId).getString(fieldIndex);
      }
    };
  }

  /**
   * Writes a Delta commit containing append actions.
   *
   * @param engine the Delta Lake {@link Engine} instance to use
   * @param tablePath the path of the Delta table to write to
   * @param expectedVersion the expected version of the commit to be created
   * @param timestamp the timestamp of the commit file
   * @param deltaSchema the schema of the Delta table
   * @param beamRows the rows to write
   * @return the list of names of the written Parquet data files
   * @throws Exception if any error occurs during write or commit
   */
  static List<String> writeAppendCommit(
      Engine engine,
      String tablePath,
      long expectedVersion,
      long timestamp,
      StructType deltaSchema,
      List<Row> beamRows)
      throws Exception {

    Table table = Table.forPath(engine, tablePath);
    TransactionBuilder txnBuilder =
        table.createTransactionBuilder(engine, "DeltaTestUtils", Operation.WRITE);
    if (expectedVersion == 0) {
      txnBuilder =
          txnBuilder
              .withSchema(engine, deltaSchema)
              .withTableProperties(
                  engine, Collections.singletonMap("delta.enableChangeDataFeed", "true"));
    }
    Transaction txn = txnBuilder.build(engine);
    io.delta.kernel.data.Row txnState = txn.getTransactionState(engine);

    ColumnVector[] vectors = new ColumnVector[deltaSchema.fields().size()];
    for (int i = 0; i < deltaSchema.fields().size(); i++) {
      StructField field = deltaSchema.fields().get(i);
      vectors[i] = createColumnVector(beamRows, i, field.getDataType());
    }
    ColumnarBatch columnarBatch = new DefaultColumnarBatch(beamRows.size(), deltaSchema, vectors);
    FilteredColumnarBatch filteredBatch =
        new FilteredColumnarBatch(columnarBatch, Optional.empty());
    CloseableIterator<FilteredColumnarBatch> data =
        io.delta.kernel.internal.util.Utils.toCloseableIterator(
            Collections.singletonList(filteredBatch).iterator());
    CloseableIterator<FilteredColumnarBatch> physicalData =
        Transaction.transformLogicalData(engine, txnState, data, Collections.emptyMap());
    DataWriteContext writeContext =
        Transaction.getWriteContext(engine, txnState, Collections.emptyMap());
    CloseableIterator<DataFileStatus> dataFiles =
        engine
            .getParquetHandler()
            .writeParquetFiles(
                writeContext.getTargetDirectory(),
                physicalData,
                writeContext.getStatisticsColumns());

    List<String> writtenFiles = new ArrayList<>();
    List<DataFileStatus> filesList = new ArrayList<>();
    while (dataFiles.hasNext()) {
      DataFileStatus file = dataFiles.next();
      filesList.add(file);
      writtenFiles.add(new File(file.getPath()).getName());
    }
    CloseableIterator<DataFileStatus> dataFilesCopy =
        io.delta.kernel.internal.util.Utils.toCloseableIterator(filesList.iterator());

    CloseableIterator<io.delta.kernel.data.Row> dataActions =
        Transaction.generateAppendActions(engine, txnState, dataFilesCopy, writeContext);

    TransactionCommitResult result =
        txn.commit(engine, CloseableIterable.inMemoryIterable(dataActions));
    org.junit.Assert.assertEquals(expectedVersion, result.getVersion());
    if (!tablePath.startsWith("gs://")
        && !tablePath.startsWith("s3://")
        && !tablePath.startsWith("hdfs://")) {
      File commitFile =
          new File(new File(tablePath, "_delta_log"), String.format("%020d.json", expectedVersion));
      commitFile.setLastModified(timestamp);
    }
    return writtenFiles;
  }

  /**
   * Writes a Delta commit containing CDC actions (simulating updates/deletes).
   *
   * <p>Note on why this is manual: In a standard Spark or Flink writer, setting the table property
   * {@code "delta.enableChangeDataFeed" = "true"} automatically instructs the engine to compute and
   * write the change data files to {@code _change_data/} and append the {@code cdc} actions to the
   * commit log whenever DML statements (like UPDATE/DELETE) are executed.
   *
   * <p>However, we are using the Delta Lake Kernel API which does not contain an SQL execution
   * engine or a DML parser. Thus, it cannot automatically compute which rows were deleted or
   * updated. To generate a realistic integration test dataset, we must manually construct these
   * change records, write them into the GCS {@code _change_data/} directory using the low-level
   * parquet handler, and manually register them as {@code cdc} actions in the committed
   * transaction.
   *
   * @param engine the Delta Lake {@link Engine} instance to use
   * @param tablePath the path of the Delta table to write to
   * @param expectedVersion the expected version of the commit to be created
   * @param timestamp the timestamp of the commit file
   * @param deltaSchema the schema of the Delta table
   * @param addBeamRows the optional list of rows to add in this commit
   * @param removePath the optional path of the file to remove in this commit
   * @param cdcBeamRows the optional list of CDC rows to write
   * @param cdcWriteSchema the schema used for writing the CDC files
   * @throws Exception if any error occurs during write or commit
   */
  static void writeCdcCommit(
      Engine engine,
      String tablePath,
      long expectedVersion,
      long timestamp,
      StructType deltaSchema,
      @Nullable List<Row> addBeamRows,
      @Nullable String removePath,
      @Nullable List<Row> cdcBeamRows,
      StructType cdcWriteSchema)
      throws Exception {

    Table table = Table.forPath(engine, tablePath);
    TransactionBuilder txnBuilder =
        table.createTransactionBuilder(engine, "DeltaTestUtils", Operation.WRITE);
    Transaction txn = txnBuilder.build(engine);
    io.delta.kernel.data.Row txnState = txn.getTransactionState(engine);

    StructType customSingleActionSchema = getCustomSingleActionSchema();
    List<io.delta.kernel.data.Row> commitActions = new ArrayList<>();

    if (addBeamRows != null && !addBeamRows.isEmpty()) {
      ColumnVector[] vectors = new ColumnVector[deltaSchema.fields().size()];
      for (int i = 0; i < deltaSchema.fields().size(); i++) {
        StructField field = deltaSchema.fields().get(i);
        vectors[i] = createColumnVector(addBeamRows, i, field.getDataType());
      }
      ColumnarBatch columnarBatch =
          new DefaultColumnarBatch(addBeamRows.size(), deltaSchema, vectors);
      FilteredColumnarBatch filteredBatch =
          new FilteredColumnarBatch(columnarBatch, Optional.empty());
      CloseableIterator<FilteredColumnarBatch> data =
          io.delta.kernel.internal.util.Utils.toCloseableIterator(
              Collections.singletonList(filteredBatch).iterator());
      CloseableIterator<FilteredColumnarBatch> physicalData =
          Transaction.transformLogicalData(engine, txnState, data, Collections.emptyMap());
      DataWriteContext writeContext =
          Transaction.getWriteContext(engine, txnState, Collections.emptyMap());
      CloseableIterator<DataFileStatus> dataFiles =
          engine
              .getParquetHandler()
              .writeParquetFiles(
                  writeContext.getTargetDirectory(),
                  physicalData,
                  writeContext.getStatisticsColumns());
      CloseableIterator<io.delta.kernel.data.Row> addActions =
          Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);
      while (addActions.hasNext()) {
        commitActions.add(addActions.next());
      }
    }

    if (removePath != null) {
      StructType removeSchema =
          (StructType)
              io.delta.kernel.internal.actions.SingleAction.FULL_SCHEMA
                  .fields()
                  .get(io.delta.kernel.internal.actions.SingleAction.REMOVE_FILE_ORDINAL)
                  .getDataType();
      io.delta.kernel.data.Row removeAction =
          createRemoveAction(removeSchema, removePath, timestamp);
      commitActions.add(createSingleAction(customSingleActionSchema, "remove", removeAction));
    }

    if (cdcBeamRows != null && !cdcBeamRows.isEmpty()) {
      ColumnVector[] vectors = new ColumnVector[cdcWriteSchema.fields().size()];
      for (int i = 0; i < cdcWriteSchema.fields().size(); i++) {
        StructField field = cdcWriteSchema.fields().get(i);
        vectors[i] = createColumnVector(cdcBeamRows, i, field.getDataType());
      }
      ColumnarBatch columnarBatch =
          new DefaultColumnarBatch(cdcBeamRows.size(), cdcWriteSchema, vectors);
      FilteredColumnarBatch filteredBatch =
          new FilteredColumnarBatch(columnarBatch, Optional.empty());
      CloseableIterator<FilteredColumnarBatch> data =
          io.delta.kernel.internal.util.Utils.toCloseableIterator(
              Collections.singletonList(filteredBatch).iterator());

      String cdcDir = new org.apache.hadoop.fs.Path(tablePath, "_change_data").toString();

      CloseableIterator<DataFileStatus> cdcFiles =
          engine.getParquetHandler().writeParquetFiles(cdcDir, data, Collections.emptyList());

      StructType cdcActionSchema = CDC_ACTION_SCHEMA;
      while (cdcFiles.hasNext()) {
        DataFileStatus cdcFile = cdcFiles.next();
        String relativeCdcPath = "_change_data/" + new File(cdcFile.getPath()).getName();
        io.delta.kernel.data.Row cdcAction =
            createCdcAction(cdcActionSchema, relativeCdcPath, cdcFile.getSize());
        commitActions.add(createSingleAction(customSingleActionSchema, "cdc", cdcAction));
      }
    }

    TransactionCommitResult result =
        txn.commit(
            engine,
            CloseableIterable.inMemoryIterable(
                io.delta.kernel.internal.util.Utils.toCloseableIterator(commitActions.iterator())));
    org.junit.Assert.assertEquals(expectedVersion, result.getVersion());
    if (!tablePath.startsWith("gs://")
        && !tablePath.startsWith("s3://")
        && !tablePath.startsWith("hdfs://")) {
      File commitFile =
          new File(new File(tablePath, "_delta_log"), String.format("%020d.json", expectedVersion));
      commitFile.setLastModified(timestamp);
    }
  }
}
