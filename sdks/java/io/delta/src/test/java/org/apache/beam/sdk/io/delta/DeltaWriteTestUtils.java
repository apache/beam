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
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/** Utility class for writing test commits (appends and CDC actions) to Delta tables in tests. */
public final class DeltaWriteTestUtils {

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
        if (dataType instanceof DateType) {
          Object val = rows.get(rowId).getValue(fieldIndex);
          if (val instanceof java.time.LocalDate) {
            return (int) ((java.time.LocalDate) val).toEpochDay();
          }
        }
        return rows.get(rowId).getInt32(fieldIndex);
      }

      @Override
      @SuppressWarnings("PatternMatchingInstanceof") // Not available as spotless is on still Java11
      public long getLong(int rowId) {
        if (dataType instanceof TimestampType) {
          Object val = rows.get(rowId).getValue(fieldIndex);
          if (val instanceof java.time.Instant) {
            java.time.Instant inst = (java.time.Instant) val;
            return inst.getEpochSecond() * 1_000_000L + inst.getNano() / 1000L;
          } else {
            throw new RuntimeException(
                "Unexpected value for field " + rowId + " of type 'TimestampType': " + val);
          }
        }
        if (dataType instanceof TimestampNTZType) {
          Object val = rows.get(rowId).getValue(fieldIndex);
          if (val instanceof java.time.LocalDateTime) {
            java.time.Instant inst =
                ((java.time.LocalDateTime) val).toInstant(java.time.ZoneOffset.UTC);
            return inst.getEpochSecond() * 1_000_000L + inst.getNano() / 1000L;
          }
        }
        return rows.get(rowId).getInt64(fieldIndex);
      }

      @Override
      public double getDouble(int rowId) {
        return rows.get(rowId).getDouble(fieldIndex);
      }

      @Override
      public float getFloat(int rowId) {
        return rows.get(rowId).getFloat(fieldIndex);
      }

      @Override
      public short getShort(int rowId) {
        return rows.get(rowId).getInt16(fieldIndex);
      }

      @Override
      public byte getByte(int rowId) {
        return rows.get(rowId).getByte(fieldIndex);
      }

      @Override
      public byte[] getBinary(int rowId) {
        return rows.get(rowId).getBytes(fieldIndex);
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
  public static List<String> writeAppendCommit(
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
  public static void writeCdcCommit(
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

  /**
   * Writes a metadata-only Delta commit (i.e. a commit without any data files) that sets the schema
   * of the table.
   *
   * <p>If {@code expectedVersion} is 0, a new table is created with the given schema, with Change
   * Data Feed and column mapping (name mode) enabled. Otherwise, the schema of the existing table
   * is evolved to the given schema.
   *
   * <p>Note on why this is metadata-only: The Delta Lake Kernel API only supports schema evolution
   * for tables that have column mapping enabled, but it does not yet support writing data into such
   * tables.
   *
   * <p>When evolving the schema, the given schema must preserve the field metadata (column mapping
   * IDs and physical names) of existing columns, for example, by deriving the new schema from the
   * schema of the latest snapshot. Fields without such metadata are considered to be new columns.
   *
   * @param engine the Delta Lake {@link Engine} instance to use
   * @param tablePath the path of the Delta table to write to
   * @param expectedVersion the expected version of the commit to be created
   * @param timestamp the timestamp of the commit file
   * @param deltaSchema the new schema of the Delta table
   * @throws Exception if any error occurs during commit
   */
  public static void writeSchemaCommit(
      Engine engine, String tablePath, long expectedVersion, long timestamp, StructType deltaSchema)
      throws Exception {

    Table table = Table.forPath(engine, tablePath);
    TransactionBuilder txnBuilder;
    if (expectedVersion == 0) {
      Map<String, String> tableProperties = new HashMap<>();
      tableProperties.put("delta.enableChangeDataFeed", "true");
      tableProperties.put("delta.columnMapping.mode", "name");
      txnBuilder =
          table
              .createTransactionBuilder(engine, "DeltaTestUtils", Operation.CREATE_TABLE)
              .withTableProperties(engine, tableProperties);
    } else {
      txnBuilder =
          table.createTransactionBuilder(engine, "DeltaTestUtils", Operation.MANUAL_UPDATE);
    }
    Transaction txn = txnBuilder.withSchema(engine, deltaSchema).build(engine);

    TransactionCommitResult result = txn.commit(engine, CloseableIterable.emptyIterable());
    org.junit.Assert.assertEquals(expectedVersion, result.getVersion());
    setCommitFileTimestamp(tablePath, expectedVersion, timestamp);
  }

  /**
   * Writes a metadata-only Delta commit (i.e. a commit without any data files) that sets the given
   * table properties of an existing table.
   *
   * <p>For example, this can be used to enable column mapping on an existing table by setting
   * {@code delta.columnMapping.mode} to {@code name}.
   *
   * @param engine the Delta Lake {@link Engine} instance to use
   * @param tablePath the path of the Delta table to write to
   * @param expectedVersion the expected version of the commit to be created
   * @param timestamp the timestamp of the commit file
   * @param tableProperties the table properties to set
   * @throws Exception if any error occurs during commit
   */
  public static void writeTablePropertiesCommit(
      Engine engine,
      String tablePath,
      long expectedVersion,
      long timestamp,
      Map<String, String> tableProperties)
      throws Exception {

    Transaction txn =
        Table.forPath(engine, tablePath)
            .createTransactionBuilder(engine, "DeltaTestUtils", Operation.MANUAL_UPDATE)
            .withTableProperties(engine, tableProperties)
            .build(engine);

    TransactionCommitResult result = txn.commit(engine, CloseableIterable.emptyIterable());
    org.junit.Assert.assertEquals(expectedVersion, result.getVersion());
    setCommitFileTimestamp(tablePath, expectedVersion, timestamp);
  }

  /**
   * Sets the modification time of the commit file of the given version, which determines the
   * timestamp of the commit, to the given timestamp. This is only supported for local tables.
   */
  private static void setCommitFileTimestamp(String tablePath, long version, long timestamp) {
    if (!tablePath.startsWith("gs://")
        && !tablePath.startsWith("s3://")
        && !tablePath.startsWith("hdfs://")) {
      File commitFile =
          new File(new File(tablePath, "_delta_log"), String.format("%020d.json", version));
      commitFile.setLastModified(timestamp);
    }
  }

  /**
   * Sets up a Delta table with two commit versions containing test rows.
   *
   * <p>Version 0 is committed at timestamp 100000000000L with rows ["row-1", "row-2"]. Version 1 is
   * committed at timestamp 200000000000L with row ["row-3"].
   *
   * @param engine the Delta Lake {@link Engine} instance to use
   * @param tablePath the path of the Delta table to create
   * @return the list of {@link Row} objects written [row1, row2, row3]
   * @throws Exception if any error occurs during write or commit
   */
  static List<Row> setupTwoVersionTable(Engine engine, String tablePath) throws Exception {
    Schema schema = Schema.builder().addField("name", Schema.FieldType.STRING).build();
    Row row1 = Row.withSchema(schema).addValues("row-1").build();
    Row row2 = Row.withSchema(schema).addValues("row-2").build();
    Row row3 = Row.withSchema(schema).addValues("row-3").build();
    StructType deltaSchema = new StructType().add("name", StringType.STRING);

    // Commit version 0
    writeAppendCommit(
        engine, tablePath, 0L, 100000000000L, deltaSchema, java.util.Arrays.asList(row1, row2));

    // Commit version 1
    writeAppendCommit(
        engine, tablePath, 1L, 200000000000L, deltaSchema, java.util.Arrays.asList(row3));

    return java.util.Arrays.asList(row1, row2, row3);
  }

  /**
   * Sets up a Delta table with two metadata-only commit versions where a column is dropped, which
   * is a non-additive schema change.
   *
   * <p>Version 0 is committed at timestamp 100000000000L and creates the table with schema {@code
   * (name STRING, age INTEGER)}. Version 1 is committed at timestamp 200000000000L and drops the
   * {@code age} column, resulting in schema {@code (name STRING)}.
   *
   * @param engine the Delta Lake {@link Engine} instance to use
   * @param tablePath the path of the Delta table to create
   * @throws Exception if any error occurs during commit
   */
  static void setupDroppedColumnTable(Engine engine, String tablePath) throws Exception {
    // Commit version 0
    writeSchemaCommit(
        engine,
        tablePath,
        0L,
        100000000000L,
        new StructType().add("name", StringType.STRING).add("age", IntegerType.INTEGER));

    // Commit version 1. The new schema keeps the existing 'name' field of the latest schema of the
    // table to preserve its column mapping metadata.
    StructType latestSchema =
        Table.forPath(engine, tablePath).getLatestSnapshot(engine).getSchema();
    writeSchemaCommit(
        engine, tablePath, 1L, 200000000000L, new StructType().add(latestSchema.get("name")));
  }
}
