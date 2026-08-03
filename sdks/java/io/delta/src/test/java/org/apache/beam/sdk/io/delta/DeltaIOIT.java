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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration tests for {@link DeltaIO}. */
@RunWith(JUnit4.class)
public class DeltaIOIT {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaIOIT.class);

  private static final String DEFAULT_BUCKET = "apache-beam-testing-delta-lake";
  @Rule public final TestPipeline readPipeline = TestPipeline.create();
  @Rule public final TestName testName = new TestName();

  private String bucket;
  private String repoPath;
  private String repoPrefix;
  private Storage storage;
  private String version0FilePath;

  private static final Schema ROW_SCHEMA =
      Schema.builder().addInt32Field("id").addStringField("name").build();

  private static final List<Row> TEST_ROWS =
      IntStream.range(0, 100)
          .mapToObj(i -> Row.withSchema(ROW_SCHEMA).addValues(i, "name_" + i).build())
          .collect(Collectors.toList());

  @Before
  public void setup() throws Exception {
    storage = StorageOptions.newBuilder().build().getService();
    long salt = System.currentTimeMillis();

    String tempLocation = readPipeline.getOptions().getTempLocation();
    if (tempLocation != null && tempLocation.startsWith("gs://")) {
      org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath gcsPath =
          org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath.fromUri(tempLocation);
      bucket = gcsPath.getBucket();
      repoPrefix = gcsPath.getObject() + "/delta_io_it/" + testName.getMethodName() + "-" + salt;
    } else {
      bucket = DEFAULT_BUCKET;
      repoPrefix = "delta_io_it/" + testName.getMethodName() + "-" + salt;
    }
    repoPath = "gs://" + bucket + "/" + repoPrefix;

    LOG.info("Generating Delta Lake repository at {}", repoPath);

    Configuration configuration = new Configuration();
    configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    configuration.set(
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    configuration.set("fs.gs.auth.type", "APPLICATION_DEFAULT");
    String project =
        readPipeline
            .getOptions()
            .as(org.apache.beam.sdk.extensions.gcp.options.GcpOptions.class)
            .getProject();
    if (project != null) {
      configuration.set("fs.gs.project.id", project);
    }

    Engine engine = DefaultEngine.create(configuration);
    Table table = Table.forPath(engine, repoPath);

    StructType deltaSchema =
        new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);

    TransactionBuilder txnBuilder =
        table.createTransactionBuilder(engine, "DeltaIOIT", Operation.CREATE_TABLE);
    txnBuilder =
        txnBuilder
            .withSchema(engine, deltaSchema)
            .withTableProperties(
                engine, Collections.singletonMap("delta.enableChangeDataFeed", "true"));
    Transaction txn = txnBuilder.build(engine);
    io.delta.kernel.data.Row txnState = txn.getTransactionState(engine);

    ColumnVector idVector =
        new ColumnVector() {
          @Override
          public DataType getDataType() {
            return IntegerType.INTEGER;
          }

          @Override
          public int getSize() {
            return TEST_ROWS.size();
          }

          @Override
          public void close() {}

          @Override
          public boolean isNullAt(int rowId) {
            return TEST_ROWS.get(rowId).getValue("id") == null;
          }

          @Override
          public int getInt(int rowId) {
            return TEST_ROWS.get(rowId).getInt32("id");
          }
        };

    ColumnVector nameVector =
        new ColumnVector() {
          @Override
          public DataType getDataType() {
            return StringType.STRING;
          }

          @Override
          public int getSize() {
            return TEST_ROWS.size();
          }

          @Override
          public void close() {}

          @Override
          public boolean isNullAt(int rowId) {
            return TEST_ROWS.get(rowId).getValue("name") == null;
          }

          @Override
          public String getString(int rowId) {
            return TEST_ROWS.get(rowId).getString("name");
          }
        };

    ColumnVector[] vectors = new ColumnVector[] {idVector, nameVector};
    ColumnarBatch columnarBatch = new DefaultColumnarBatch(TEST_ROWS.size(), deltaSchema, vectors);
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

    CloseableIterator<io.delta.kernel.data.Row> dataActions =
        Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);

    List<io.delta.kernel.data.Row> addActionsList = new ArrayList<>();
    while (dataActions.hasNext()) {
      addActionsList.add(dataActions.next());
    }

    if (!addActionsList.isEmpty()) {
      io.delta.kernel.data.Row addAction = addActionsList.get(0);
      version0FilePath = addAction.getString(addAction.getSchema().indexOf("path"));
    }

    CloseableIterable<io.delta.kernel.data.Row> dataActionsIterable =
        CloseableIterable.inMemoryIterable(
            io.delta.kernel.internal.util.Utils.toCloseableIterator(addActionsList.iterator()));

    TransactionCommitResult commitResult = txn.commit(engine, dataActionsIterable);

    if (commitResult.getVersion() < 0) {
      throw new RuntimeException("Table creation/write failed");
    }

    LOG.info("Successfully generated Delta Lake repository");
  }

  @After
  public void teardown() {
    if (storage == null) {
      return;
    }
    LOG.info("Cleaning up Delta Lake repository at {}", repoPath);
    try {
      Iterable<Blob> blobs =
          storage.list(bucket, Storage.BlobListOption.prefix(repoPrefix)).getValues();
      blobs.forEach(b -> storage.delete(b.getBlobId()));
    } catch (Exception e) {
      LOG.warn("Failed to clean up GCS repository at {}", repoPath, e);
    }
  }

  @Test
  public void testReadDeltaLakeTable() {
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    hadoopConfig.put(
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    String project =
        readPipeline
            .getOptions()
            .as(org.apache.beam.sdk.extensions.gcp.options.GcpOptions.class)
            .getProject();
    if (project != null) {
      hadoopConfig.put("fs.gs.project.id", project);
    }

    PCollection<Row> output =
        readPipeline
            .apply(
                Managed.read(Managed.DELTA_LAKE)
                    .withConfig(ImmutableMap.of("table", repoPath, "hadoop_config", hadoopConfig)))
            .getSinglePCollection();

    PAssert.that(output).containsInAnyOrder(TEST_ROWS);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadChangesDeltaLake() throws Exception {
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    hadoopConfig.put(
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    hadoopConfig.put("fs.gs.auth.type", "APPLICATION_DEFAULT");
    String project =
        readPipeline
            .getOptions()
            .as(org.apache.beam.sdk.extensions.gcp.options.GcpOptions.class)
            .getProject();
    if (project != null) {
      hadoopConfig.put("fs.gs.project.id", project);
    }

    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    Engine engine = DefaultEngine.create(conf);

    StructType deltaSchema =
        new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);

    // 1. Write version 1 containing cdc actions for testing updates and deletes
    Schema cdcWriteSchema =
        Schema.builder()
            .addField("id", Schema.FieldType.INT32)
            .addField("name", Schema.FieldType.STRING)
            .addField(DeltaIO.CHANGE_TYPE_COLUMN, Schema.FieldType.STRING)
            .addField(DeltaIO.COMMIT_VERSION_COLUMN, Schema.FieldType.INT64)
            .addField(DeltaIO.COMMIT_TIMESTAMP_COLUMN, Schema.FieldType.DATETIME)
            .build();
    StructType cdcWriteDeltaSchema =
        new StructType()
            .add("id", IntegerType.INTEGER)
            .add("name", StringType.STRING)
            .add(DeltaIO.CHANGE_TYPE_COLUMN, StringType.STRING)
            .add(DeltaIO.COMMIT_VERSION_COLUMN, LongType.LONG)
            .add(DeltaIO.COMMIT_TIMESTAMP_COLUMN, TimestampType.TIMESTAMP);

    Row cdcRow1 =
        Row.withSchema(cdcWriteSchema)
            .addValues(0, "name_0", "delete", 1L, new Instant(123456789000L))
            .build();
    Row cdcRow2 =
        Row.withSchema(cdcWriteSchema)
            .addValues(1, "name_1", "update_preimage", 1L, new Instant(123456789000L))
            .build();
    Row cdcRow3 =
        Row.withSchema(cdcWriteSchema)
            .addValues(1, "name_1_updated", "update_postimage", 1L, new Instant(123456789000L))
            .build();

    writeCdcCommit(
        engine,
        repoPath,
        1L,
        deltaSchema,
        null,
        version0FilePath,
        java.util.Arrays.asList(cdcRow1, cdcRow2, cdcRow3),
        cdcWriteDeltaSchema);

    // 2. Read CDF data from table using Managed.read(Managed.DELTA_LAKE_CDC)
    Map<String, Object> readConfig = new HashMap<>();
    readConfig.put("table", repoPath);
    readConfig.put("start_version", 0L);
    readConfig.put("hadoop_config", hadoopConfig);
    readConfig.put(
        "include_metadata_columns",
        java.util.Arrays.asList(
            DeltaIO.CHANGE_TYPE_COLUMN,
            DeltaIO.COMMIT_VERSION_COLUMN,
            DeltaIO.COMMIT_TIMESTAMP_COLUMN));

    PCollection<Row> output =
        readPipeline
            .apply(Managed.read(Managed.DELTA_LAKE_CDC).withConfig(readConfig))
            .getSinglePCollection();

    PCollection<String> formattedOutput =
        output.apply("Format Row with Metadata", ParDo.of(new FormatITRowWithMetadata()));

    // Generate expected outputs for version 0 (inserts of id 0-99)
    List<String> expectedOutputs = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      expectedOutputs.add(String.format("%d:name_%d:insert:v0", i, i));
    }
    // Expected outputs for version 1
    expectedOutputs.add("0:name_0:delete:v1");
    expectedOutputs.add("1:name_1:update_preimage:v1");
    expectedOutputs.add("1:name_1_updated:update_postimage:v1");

    PAssert.that(formattedOutput).containsInAnyOrder(expectedOutputs);

    readPipeline.run().waitUntilFinish();
  }

  private static final class FormatITRowWithMetadata extends DoFn<Row, String> {
    @ProcessElement
    public void process(@Element Row row, OutputReceiver<String> out) {
      out.output(
          String.format(
              "%d:%s:%s:v%d",
              row.getInt32("id"),
              row.getString("name"),
              row.getString(DeltaIO.CHANGE_TYPE_COLUMN),
              row.getInt64(DeltaIO.COMMIT_VERSION_COLUMN)));
    }
  }

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
   */
  private void writeCdcCommit(
      Engine engine,
      String tablePath,
      long expectedVersion,
      StructType deltaSchema,
      @Nullable List<Row> addBeamRows,
      @Nullable String removePath,
      @Nullable List<Row> cdcBeamRows,
      StructType cdcWriteSchema)
      throws Exception {

    Table table = Table.forPath(engine, tablePath);
    TransactionBuilder txnBuilder =
        table.createTransactionBuilder(engine, "DeltaIOIT", Operation.WRITE);
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
          createRemoveAction(removeSchema, removePath, System.currentTimeMillis());
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

      String cdcDir = tablePath + "/_change_data";

      CloseableIterator<DataFileStatus> cdcFiles =
          engine.getParquetHandler().writeParquetFiles(cdcDir, data, Collections.emptyList());

      StructType cdcActionSchema = CDC_ACTION_SCHEMA;
      while (cdcFiles.hasNext()) {
        DataFileStatus cdcFile = cdcFiles.next();
        String relativeCdcPath =
            "_change_data/" + cdcFile.getPath().substring(cdcFile.getPath().lastIndexOf('/') + 1);
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
  }
}
