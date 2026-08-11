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
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
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
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.ExperimentalOptions;
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
      io.delta.kernel.data.Row action = addActionsList.get(0);
      int addOrdinal = action.getSchema().indexOf("add");
      if (addOrdinal < 0) {
        throw new IllegalStateException(
            "Expected append action to contain 'add' field, but it didn't: " + action.getSchema());
      }
      io.delta.kernel.data.Row addAction = action.getStruct(addOrdinal);
      if (addAction == null) {
        throw new IllegalStateException("Action 'add' struct is null");
      }
      int pathOrdinal = addAction.getSchema().indexOf("path");
      if (pathOrdinal < 0) {
        throw new IllegalStateException(
            "'add' action schema does not contain 'path': " + addAction.getSchema());
      }
      version0FilePath = addAction.getString(pathOrdinal);
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
    ExperimentalOptions options = readPipeline.getOptions().as(ExperimentalOptions.class);
    ExperimentalOptions.addExperiment(options, "use_runner_v2");

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
    ExperimentalOptions options = readPipeline.getOptions().as(ExperimentalOptions.class);
    List<String> experiments = options.getExperiments();
    if (experiments != null) {
      List<String> modifiableExperiments = new java.util.ArrayList<>(experiments);
      // TODO: remove this when Runner v2 supports elements that includes CDC metadata
      // (ValueKind).
      modifiableExperiments.remove("use_runner_v2");
      options.setExperiments(modifiableExperiments);
    }

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

    DeltaWriteTestUtils.writeCdcCommit(
        engine,
        repoPath,
        1L,
        System.currentTimeMillis(),
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
}
