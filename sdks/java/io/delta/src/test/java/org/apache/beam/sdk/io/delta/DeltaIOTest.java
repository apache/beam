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
import io.delta.kernel.data.MapValue;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
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
import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.delta.DeltaIO.ReadRows;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit and local integration tests for {@link DeltaIO}. */
@RunWith(JUnit4.class)
public class DeltaIOTest {

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TestPipeline filteringPipeline = TestPipeline.create();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testReadRowsBuilderAndGetters() {
    String tablePath = "/path/to/table";
    long version = 5L;
    String timestamp = "2026-05-20T15:43:26Z";
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("fs.defaultFS", "file:///");

    ReadRows readRows =
        DeltaIO.readRows()
            .from(tablePath)
            .withVersion(version)
            .withTimestamp(timestamp)
            .withConfig(hadoopConfig);

    Assert.assertEquals(tablePath, readRows.getTablePath());
    Assert.assertEquals(Long.valueOf(version), readRows.getVersion());
    Assert.assertEquals(timestamp, readRows.getTimestamp());
    Assert.assertEquals(hadoopConfig, readRows.getHadoopConfig());
  }

  @Test
  public void testReadRowsNullDefaults() {
    ReadRows readRows = DeltaIO.readRows();

    Assert.assertNull(readRows.getTablePath());
    Assert.assertNull(readRows.getVersion());
    Assert.assertNull(readRows.getTimestamp());
    Assert.assertNull(readRows.getHadoopConfig());
  }

  @Test
  public void testPrintScanStateSchema() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-schema");
    File logDir = new File(tableDir, "_delta_log");
    logDir.mkdirs();
    File commitFile = new File(logDir, "00000000000000000000.json");

    String commitContent =
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
            + "{\"metaData\":{\"id\":\"test-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[],\"configuration\":{},\"createdAt\":123456789}}\n"
            + "{\"add\":{\"path\":\"part-00000.parquet\",\"partitionValues\":{},\"size\":100,\"modificationTime\":123456789,\"dataChange\":true}}";

    Files.write(commitFile.toPath(), commitContent.getBytes(StandardCharsets.UTF_8));

    io.delta.kernel.defaults.engine.DefaultEngine engine =
        io.delta.kernel.defaults.engine.DefaultEngine.create(
            new org.apache.hadoop.conf.Configuration());
    io.delta.kernel.Table table = io.delta.kernel.Table.forPath(engine, tableDir.getAbsolutePath());
    io.delta.kernel.Snapshot snapshot = table.getLatestSnapshot(engine);
    io.delta.kernel.Scan scan = snapshot.getScanBuilder().build();

    try (io.delta.kernel.utils.CloseableIterator<io.delta.kernel.data.FilteredColumnarBatch>
        scanFiles = scan.getScanFiles(engine)) {
      while (scanFiles.hasNext()) {
        io.delta.kernel.data.FilteredColumnarBatch batch = scanFiles.next();
        try (io.delta.kernel.utils.CloseableIterator<io.delta.kernel.data.Row> rows =
            batch.getRows()) {
          while (rows.hasNext()) {
            io.delta.kernel.data.Row row = rows.next();
            verifySerialization(row);
          }
        }
      }
    }
  }

  private void verifySerialization(io.delta.kernel.data.Row row) throws Exception {
    SerializableRow serializableRow = new SerializableRow(row);

    // Serialize using standard Java Serialization
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos)) {
      oos.writeObject(serializableRow);
    }

    byte[] bytes = baos.toByteArray();

    // Deserialize
    SerializableRow deserializedRow;
    java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(bytes);
    try (java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bais)) {
      deserializedRow = (SerializableRow) ois.readObject();
    }

    // Assert equals
    org.junit.Assert.assertEquals(serializableRow, deserializedRow);
    org.junit.Assert.assertEquals(
        row.getSchema().toString(), deserializedRow.getSchema().toString());

    // Deep verify fields
    io.delta.kernel.types.StructType schema = row.getSchema();
    for (int i = 0; i < schema.fields().size(); i++) {
      org.junit.Assert.assertEquals(row.isNullAt(i), deserializedRow.isNullAt(i));
      if (!row.isNullAt(i)) {
        io.delta.kernel.types.DataType type = schema.fields().get(i).getDataType();
        if (type instanceof io.delta.kernel.types.StringType) {
          org.junit.Assert.assertEquals(row.getString(i), deserializedRow.getString(i));
        } else if (type instanceof io.delta.kernel.types.LongType) {
          org.junit.Assert.assertEquals(row.getLong(i), deserializedRow.getLong(i));
        }
      }
    }
  }

  @Test
  public void testCreateReadTasksDoFn() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table");
    File logDir = new File(tableDir, "_delta_log");
    logDir.mkdirs();
    File commitFile = new File(logDir, "00000000000000000000.json");

    String commitContent =
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
            + "{\"metaData\":{\"id\":\"test-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[],\"configuration\":{},\"createdAt\":123456789}}\n"
            + "{\"add\":{\"path\":\"part-00000.parquet\",\"partitionValues\":{},\"size\":100,\"modificationTime\":123456789,\"dataChange\":true}}";

    Files.write(commitFile.toPath(), commitContent.getBytes(StandardCharsets.UTF_8));

    Schema schema = Schema.builder().addField("name", Schema.FieldType.STRING).build();
    Row dummyRow = Row.withSchema(schema).addValues("test-name").build();
    writeParquetFile(new File(tableDir, "part-00000.parquet"), dummyRow);

    PCollection<DeltaReadTask> output =
        writePipeline
            .apply(Create.of(tableDir.getAbsolutePath()))
            .apply(ParDo.of(new CreateReadTasksDoFn(null)));

    PCollection<String> paths =
        output.apply(
            org.apache.beam.sdk.transforms.MapElements.into(
                    org.apache.beam.sdk.values.TypeDescriptors.strings())
                .via(
                    task ->
                        io.delta.kernel.internal.InternalScanFileUtils.getAddFileStatus(
                                task.getScanFileRows().get(0))
                            .getPath()));

    PAssert.that(paths)
        .containsInAnyOrder("file:" + tableDir.getAbsolutePath() + "/part-00000.parquet");

    writePipeline.run().waitUntilFinish();
  }

  @Test
  public void testCreateReadTasksDoFnGrouping() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-grouping");
    File logDir = new File(tableDir, "_delta_log");
    logDir.mkdirs();
    File commitFile = new File(logDir, "00000000000000000000.json");

    String commitContent =
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
            + "{\"metaData\":{\"id\":\"test-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[],\"configuration\":{},\"createdAt\":123456789}}\n"
            + "{\"add\":{\"path\":\"part-00001.parquet\",\"partitionValues\":{},\"size\":400000000,\"modificationTime\":123456789,\"dataChange\":true}}\n"
            + "{\"add\":{\"path\":\"part-00002.parquet\",\"partitionValues\":{},\"size\":400000000,\"modificationTime\":123456789,\"dataChange\":true}}\n"
            + "{\"add\":{\"path\":\"part-00003.parquet\",\"partitionValues\":{},\"size\":1200000000,\"modificationTime\":123456789,\"dataChange\":true}}\n"
            + "{\"add\":{\"path\":\"part-00004.parquet\",\"partitionValues\":{},\"size\":100,\"modificationTime\":123456789,\"dataChange\":true}}";

    Files.write(commitFile.toPath(), commitContent.getBytes(StandardCharsets.UTF_8));

    Schema schema = Schema.builder().addField("name", Schema.FieldType.STRING).build();
    Row dummyRow = Row.withSchema(schema).addValues("test-name").build();
    writeParquetFile(new File(tableDir, "part-00001.parquet"), dummyRow);
    writeParquetFile(new File(tableDir, "part-00002.parquet"), dummyRow);
    writeParquetFile(new File(tableDir, "part-00003.parquet"), dummyRow);
    writeParquetFile(new File(tableDir, "part-00004.parquet"), dummyRow);

    PCollection<DeltaReadTask> output =
        writePipeline
            .apply("Create Grouping Input", Create.of(tableDir.getAbsolutePath()))
            .apply("Plan Grouped Files", ParDo.of(new CreateReadTasksDoFn(null)));

    PCollection<String> taskDescriptions =
        output.apply(
            org.apache.beam.sdk.transforms.MapElements.into(
                    org.apache.beam.sdk.values.TypeDescriptors.strings())
                .via(
                    task -> {
                      StringBuilder sb = new StringBuilder();
                      for (SerializableRow row : task.getScanFileRows()) {
                        if (sb.length() > 0) {
                          sb.append(",");
                        }
                        String fullPath =
                            io.delta.kernel.internal.InternalScanFileUtils.getAddFileStatus(row)
                                .getPath();
                        String filename = fullPath.substring(fullPath.lastIndexOf('/') + 1);
                        sb.append(filename);
                      }
                      return sb.toString();
                    }));

    PAssert.that(taskDescriptions)
        .containsInAnyOrder(
            "part-00001.parquet,part-00002.parquet", "part-00003.parquet", "part-00004.parquet");

    writePipeline.run().waitUntilFinish();
  }

  @Test
  public void testFullPipelineRead() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-full");

    // 1. Write a Parquet file using Beam
    Schema schema = Schema.builder().addField("name", Schema.FieldType.STRING).build();
    Row row = Row.withSchema(schema).addValues("test-name").build();

    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
    GenericRecord record = AvroUtils.toGenericRecord(row, avroSchema);

    writePipeline
        .apply("Create Input", Create.of(record).withCoder(AvroCoder.of(avroSchema)))
        .apply(
            "Write Parquet",
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(avroSchema))
                .to(tableDir.getAbsolutePath() + "/")
                .withNaming(
                    (BoundedWindow window,
                        PaneInfo paneInfo,
                        int numShards,
                        int shardIndex,
                        Compression compression) -> "part-00000.parquet"));

    writePipeline.run().waitUntilFinish();

    File parquetFile = new File(tableDir, "part-00000.parquet");
    byte[] fileBytes = Files.readAllBytes(parquetFile.toPath());

    // 2. Create the Delta log
    File logDir = new File(tableDir, "_delta_log");
    logDir.mkdirs();
    File commitFile = new File(logDir, "00000000000000000000.json");

    String commitContent =
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
            + "{\"metaData\":{\"id\":\"test-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[],\"configuration\":{},\"createdAt\":123456789}}\n"
            + "{\"add\":{\"path\":\"part-00000.parquet\",\"partitionValues\":{},\"size\":"
            + fileBytes.length
            + ",\"modificationTime\":123456789,\"dataChange\":true}}";

    Files.write(commitFile.toPath(), commitContent.getBytes(StandardCharsets.UTF_8));

    // 3. Read it using DeltaIO
    PCollection<Row> output =
        readPipeline.apply(DeltaIO.readRows().from(tableDir.getAbsolutePath()));

    PAssert.that(output).containsInAnyOrder(row);

    readPipeline.run().waitUntilFinish();
  }

  private byte[] writeParquetFile(File file, Row row) throws Exception {
    org.apache.avro.Schema avroSchema =
        org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils.toAvroSchema(row.getSchema());
    org.apache.avro.generic.GenericRecord record =
        org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils.toGenericRecord(
            row, avroSchema);
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
    try (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericRecord> writer =
        org.apache.parquet.avro.AvroParquetWriter.<org.apache.avro.generic.GenericRecord>builder(
                path)
            .withSchema(avroSchema)
            .withConf(new org.apache.hadoop.conf.Configuration())
            .build()) {
      writer.write(record);
    }
    return java.nio.file.Files.readAllBytes(file.toPath());
  }

  @Test
  public void testManagedDeltaRead() throws Exception {
    File tableDir = tempFolder.newFolder("managed-delta-table");
    Engine engine = DefaultEngine.create(new org.apache.hadoop.conf.Configuration());

    Schema schema = Schema.builder().addField("name", Schema.FieldType.STRING).build();
    Row row = Row.withSchema(schema).addValues("test-name").build();
    StructType deltaSchema = new StructType().add("name", StringType.STRING);

    writeAppendCommit(
        engine,
        tableDir.getAbsolutePath(),
        0L,
        123456789L,
        deltaSchema,
        Collections.singletonList(row));

    // 3. Read it using Managed
    PCollection<Row> output =
        readPipeline
            .apply(
                Managed.read(Managed.DELTA_LAKE)
                    .withConfig(ImmutableMap.of("table", tableDir.getAbsolutePath())))
            .getSinglePCollection();

    PAssert.that(output).containsInAnyOrder(row);

    readPipeline.run().waitUntilFinish();
  }

  @Test
  @org.junit.Ignore("Manual integration test with external local table")
  public void testReadingLocalTable() throws Exception {
    PCollection<Row> output =
        readPipeline.apply(
            DeltaIO.readRows()
                .from("/Users/chamikara/testing/delta_lake/test_repo/test_table_1_gb"));
    PCollection<Long> counted = output.apply(Count.globally());

    counted
        .apply(
            "Convert to String",
            org.apache.beam.sdk.transforms.MapElements.into(
                    org.apache.beam.sdk.values.TypeDescriptors.strings())
                .via(String::valueOf))
        .apply(
            "Write to File",
            org.apache.beam.sdk.io.TextIO.write()
                .to("/Users/chamikara/testing/delta_lake/test_repo_pipeline_output/output")
                .withSuffix(".txt")
                .withoutSharding());

    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testConvertToBeamSchema() {
    StructType deltaSchema =
        new StructType(
            java.util.Arrays.asList(
                new StructField("string", StringType.STRING, false),
                new StructField("integer", IntegerType.INTEGER, false),
                new StructField("long", LongType.LONG, false),
                new StructField("float", FloatType.FLOAT, false),
                new StructField("double", DoubleType.DOUBLE, false),
                new StructField("boolean", BooleanType.BOOLEAN, false),
                new StructField("binary", BinaryType.BINARY, false),
                new StructField("timestamp", TimestampType.TIMESTAMP, false),
                new StructField("date", DateType.DATE, false),
                new StructField("array", new ArrayType(StringType.STRING, true), false),
                new StructField(
                    "map", new MapType(StringType.STRING, IntegerType.INTEGER, true), false),
                new StructField(
                    "struct",
                    new StructType(
                        java.util.Arrays.asList(
                            new StructField("nested_string", StringType.STRING, false))),
                    false)));

    Schema nestedSchema =
        Schema.builder().addField("nested_string", Schema.FieldType.STRING).build();

    Schema expectedSchema =
        Schema.builder()
            .addField("string", Schema.FieldType.STRING)
            .addField("integer", Schema.FieldType.INT32)
            .addField("long", Schema.FieldType.INT64)
            .addField("float", Schema.FieldType.FLOAT)
            .addField("double", Schema.FieldType.DOUBLE)
            .addField("boolean", Schema.FieldType.BOOLEAN)
            .addField("binary", Schema.FieldType.BYTES)
            .addField("timestamp", Schema.FieldType.DATETIME)
            .addField("date", Schema.FieldType.DATETIME)
            .addField("array", Schema.FieldType.iterable(Schema.FieldType.STRING))
            .addField("map", Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.INT32))
            .addField("struct", Schema.FieldType.row(nestedSchema))
            .build();

    Schema actualSchema = DeltaIO.ReadRows.convertToBeamSchema(deltaSchema);
    org.junit.Assert.assertEquals(expectedSchema, actualSchema);
  }

  @Test
  public void testDeltaReadTaskTracker() {
    java.util.List<Long> sizes = java.util.Arrays.asList(100L, 200L, 300L);
    org.apache.beam.sdk.io.range.OffsetRange range =
        new org.apache.beam.sdk.io.range.OffsetRange(0L, 3L);
    DeltaReadTaskTracker tracker = new DeltaReadTaskTracker(range, sizes);

    org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.Progress progress =
        tracker.getProgress();
    org.junit.Assert.assertEquals(0.0, progress.getWorkCompleted(), 0.001);
    org.junit.Assert.assertEquals(600.0, progress.getWorkRemaining(), 0.001);

    org.junit.Assert.assertTrue(tracker.tryClaim(0L));
    progress = tracker.getProgress();
    org.junit.Assert.assertEquals(100.0, progress.getWorkCompleted(), 0.001);
    org.junit.Assert.assertEquals(500.0, progress.getWorkRemaining(), 0.001);

    org.junit.Assert.assertTrue(tracker.tryClaim(1L));
    progress = tracker.getProgress();
    org.junit.Assert.assertEquals(300.0, progress.getWorkCompleted(), 0.001);
    org.junit.Assert.assertEquals(300.0, progress.getWorkRemaining(), 0.001);

    org.junit.Assert.assertTrue(tracker.tryClaim(2L));
    progress = tracker.getProgress();
    org.junit.Assert.assertEquals(600.0, progress.getWorkCompleted(), 0.001);
    org.junit.Assert.assertEquals(0.0, progress.getWorkRemaining(), 0.001);

    tracker.checkDone();
  }

  @Test
  public void testBeamParquetHandler() {
    java.util.List<Long> sizes = java.util.Arrays.asList(100L, 200L);
    org.apache.beam.sdk.io.range.OffsetRange range =
        new org.apache.beam.sdk.io.range.OffsetRange(0L, 2L);
    DeltaReadTaskTracker tracker = new DeltaReadTaskTracker(range, sizes);

    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    io.delta.kernel.engine.ParquetHandler dummyDelegate =
        new io.delta.kernel.engine.ParquetHandler() {
          @Override
          public io.delta.kernel.utils.CloseableIterator<io.delta.kernel.engine.FileReadResult>
              readParquetFiles(
                  io.delta.kernel.utils.CloseableIterator<io.delta.kernel.utils.FileStatus>
                      fileIter,
                  io.delta.kernel.types.StructType physicalSchema,
                  java.util.Optional<io.delta.kernel.expressions.Predicate> predicate)
                  throws java.io.IOException {
            return new io.delta.kernel.utils.CloseableIterator<
                io.delta.kernel.engine.FileReadResult>() {
              @Override
              public boolean hasNext() {
                return false;
              }

              @Override
              public io.delta.kernel.engine.FileReadResult next() {
                throw new java.util.NoSuchElementException();
              }

              @Override
              public void close() {}
            };
          }

          @Override
          public void writeParquetFileAtomically(
              String filePath,
              io.delta.kernel.utils.CloseableIterator<io.delta.kernel.data.FilteredColumnarBatch>
                  data)
              throws java.io.IOException {}

          @Override
          public io.delta.kernel.utils.CloseableIterator<io.delta.kernel.utils.DataFileStatus>
              writeParquetFiles(
                  String filePath,
                  io.delta.kernel.utils.CloseableIterator<
                          io.delta.kernel.data.FilteredColumnarBatch>
                      data,
                  java.util.List<io.delta.kernel.expressions.Column> statsColumns)
                  throws java.io.IOException {
            return new io.delta.kernel.utils.CloseableIterator<
                io.delta.kernel.utils.DataFileStatus>() {
              @Override
              public boolean hasNext() {
                return false;
              }

              @Override
              public io.delta.kernel.utils.DataFileStatus next() {
                throw new java.util.NoSuchElementException();
              }

              @Override
              public void close() {}
            };
          }
        };

    BeamParquetHandler handler = new BeamParquetHandler(conf, dummyDelegate, tracker);
    org.junit.Assert.assertNotNull(handler);

    BeamEngine beamEngine =
        new BeamEngine(io.delta.kernel.defaults.engine.DefaultEngine.create(conf), handler);
    org.junit.Assert.assertEquals(handler, beamEngine.getParquetHandler());
  }

  @Test
  public void testBeamParquetHandlerWriteDelegation() throws Exception {
    java.util.List<Long> sizes = java.util.Arrays.asList(100L);
    org.apache.beam.sdk.io.range.OffsetRange range =
        new org.apache.beam.sdk.io.range.OffsetRange(0L, 1L);
    DeltaReadTaskTracker tracker = new DeltaReadTaskTracker(range, sizes);
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

    boolean[] flags = new boolean[2];
    io.delta.kernel.engine.ParquetHandler delegate =
        new io.delta.kernel.engine.ParquetHandler() {
          @Override
          public io.delta.kernel.utils.CloseableIterator<io.delta.kernel.engine.FileReadResult>
              readParquetFiles(
                  io.delta.kernel.utils.CloseableIterator<io.delta.kernel.utils.FileStatus>
                      fileIter,
                  io.delta.kernel.types.StructType physicalSchema,
                  java.util.Optional<io.delta.kernel.expressions.Predicate> predicate) {
            return null;
          }

          @Override
          public void writeParquetFileAtomically(
              String filePath,
              io.delta.kernel.utils.CloseableIterator<io.delta.kernel.data.FilteredColumnarBatch>
                  data) {
            flags[0] = true;
          }

          @Override
          public io.delta.kernel.utils.CloseableIterator<io.delta.kernel.utils.DataFileStatus>
              writeParquetFiles(
                  String filePath,
                  io.delta.kernel.utils.CloseableIterator<
                          io.delta.kernel.data.FilteredColumnarBatch>
                      data,
                  java.util.List<io.delta.kernel.expressions.Column> statsColumns) {
            flags[1] = true;
            return new io.delta.kernel.utils.CloseableIterator<
                io.delta.kernel.utils.DataFileStatus>() {
              @Override
              public boolean hasNext() {
                return false;
              }

              @Override
              public io.delta.kernel.utils.DataFileStatus next() {
                throw new java.util.NoSuchElementException();
              }

              @Override
              public void close() {}
            };
          }
        };

    BeamParquetHandler handler = new BeamParquetHandler(conf, delegate, tracker);
    handler.writeParquetFileAtomically("path", null);
    org.junit.Assert.assertTrue(flags[0]);

    handler.writeParquetFiles("path", null, java.util.Collections.emptyList());
    org.junit.Assert.assertTrue(flags[1]);
  }

  @Test
  public void testBeamParquetHandlerReadFiltering() throws Exception {
    File tableDir = tempFolder.newFolder("parquet-filtering-test");

    Schema schema = Schema.builder().addField("name", Schema.FieldType.STRING).build();
    Row row = Row.withSchema(schema).addValues("test-name").build();
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
    GenericRecord record = AvroUtils.toGenericRecord(row, avroSchema);

    filteringPipeline
        .apply("Create Input", Create.of(record).withCoder(AvroCoder.of(avroSchema)))
        .apply(
            "Write Parquet",
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(avroSchema))
                .to(tableDir.getAbsolutePath() + "/")
                .withNaming((w, p, n, s, c) -> "part-00000.parquet"));

    filteringPipeline.run().waitUntilFinish();

    File parquetFile = new File(tableDir, "part-00000.parquet");
    io.delta.kernel.utils.FileStatus fileStatus =
        io.delta.kernel.utils.FileStatus.of(
            parquetFile.getAbsolutePath(), parquetFile.length(), 123456789L);

    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    io.delta.kernel.types.StructType physicalSchema =
        new io.delta.kernel.types.StructType(
            java.util.Arrays.asList(
                new io.delta.kernel.types.StructField(
                    "name", io.delta.kernel.types.StringType.STRING, true)));

    io.delta.kernel.engine.ParquetHandler dummyDelegate =
        new io.delta.kernel.engine.ParquetHandler() {
          @Override
          public io.delta.kernel.utils.CloseableIterator<io.delta.kernel.engine.FileReadResult>
              readParquetFiles(
                  io.delta.kernel.utils.CloseableIterator<io.delta.kernel.utils.FileStatus>
                      fileIter,
                  io.delta.kernel.types.StructType physicalSchema,
                  java.util.Optional<io.delta.kernel.expressions.Predicate> predicate) {
            return null;
          }

          @Override
          public void writeParquetFileAtomically(
              String filePath,
              io.delta.kernel.utils.CloseableIterator<io.delta.kernel.data.FilteredColumnarBatch>
                  data) {}

          @Override
          public io.delta.kernel.utils.CloseableIterator<io.delta.kernel.utils.DataFileStatus>
              writeParquetFiles(
                  String filePath,
                  io.delta.kernel.utils.CloseableIterator<
                          io.delta.kernel.data.FilteredColumnarBatch>
                      data,
                  java.util.List<io.delta.kernel.expressions.Column> statsColumns) {
            return null;
          }
        };

    // Case A: Out of bounds before (tracker range [10, 20))
    DeltaReadTaskTracker trackerA =
        new DeltaReadTaskTracker(
            new org.apache.beam.sdk.io.range.OffsetRange(10L, 20L),
            java.util.Collections.singletonList(parquetFile.length()));
    BeamParquetHandler handlerA = new BeamParquetHandler(conf, dummyDelegate, trackerA);
    try (io.delta.kernel.utils.CloseableIterator<io.delta.kernel.engine.FileReadResult> iter =
        handlerA.readParquetFiles(
            io.delta.kernel.internal.util.Utils.singletonCloseableIterator(fileStatus),
            physicalSchema,
            java.util.Optional.empty())) {
      org.junit.Assert.assertFalse(iter.hasNext());
      try {
        iter.next();
        org.junit.Assert.fail("Expected NoSuchElementException");
      } catch (java.util.NoSuchElementException e) {
        // expected
      }
    }

    // Case B: Out of bounds after (tracker range [0, 0))
    DeltaReadTaskTracker trackerB =
        new DeltaReadTaskTracker(
            new org.apache.beam.sdk.io.range.OffsetRange(0L, 0L),
            java.util.Collections.singletonList(parquetFile.length()));
    BeamParquetHandler handlerB = new BeamParquetHandler(conf, dummyDelegate, trackerB);
    try (io.delta.kernel.utils.CloseableIterator<io.delta.kernel.engine.FileReadResult> iter =
        handlerB.readParquetFiles(
            io.delta.kernel.internal.util.Utils.singletonCloseableIterator(fileStatus),
            physicalSchema,
            java.util.Optional.empty())) {
      org.junit.Assert.assertFalse(iter.hasNext());
    }

    // Case C: Claim fails
    DeltaReadTaskTracker trackerC =
        new DeltaReadTaskTracker(
            new org.apache.beam.sdk.io.range.OffsetRange(0L, 1L),
            java.util.Collections.singletonList(parquetFile.length())) {
          @Override
          public boolean tryClaim(Long i) {
            return false; // Simulate failure to claim
          }
        };
    BeamParquetHandler handlerC = new BeamParquetHandler(conf, dummyDelegate, trackerC);
    try (io.delta.kernel.utils.CloseableIterator<io.delta.kernel.engine.FileReadResult> iter =
        handlerC.readParquetFiles(
            io.delta.kernel.internal.util.Utils.singletonCloseableIterator(fileStatus),
            physicalSchema,
            java.util.Optional.empty())) {
      org.junit.Assert.assertFalse(iter.hasNext());
    }

    // Case D: Successful claim and read
    DeltaReadTaskTracker trackerD =
        new DeltaReadTaskTracker(
            new org.apache.beam.sdk.io.range.OffsetRange(0L, 1L),
            java.util.Collections.singletonList(parquetFile.length()));
    BeamParquetHandler handlerD = new BeamParquetHandler(conf, dummyDelegate, trackerD);
    try (io.delta.kernel.utils.CloseableIterator<io.delta.kernel.engine.FileReadResult> iter =
        handlerD.readParquetFiles(
            io.delta.kernel.internal.util.Utils.singletonCloseableIterator(fileStatus),
            physicalSchema,
            java.util.Optional.empty())) {
      org.junit.Assert.assertTrue(iter.hasNext());
      io.delta.kernel.engine.FileReadResult res = iter.next();
      org.junit.Assert.assertNotNull(res);
      org.junit.Assert.assertNotNull(res.getData());
      org.junit.Assert.assertFalse(iter.hasNext());
      try {
        iter.next();
        org.junit.Assert.fail("Expected NoSuchElementException");
      } catch (java.util.NoSuchElementException e) {
        // expected
      }
    }
  }

  @Test
  public void testReadChanges() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-changes");
    Engine engine = DefaultEngine.create(new org.apache.hadoop.conf.Configuration());

    // 1. Write parquet files for Version 0 (insert-only commit)
    Schema tableSchema = Schema.builder().addField("name", Schema.FieldType.STRING).build();
    Row tableRow1 = Row.withSchema(tableSchema).addValues("row-1").build();
    Row tableRow2 = Row.withSchema(tableSchema).addValues("row-2").build();
    StructType deltaSchema = new StructType().add("name", StringType.STRING);

    writeAppendCommit(
        engine,
        tableDir.getAbsolutePath(),
        0L,
        100000000000L,
        deltaSchema,
        java.util.Arrays.asList(tableRow1, tableRow2));

    // 2. Write cdc parquet file for Version 1 (commit with cdc actions)
    Schema cdcWriteSchema =
        Schema.builder()
            .addField("name", Schema.FieldType.STRING)
            .addField(DeltaIO.CHANGE_TYPE_COLUMN, Schema.FieldType.STRING)
            .addField(DeltaIO.COMMIT_VERSION_COLUMN, Schema.FieldType.INT64)
            .addField(DeltaIO.COMMIT_TIMESTAMP_COLUMN, Schema.FieldType.DATETIME)
            .build();
    StructType cdcWriteDeltaSchema =
        new StructType()
            .add("name", StringType.STRING)
            .add(DeltaIO.CHANGE_TYPE_COLUMN, StringType.STRING)
            .add(DeltaIO.COMMIT_VERSION_COLUMN, LongType.LONG)
            .add(DeltaIO.COMMIT_TIMESTAMP_COLUMN, TimestampType.TIMESTAMP);

    Row cdcRow1 =
        Row.withSchema(cdcWriteSchema)
            .addValues("row-1", "update_preimage", 1L, new Instant(123456789000L))
            .build();
    Row cdcRow2 =
        Row.withSchema(cdcWriteSchema)
            .addValues("row-1-updated", "update_postimage", 1L, new Instant(123456789000L))
            .build();
    Row cdcRow3 =
        Row.withSchema(cdcWriteSchema)
            .addValues("row-2", "delete", 1L, new Instant(123456789000L))
            .build();

    writeCdcCommit(
        engine,
        tableDir.getAbsolutePath(),
        1L,
        200000000000L,
        deltaSchema,
        null,
        null,
        java.util.Arrays.asList(cdcRow1, cdcRow2, cdcRow3),
        cdcWriteDeltaSchema);

    // 3. Read CDF data from table using ReadChanges
    PCollection<Row> output =
        readPipeline.apply(
            DeltaIO.readChanges().from(tableDir.getAbsolutePath()).withStartVersion(0L));

    PCollection<String> formattedOutput =
        output.apply("Format ValueKind and Row", ParDo.of(new FormatValueKindAndRow()));

    PAssert.that(formattedOutput)
        .containsInAnyOrder(
            "INSERT:row-1",
            "INSERT:row-2",
            "UPDATE_BEFORE:row-1",
            "UPDATE_AFTER:row-1-updated",
            "DELETE:row-2");

    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadChangesRanges() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-changes-ranges");
    Engine engine = DefaultEngine.create(new org.apache.hadoop.conf.Configuration());

    Schema tableSchema = Schema.builder().addField("name", Schema.FieldType.STRING).build();
    StructType deltaSchema = new StructType().add("name", StringType.STRING);

    // 1. Write parquet files for Version 0 (insert-only commit)
    Row tableRow1 = Row.withSchema(tableSchema).addValues("row-1").build();
    Row tableRow2 = Row.withSchema(tableSchema).addValues("row-2").build();
    writeAppendCommit(
        engine,
        tableDir.getAbsolutePath(),
        0L,
        100000000000L,
        deltaSchema,
        java.util.Arrays.asList(tableRow1, tableRow2));

    // 2. Write parquet files for Version 1 (commit with updates and deletes)
    Schema cdcWriteSchema =
        Schema.builder()
            .addField("name", Schema.FieldType.STRING)
            .addField(DeltaIO.CHANGE_TYPE_COLUMN, Schema.FieldType.STRING)
            .addField(DeltaIO.COMMIT_VERSION_COLUMN, Schema.FieldType.INT64)
            .addField(DeltaIO.COMMIT_TIMESTAMP_COLUMN, Schema.FieldType.DATETIME)
            .build();
    StructType cdcWriteDeltaSchema =
        new StructType()
            .add("name", StringType.STRING)
            .add(DeltaIO.CHANGE_TYPE_COLUMN, StringType.STRING)
            .add(DeltaIO.COMMIT_VERSION_COLUMN, LongType.LONG)
            .add(DeltaIO.COMMIT_TIMESTAMP_COLUMN, TimestampType.TIMESTAMP);

    Row cdcRow1 =
        Row.withSchema(cdcWriteSchema)
            .addValues("row-1", "update_preimage", 1L, new Instant(200000000000L))
            .build();
    Row cdcRow2 =
        Row.withSchema(cdcWriteSchema)
            .addValues("row-1-updated", "update_postimage", 1L, new Instant(200000000000L))
            .build();
    Row cdcRow3 =
        Row.withSchema(cdcWriteSchema)
            .addValues("row-2", "delete", 1L, new Instant(200000000000L))
            .build();

    writeCdcCommit(
        engine,
        tableDir.getAbsolutePath(),
        1L,
        200000000000L,
        deltaSchema,
        null,
        null,
        java.util.Arrays.asList(cdcRow1, cdcRow2, cdcRow3),
        cdcWriteDeltaSchema);

    // 3. Write parquet files for Version 2 (insert-only commit)
    Row tableRow3 = Row.withSchema(tableSchema).addValues("row-3").build();
    writeAppendCommit(
        engine,
        tableDir.getAbsolutePath(),
        2L,
        300000000000L,
        deltaSchema,
        java.util.Arrays.asList(tableRow3));

    // Test 1: Read changes between start version 0 and end version 2
    PCollection<Row> outputVersions =
        readPipeline.apply(
            "Read Changes Version Range",
            DeltaIO.readChanges()
                .from(tableDir.getAbsolutePath())
                .withStartVersion(0L)
                .withEndVersion(2L));

    PCollection<String> formattedVersions =
        outputVersions.apply("Format Version Output", ParDo.of(new FormatValueKindAndRow()));

    PAssert.that(formattedVersions)
        .containsInAnyOrder(
            "INSERT:row-1",
            "INSERT:row-2",
            "UPDATE_BEFORE:row-1",
            "UPDATE_AFTER:row-1-updated",
            "DELETE:row-2",
            "INSERT:row-3");

    // Test 2: Read changes between start timestamp (after version 0) and end timestamp (after
    // version 2)
    String startTimestamp = java.time.Instant.ofEpochMilli(150000000000L).toString();
    String endTimestamp = java.time.Instant.ofEpochMilli(350000000000L).toString();

    PCollection<Row> outputTimestamps =
        filteringPipeline.apply(
            "Read Changes Timestamp Range",
            DeltaIO.readChanges()
                .from(tableDir.getAbsolutePath())
                .withStartTimestamp(startTimestamp)
                .withEndTimestamp(endTimestamp));

    PCollection<String> formattedTimestamps =
        outputTimestamps.apply("Format Timestamp Output", ParDo.of(new FormatValueKindAndRow()));

    PAssert.that(formattedTimestamps)
        .containsInAnyOrder(
            "UPDATE_BEFORE:row-1", "UPDATE_AFTER:row-1-updated", "DELETE:row-2", "INSERT:row-3");

    readPipeline.run().waitUntilFinish();
    filteringPipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadChangesPartialRange() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-changes-partial-range");
    Engine engine = DefaultEngine.create(new org.apache.hadoop.conf.Configuration());

    Schema tableSchema = Schema.builder().addField("name", Schema.FieldType.STRING).build();
    StructType deltaSchema = new StructType().add("name", StringType.STRING);

    // 1. Write parquet files for Version 0 (insert-only commit)
    Row tableRow1 = Row.withSchema(tableSchema).addValues("row-1").build();
    Row tableRow2 = Row.withSchema(tableSchema).addValues("row-2").build();
    writeAppendCommit(
        engine,
        tableDir.getAbsolutePath(),
        0L,
        100000000000L,
        deltaSchema,
        java.util.Arrays.asList(tableRow1, tableRow2));

    // 2. Write parquet files for Version 1 (commit with updates and deletes)
    Schema cdcWriteSchema =
        Schema.builder()
            .addField("name", Schema.FieldType.STRING)
            .addField(DeltaIO.CHANGE_TYPE_COLUMN, Schema.FieldType.STRING)
            .addField(DeltaIO.COMMIT_VERSION_COLUMN, Schema.FieldType.INT64)
            .addField(DeltaIO.COMMIT_TIMESTAMP_COLUMN, Schema.FieldType.DATETIME)
            .build();
    StructType cdcWriteDeltaSchema =
        new StructType()
            .add("name", StringType.STRING)
            .add(DeltaIO.CHANGE_TYPE_COLUMN, StringType.STRING)
            .add(DeltaIO.COMMIT_VERSION_COLUMN, LongType.LONG)
            .add(DeltaIO.COMMIT_TIMESTAMP_COLUMN, TimestampType.TIMESTAMP);

    Row cdcRow1 =
        Row.withSchema(cdcWriteSchema)
            .addValues("row-1", "update_preimage", 1L, new Instant(200000000000L))
            .build();
    Row cdcRow2 =
        Row.withSchema(cdcWriteSchema)
            .addValues("row-1-updated", "update_postimage", 1L, new Instant(200000000000L))
            .build();
    Row cdcRow3 =
        Row.withSchema(cdcWriteSchema)
            .addValues("row-2", "delete", 1L, new Instant(200000000000L))
            .build();

    writeCdcCommit(
        engine,
        tableDir.getAbsolutePath(),
        1L,
        200000000000L,
        deltaSchema,
        null,
        null,
        java.util.Arrays.asList(cdcRow1, cdcRow2, cdcRow3),
        cdcWriteDeltaSchema);

    // 3. Write parquet files for Version 2 (insert-only commit)
    Row tableRow3 = Row.withSchema(tableSchema).addValues("row-3").build();
    writeAppendCommit(
        engine,
        tableDir.getAbsolutePath(),
        2L,
        300000000000L,
        deltaSchema,
        java.util.Arrays.asList(tableRow3));

    // 4. Write parquet files for Version 3 (commit with updates and deletes)
    Row cdcRow4 =
        Row.withSchema(cdcWriteSchema)
            .addValues("row-3", "update_preimage", 3L, new Instant(400000000000L))
            .build();
    Row cdcRow5 =
        Row.withSchema(cdcWriteSchema)
            .addValues("row-3-updated", "update_postimage", 3L, new Instant(400000000000L))
            .build();
    Row cdcRow6 =
        Row.withSchema(cdcWriteSchema)
            .addValues("row-1-updated", "delete", 3L, new Instant(400000000000L))
            .build();

    writeCdcCommit(
        engine,
        tableDir.getAbsolutePath(),
        3L,
        400000000000L,
        deltaSchema,
        null,
        null,
        java.util.Arrays.asList(cdcRow4, cdcRow5, cdcRow6),
        cdcWriteDeltaSchema);

    // 5. Write parquet files for Version 4 (insert-only commit)
    Row tableRow4 = Row.withSchema(tableSchema).addValues("row-4").build();
    writeAppendCommit(
        engine,
        tableDir.getAbsolutePath(),
        4L,
        500000000000L,
        deltaSchema,
        java.util.Arrays.asList(tableRow4));

    // Read changes between start version 1 and end version 3
    PCollection<Row> outputVersions =
        readPipeline.apply(
            "Read Changes Partial Version Range",
            DeltaIO.readChanges()
                .from(tableDir.getAbsolutePath())
                .withStartVersion(1L)
                .withEndVersion(3L));

    PCollection<String> formattedVersions =
        outputVersions.apply("Format Version Output", ParDo.of(new FormatValueKindAndRow()));

    PAssert.that(formattedVersions)
        .containsInAnyOrder(
            "UPDATE_BEFORE:row-1",
            "UPDATE_AFTER:row-1-updated",
            "DELETE:row-2",
            "INSERT:row-3",
            "UPDATE_BEFORE:row-3",
            "UPDATE_AFTER:row-3-updated",
            "DELETE:row-1-updated");

    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testS3SchemeRegistrationWithAnonymousCredentials() {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A");
    conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");

    Engine engine = DefaultEngine.create(conf);
    Table table = Table.forPath(engine, "s3://fake-bucket/table");
    Exception e = Assert.assertThrows(Exception.class, () -> table.getLatestSnapshot(engine));
    String msg = e.toString();
    Assert.assertFalse(
        "Should not throw UnsupportedFileSystemException. Error was: " + msg,
        msg.contains("UnsupportedFileSystemException") || msg.contains("No FileSystem for scheme"));
  }

  private static final class FormatValueKindAndRow extends DoFn<Row, String> {
    @ProcessElement
    public void process(
        @Element Row row, ValueKind valueKind, OutputReceiver<String> outputReceiver) {
      outputReceiver.output(valueKind.name() + ":" + row.getString("name"));
    }
  }

  private List<String> writeAppendCommit(
      Engine engine,
      String tablePath,
      long expectedVersion,
      long timestamp,
      StructType deltaSchema,
      List<Row> beamRows)
      throws Exception {

    Table table = Table.forPath(engine, tablePath);
    TransactionBuilder txnBuilder =
        table.createTransactionBuilder(engine, "DeltaIOTest", Operation.WRITE);
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
    File commitFile =
        new File(new File(tablePath, "_delta_log"), String.format("%020d.json", expectedVersion));
    commitFile.setLastModified(timestamp);
    return writtenFiles;
  }

  private void writeCdcCommit(
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
        table.createTransactionBuilder(engine, "DeltaIOTest", Operation.WRITE);
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

      String cdcDir = new File(tablePath, "_change_data").getAbsolutePath();

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
    File commitFile =
        new File(new File(tablePath, "_delta_log"), String.format("%020d.json", expectedVersion));
    commitFile.setLastModified(timestamp);
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
    Map<String, Object> values = new HashMap<>();
    values.put(actionName, actionRow);
    return new TestRow(customSingleActionSchema, values);
  }

  private static final MapValue EMPTY_MAP_VALUE =
      new MapValue() {
        @Override
        public int getSize() {
          return 0;
        }

        @Override
        public ColumnVector getKeys() {
          return new ColumnVector() {
            @Override
            public DataType getDataType() {
              return StringType.STRING;
            }

            @Override
            public int getSize() {
              return 0;
            }

            @Override
            public void close() {}

            @Override
            public boolean isNullAt(int rowId) {
              return true;
            }
          };
        }

        @Override
        public ColumnVector getValues() {
          return new ColumnVector() {
            @Override
            public DataType getDataType() {
              return StringType.STRING;
            }

            @Override
            public int getSize() {
              return 0;
            }

            @Override
            public void close() {}

            @Override
            public boolean isNullAt(int rowId) {
              return true;
            }
          };
        }
      };

  private static io.delta.kernel.data.Row createRemoveAction(
      StructType removeSchema, String path, long deletionTimestamp) {
    Map<String, Object> values = new HashMap<>();
    values.put("path", path);
    values.put("deletionTimestamp", deletionTimestamp);
    values.put("dataChange", true);
    values.put("size", 100L);
    return new TestRow(removeSchema, values);
  }

  private static io.delta.kernel.data.Row createCdcAction(
      StructType cdcSchema, String path, long size) {
    Map<String, Object> values = new HashMap<>();
    values.put("path", path);
    values.put("partitionValues", EMPTY_MAP_VALUE);
    values.put("size", size);
    values.put("dataChange", true);
    return new TestRow(cdcSchema, values);
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
          org.joda.time.Instant instant = rows.get(rowId).getDateTime(fieldIndex).toInstant();
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

  private static class TestRow implements io.delta.kernel.data.Row {
    private final StructType schema;
    private final Map<String, Object> values;

    public TestRow(StructType schema, Map<String, Object> values) {
      this.schema = schema;
      this.values = values;
    }

    @Override
    public StructType getSchema() {
      return schema;
    }

    private Object getVal(int ord) {
      String name = schema.fields().get(ord).getName();
      return values.get(name);
    }

    @Override
    public boolean isNullAt(int ord) {
      return getVal(ord) == null;
    }

    @Override
    public boolean getBoolean(int ord) {
      return (Boolean) getVal(ord);
    }

    @Override
    public byte getByte(int ord) {
      return (Byte) getVal(ord);
    }

    @Override
    public short getShort(int ord) {
      return (Short) getVal(ord);
    }

    @Override
    public int getInt(int ord) {
      return (Integer) getVal(ord);
    }

    @Override
    public long getLong(int ord) {
      return (Long) getVal(ord);
    }

    @Override
    public float getFloat(int ord) {
      return (Float) getVal(ord);
    }

    @Override
    public double getDouble(int ord) {
      return (Double) getVal(ord);
    }

    @Override
    public String getString(int ord) {
      return (String) getVal(ord);
    }

    @Override
    public byte[] getBinary(int ord) {
      return (byte[]) getVal(ord);
    }

    @Override
    public BigDecimal getDecimal(int ord) {
      return (BigDecimal) getVal(ord);
    }

    @Override
    public io.delta.kernel.data.Row getStruct(int ord) {
      return (io.delta.kernel.data.Row) getVal(ord);
    }

    @Override
    public io.delta.kernel.data.ArrayValue getArray(int ord) {
      return (io.delta.kernel.data.ArrayValue) getVal(ord);
    }

    @Override
    public io.delta.kernel.data.MapValue getMap(int ord) {
      return (io.delta.kernel.data.MapValue) getVal(ord);
    }
  }
}
