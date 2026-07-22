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

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
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
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
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

    io.delta.kernel.data.Row scanState = scan.getScanState(engine);
    System.err.println("SCAN STATE SCHEMA: " + scanState.getSchema().toString());

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

    System.out.println("FILES IN TABLE DIR:");
    for (File f : tableDir.listFiles()) {
      System.out.println(
          " - " + f.getName() + " (size=" + f.length() + ", isDir=" + f.isDirectory() + ")");
      if (f.isDirectory()) {
        for (File sub : f.listFiles()) {
          System.out.println("   - " + sub.getName() + " (size=" + sub.length() + ")");
        }
      }
    }

    File parquetFile = new File(tableDir, "part-00000.parquet");
    byte[] fileBytes = Files.readAllBytes(parquetFile.toPath());
    System.out.println("PARQUET FILE LENGTH: " + fileBytes.length);
    if (fileBytes.length >= 8) {
      System.out.println(
          "PARQUET FIRST 4 BYTES: "
              + fileBytes[0]
              + ", "
              + fileBytes[1]
              + ", "
              + fileBytes[2]
              + ", "
              + fileBytes[3]
              + " ('"
              + (char) fileBytes[0]
              + (char) fileBytes[1]
              + (char) fileBytes[2]
              + (char) fileBytes[3]
              + "')");
      int len = fileBytes.length;
      System.out.println(
          "PARQUET LAST 4 BYTES: "
              + fileBytes[len - 4]
              + ", "
              + fileBytes[len - 3]
              + ", "
              + fileBytes[len - 2]
              + ", "
              + fileBytes[len - 1]
              + " ('"
              + (char) fileBytes[len - 4]
              + (char) fileBytes[len - 3]
              + (char) fileBytes[len - 2]
              + (char) fileBytes[len - 1]
              + "')");
    }

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

    // 1. Write a Parquet file to simulate a Delta table
    Schema schema = Schema.builder().addField("name", Schema.FieldType.STRING).build();
    Row row = Row.withSchema(schema).addValues("test-name").build();
    writeParquetFile(new File(tableDir, "part-00000.parquet"), row);

    // 2. Create the Delta log
    File logDir = new File(tableDir, "_delta_log");
    logDir.mkdirs();
    File commitFile = new File(logDir, "00000000000000000000.json");

    File parquetFile = new File(tableDir, "part-00000.parquet");
    byte[] fileBytes = Files.readAllBytes(parquetFile.toPath());

    String commitContent =
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
            + "{\"metaData\":{\"id\":\"test-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[],\"configuration\":{},\"createdAt\":123456789}}\n"
            + "{\"add\":{\"path\":\"part-00000.parquet\",\"partitionValues\":{},\"size\":"
            + fileBytes.length
            + ",\"modificationTime\":123456789,\"dataChange\":true}}";

    Files.write(commitFile.toPath(), commitContent.getBytes(StandardCharsets.UTF_8));

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
    File logDir = new File(tableDir, "_delta_log");
    logDir.mkdirs();

    // 1. Write parquet files for Version 0 (insert-only commit)
    Schema tableSchema = Schema.builder().addField("name", Schema.FieldType.STRING).build();
    Row tableRow1 = Row.withSchema(tableSchema).addValues("row-1").build();
    Row tableRow2 = Row.withSchema(tableSchema).addValues("row-2").build();

    File partFile = new File(tableDir, "part-00000.parquet");
    byte[] partBytes =
        writeParquetFile(partFile, tableSchema, java.util.Arrays.asList(tableRow1, tableRow2));

    writeCommit(
        logDir, 0L, 100000000000L, "part-00000.parquet", partBytes.length, null, null, 0L, true);

    // 2. Write cdc parquet file for Version 1 (commit with cdc actions)
    Schema cdcWriteSchema =
        Schema.builder()
            .addField("name", Schema.FieldType.STRING)
            .addField("_change_type", Schema.FieldType.STRING)
            .addField("_commit_version", Schema.FieldType.INT64)
            .addField("_commit_timestamp", Schema.FieldType.DATETIME)
            .build();

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

    File changeFile = new File(tableDir, "change-00000.parquet");
    byte[] changeBytes =
        writeParquetFile(
            changeFile, cdcWriteSchema, java.util.Arrays.asList(cdcRow1, cdcRow2, cdcRow3));

    writeCommit(
        logDir,
        1L,
        200000000000L,
        null,
        0L,
        null,
        "change-00000.parquet",
        changeBytes.length,
        false);

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
    File logDir = new File(tableDir, "_delta_log");
    logDir.mkdirs();

    Schema tableSchema = Schema.builder().addField("name", Schema.FieldType.STRING).build();

    // 1. Write parquet files for Version 0 (insert-only commit)
    Row tableRow1 = Row.withSchema(tableSchema).addValues("row-1").build();
    Row tableRow2 = Row.withSchema(tableSchema).addValues("row-2").build();
    File partFile0 = new File(tableDir, "part-00000.parquet");
    byte[] partBytes0 =
        writeParquetFile(partFile0, tableSchema, java.util.Arrays.asList(tableRow1, tableRow2));
    writeCommit(
        logDir, 0L, 100000000000L, "part-00000.parquet", partBytes0.length, null, null, 0L, true);

    // 2. Write parquet files for Version 1 (commit with updates and deletes)
    Schema cdcWriteSchema =
        Schema.builder()
            .addField("name", Schema.FieldType.STRING)
            .addField("_change_type", Schema.FieldType.STRING)
            .addField("_commit_version", Schema.FieldType.INT64)
            .addField("_commit_timestamp", Schema.FieldType.DATETIME)
            .build();
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
    File changeFile0 = new File(tableDir, "change-00000.parquet");
    byte[] changeBytes0 =
        writeParquetFile(
            changeFile0, cdcWriteSchema, java.util.Arrays.asList(cdcRow1, cdcRow2, cdcRow3));

    Row tableRow1Updated = Row.withSchema(tableSchema).addValues("row-1-updated").build();
    File partFile1 = new File(tableDir, "part-00001.parquet");
    byte[] partBytes1 =
        writeParquetFile(partFile1, tableSchema, java.util.Arrays.asList(tableRow1Updated));

    writeCommit(
        logDir,
        1L,
        200000000000L,
        "part-00001.parquet",
        partBytes1.length,
        "part-00000.parquet",
        "change-00000.parquet",
        changeBytes0.length,
        false);

    // 3. Write parquet files for Version 2 (insert-only commit)
    Row tableRow3 = Row.withSchema(tableSchema).addValues("row-3").build();
    File partFile2 = new File(tableDir, "part-00002.parquet");
    byte[] partBytes2 =
        writeParquetFile(partFile2, tableSchema, java.util.Arrays.asList(tableRow3));
    writeCommit(
        logDir, 2L, 300000000000L, "part-00002.parquet", partBytes2.length, null, null, 0L, false);

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

  private void writeCommit(
      File logDir,
      long version,
      long timestamp,
      @Nullable String addPath,
      long addSize,
      @Nullable String removePath,
      @Nullable String cdcPath,
      long cdcSize,
      boolean writeMetadata)
      throws IOException {
    File commitFile = new File(logDir, String.format("%020d.json", version));
    StringBuilder content = new StringBuilder();
    if (version == 0 || writeMetadata) {
      content.append("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n");
      content.append(
          "{\"metaData\":{\"id\":\"test-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[],\"configuration\":{\"delta.enableChangeDataFeed\":\"true\"},\"createdAt\":123456789}}\n");
    }
    if (addPath != null) {
      content.append(
          String.format(
              "{\"add\":{\"path\":\"%s\",\"partitionValues\":{},\"size\":%d,\"modificationTime\":%d,\"dataChange\":true}}\n",
              addPath, addSize, timestamp));
    }
    if (removePath != null) {
      content.append(
          String.format(
              "{\"remove\":{\"path\":\"%s\",\"deletionTimestamp\":%d,\"dataChange\":true}}\n",
              removePath, timestamp));
    }
    if (cdcPath != null) {
      content.append(
          String.format(
              "{\"cdc\":{\"path\":\"%s\",\"partitionValues\":{},\"size\":%d,\"dataChange\":true}}\n",
              cdcPath, cdcSize));
    }
    Files.write(commitFile.toPath(), content.toString().getBytes(StandardCharsets.UTF_8));
    commitFile.setLastModified(timestamp);
  }

  private static final class FormatValueKindAndRow extends DoFn<Row, String> {
    @ProcessElement
    public void process(
        @Element Row row, ValueKind valueKind, OutputReceiver<String> outputReceiver) {
      outputReceiver.output(valueKind.name() + ":" + row.getString("name"));
    }
  }

  private byte[] writeParquetFile(File file, Schema schema, java.util.List<Row> rows)
      throws Exception {
    org.apache.avro.Schema avroSchema =
        org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils.toAvroSchema(schema);
    org.apache.avro.generic.GenericRecord[] records =
        new org.apache.avro.generic.GenericRecord[rows.size()];
    for (int i = 0; i < rows.size(); i++) {
      records[i] =
          org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils.toGenericRecord(
              rows.get(i), avroSchema);
    }
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
    try (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericRecord> writer =
        org.apache.parquet.avro.AvroParquetWriter.<org.apache.avro.generic.GenericRecord>builder(
                path)
            .withSchema(avroSchema)
            .withConf(new org.apache.hadoop.conf.Configuration())
            .build()) {
      for (org.apache.avro.generic.GenericRecord record : records) {
        writer.write(record);
      }
    }
    return java.nio.file.Files.readAllBytes(file.toPath());
  }
}
