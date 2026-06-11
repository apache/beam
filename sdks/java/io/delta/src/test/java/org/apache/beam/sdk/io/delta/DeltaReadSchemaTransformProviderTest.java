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

import static org.apache.beam.sdk.io.delta.DeltaReadSchemaTransformProvider.Configuration;
import static org.apache.beam.sdk.io.delta.DeltaReadSchemaTransformProvider.OUTPUT_TAG;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DeltaReadSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class DeltaReadSchemaTransformProviderTest {

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testBuildTransformWithRow() {
    java.util.Map<String, String> hadoopConfig = new java.util.HashMap<>();
    hadoopConfig.put("fs.gs.project.id", "test-project");

    Row config =
        Row.withSchema(new DeltaReadSchemaTransformProvider().configurationSchema())
            .withFieldValue("table", "/path/to/table")
            .withFieldValue("version", 5L)
            .withFieldValue("timestamp", "2026-06-04T12:00:00Z")
            .withFieldValue("hadoop_config", hadoopConfig)
            .build();

    new DeltaReadSchemaTransformProvider().from(config);
  }

  @Test
  public void testSimpleScan() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-simple");

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

    // 3. Read it using DeltaReadSchemaTransformProvider
    Configuration readConfig = Configuration.builder().setTable(tableDir.getAbsolutePath()).build();

    PCollection<Row> output =
        PCollectionRowTuple.empty(readPipeline)
            .apply(new DeltaReadSchemaTransformProvider().from(readConfig))
            .get(OUTPUT_TAG);

    PAssert.that(output).containsInAnyOrder(row);

    readPipeline.run().waitUntilFinish();
  }
}
