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

import static org.apache.beam.sdk.io.delta.DeltaCdcReadSchemaTransformProvider.Configuration;
import static org.apache.beam.sdk.io.delta.DeltaCdcReadSchemaTransformProvider.OUTPUT_TAG;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

/** Tests for {@link DeltaCdcReadSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class DeltaCdcReadSchemaTransformProviderTest {

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testBuildTransformWithRow() {
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("fs.gs.project.id", "test-project");

    Row config =
        Row.withSchema(new DeltaCdcReadSchemaTransformProvider().configurationSchema())
            .withFieldValue("table", "/path/to/table")
            .withFieldValue("start_version", 0L)
            .withFieldValue("end_version", 5L)
            .withFieldValue("hadoop_config", hadoopConfig)
            .withFieldValue("include_metadata_columns", Arrays.asList(DeltaIO.CHANGE_TYPE_COLUMN))
            .build();

    new DeltaCdcReadSchemaTransformProvider().from(config);
  }

  @Test
  public void testSimpleScan() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-cdc-simple");

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

    // 2. Create the Delta log with CDF enabled
    File logDir = new File(tableDir, "_delta_log");
    logDir.mkdirs();
    File commitFile = new File(logDir, "00000000000000000000.json");

    String commitContent =
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
            + "{\"metaData\":{\"id\":\"test-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[],\"configuration\":{\"delta.enableChangeDataFeed\":\"true\"},\"createdAt\":123456789}}\n"
            + "{\"add\":{\"path\":\"part-00000.parquet\",\"partitionValues\":{},\"size\":"
            + fileBytes.length
            + ",\"modificationTime\":123456789,\"dataChange\":true}}";

    Files.write(commitFile.toPath(), commitContent.getBytes(StandardCharsets.UTF_8));

    // 3. Read it using DeltaCdcReadSchemaTransformProvider
    Configuration readConfig =
        Configuration.builder().setTable(tableDir.getAbsolutePath()).setStartVersion(0L).build();

    PCollection<Row> output =
        PCollectionRowTuple.empty(readPipeline)
            .apply(new DeltaCdcReadSchemaTransformProvider().from(readConfig))
            .get(OUTPUT_TAG);

    PAssert.that(output).containsInAnyOrder(row);

    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadWithStartVersion() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-cdc-version");
    Engine engine = DefaultEngine.create(new org.apache.hadoop.conf.Configuration());

    List<Row> rows = DeltaWriteTestUtils.setupTwoVersionTable(engine, tableDir.getAbsolutePath());
    Row row1 = rows.get(0);
    Row row2 = rows.get(1);

    Configuration readConfig =
        Configuration.builder()
            .setTable(tableDir.getAbsolutePath())
            .setStartVersion(0L)
            .setEndVersion(0L)
            .build();

    PCollection<Row> output =
        PCollectionRowTuple.empty(readPipeline)
            .apply(new DeltaCdcReadSchemaTransformProvider().from(readConfig))
            .get(OUTPUT_TAG);

    PAssert.that(output).containsInAnyOrder(row1, row2);

    readPipeline.run().waitUntilFinish();
  }
}
