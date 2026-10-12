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

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Local integration tests for {@link DeltaIO} using the Prism runner. */
@RunWith(JUnit4.class)
public class DeltaIOPrismIT {

  private static final Schema ROW_SCHEMA =
      Schema.builder().addInt32Field("id").addStringField("name").build();

  private static final List<Row> TEST_ROWS =
      Arrays.asList(
          Row.withSchema(ROW_SCHEMA).addValues(1, "one").build(),
          Row.withSchema(ROW_SCHEMA).addValues(2, "two").build(),
          Row.withSchema(ROW_SCHEMA).addValues(3, "three").build());

  @Rule public final TestPipeline readPipeline = TestPipeline.create();
  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testReadDeltaFourTable() throws Exception {
    File tableDir = tempFolder.newFolder("delta-4-table");
    Engine engine = DefaultEngine.create(new org.apache.hadoop.conf.Configuration());
    StructType deltaSchema =
        new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);

    DeltaWriteTestUtils.writeAppendCommit(
        engine, tableDir.getAbsolutePath(), 0L, 123456789L, deltaSchema, TEST_ROWS);

    assertManagedRead(tableDir);
  }

  @Test
  public void testReadDeltaThreeTable() throws Exception {
    File tableDir = tempFolder.newFolder("delta-3-table");
    writeDeltaThreeTable(tableDir);

    assertManagedRead(tableDir);
  }

  private void assertManagedRead(File tableDir) {
    PCollection<Row> output =
        readPipeline
            .apply(
                Managed.read(Managed.DELTA_LAKE)
                    .withConfig(ImmutableMap.of("table", tableDir.getAbsolutePath())))
            .getSinglePCollection();

    PAssert.that(output).containsInAnyOrder(TEST_ROWS);
    readPipeline.run().waitUntilFinish();
  }

  private void writeDeltaThreeTable(File tableDir) throws Exception {
    // Keep this fixture self-contained instead of adding a conflicting Delta 3 runtime. The log
    // uses the protocol and metadata emitted by Delta 3.0, while the Parquet data is
    // version-neutral.
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(ROW_SCHEMA);
    List<GenericRecord> records =
        TEST_ROWS.stream()
            .map(row -> AvroUtils.toGenericRecord(row, avroSchema))
            .collect(java.util.stream.Collectors.toList());

    File parquetFile = new File(tableDir, "part-00000.parquet");
    try (ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(new Path(parquetFile.getAbsolutePath()))
            .withSchema(avroSchema)
            .withConf(new org.apache.hadoop.conf.Configuration())
            .build()) {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }

    File logDir = new File(tableDir, "_delta_log");
    if (!logDir.mkdirs()) {
      throw new IllegalStateException("Could not create Delta log directory " + logDir);
    }

    String commit =
        "{\"commitInfo\":{\"timestamp\":123456789,\"operation\":\"WRITE\","
            + "\"engineInfo\":\"Apache-Spark/3.5.0 Delta-Lake/3.0.0\"}}\n"
            + "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
            + "{\"metaData\":{\"id\":\"delta-3-test\",\"format\":{\"provider\":\"parquet\","
            + "\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":["
            + "{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":false,"
            + "\\\"metadata\\\":{}},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\","
            + "\\\"nullable\\\":false,\\\"metadata\\\":{}}]}\",\"partitionColumns\":[],"
            + "\"configuration\":{},\"createdTime\":123456789}}\n"
            + "{\"add\":{\"path\":\"part-00000.parquet\",\"partitionValues\":{},\"size\":"
            + parquetFile.length()
            + ",\"modificationTime\":123456789,\"dataChange\":true}}\n";

    Files.write(
        new File(logDir, "00000000000000000000.json").toPath(),
        commit.getBytes(StandardCharsets.UTF_8));
  }
}
