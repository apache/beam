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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ReadFooterSchemaTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final Schema FLAT_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "name", Types.StringType.get()));

  private static final Schema NESTED_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(
              2,
              "address",
              Types.StructType.of(
                  optional(3, "city", Types.StringType.get()),
                  optional(4, "zip", Types.IntegerType.get()))),
          optional(5, "tags", Types.ListType.ofOptional(6, Types.StringType.get())),
          optional(
              7,
              "attributes",
              Types.MapType.ofOptional(8, 9, Types.StringType.get(), Types.DoubleType.get())));

  @Test
  public void testEmitsFlatSchema() throws IOException {
    String file = writeParquet("flat.parquet", FLAT_SCHEMA, record(FLAT_SCHEMA, "id", 1));

    assertSchemas(run(file), FLAT_SCHEMA);

    pipeline.run();
  }

  @Test
  public void testEmitsNestedSchema() throws IOException {
    String file = writeParquet("nested.parquet", NESTED_SCHEMA, record(NESTED_SCHEMA, "id", 1L));

    assertSchemas(run(file), NESTED_SCHEMA);

    pipeline.run();
  }

  /** Iceberg's writer creates no file for zero rows, so this uses parquet-avro directly. */
  @Test
  public void testZeroRowParquetEmitsSchema() throws IOException {
    String file = new File(temporaryFolder.getRoot(), "empty.parquet").getAbsolutePath();
    org.apache.avro.Schema avroSchema =
        SchemaBuilder.record("flat").fields().requiredInt("id").optionalString("name").endRecord();
    ParquetWriter<Object> writer =
        AvroParquetWriter.builder(new Path(file)).withSchema(avroSchema).build();
    writer.close();

    assertSchemas(run(file), FLAT_SCHEMA);

    pipeline.run();
  }

  @Test
  public void testMissingFileEmitsNothing() {
    String file = new File(temporaryFolder.getRoot(), "missing.parquet").getAbsolutePath();

    PAssert.that(run(file)).empty();

    pipeline.run();
  }

  @Test
  public void testGarbageBytesEmitNothing() throws IOException {
    String file =
        writeBytes("garbage.parquet", "not a parquet file".getBytes(StandardCharsets.UTF_8));

    PAssert.that(run(file)).empty();

    pipeline.run();
  }

  @Test
  public void testTruncatedParquetEmitsNothing() throws IOException {
    String good = writeParquet("good.parquet", FLAT_SCHEMA, record(FLAT_SCHEMA, "id", 1));
    byte[] bytes = Files.readAllBytes(new File(good).toPath());
    String file = writeBytes("truncated.parquet", Arrays.copyOf(bytes, bytes.length / 2));

    PAssert.that(run(file)).empty();

    pipeline.run();
  }

  @Test
  public void testZeroByteFileEmitsNothing() throws IOException {
    String file = writeBytes("zero.parquet", new byte[0]);

    PAssert.that(run(file)).empty();

    pipeline.run();
  }

  @Test
  public void testNonParquetEmitsNothing() throws IOException {
    String avro = writeBytes("data.avro", new byte[0]);
    String unknown = writeBytes("data.txt", new byte[0]);

    PAssert.that(run(avro, unknown)).empty();

    pipeline.run();
  }

  @Test
  public void testPermutedColumnsProduceIdenticalSchema() throws IOException {
    Schema permuted =
        new Schema(
            optional(1, "name", Types.StringType.get()),
            required(2, "id", Types.IntegerType.get()));
    String a = writeParquet("a.parquet", FLAT_SCHEMA, record(FLAT_SCHEMA, "id", 1));
    String b = writeParquet("b.parquet", permuted, record(permuted, "id", 1));

    PAssert.that(run(a, b))
        .satisfies(
            actual -> {
              List<String> jsons = new ArrayList<>();
              actual.forEach(jsons::add);
              assertEquals(2, jsons.size());
              assertEquals(jsons.get(0), jsons.get(1));
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testMixedBundleEmitsOnlyReadableSchemas() throws IOException {
    String flat = writeParquet("flat.parquet", FLAT_SCHEMA, record(FLAT_SCHEMA, "id", 1));
    String nested = writeParquet("nested.parquet", NESTED_SCHEMA, record(NESTED_SCHEMA, "id", 1L));
    String garbage = writeBytes("garbage.parquet", "garbage".getBytes(StandardCharsets.UTF_8));
    String avro = writeBytes("data.avro", new byte[0]);
    String missing = new File(temporaryFolder.getRoot(), "missing.parquet").getAbsolutePath();

    assertSchemas(run(flat, nested, garbage, avro, missing), FLAT_SCHEMA, NESTED_SCHEMA);

    PipelineResult result = pipeline.run();

    assertEquals(5L, counter(result, ReadFooterSchema.FILES_READ_COUNTER));
    assertEquals(2L, counter(result, ReadFooterSchema.SCHEMAS_EMITTED_COUNTER));
    assertEquals(2L, counter(result, ReadFooterSchema.FOOTER_READ_ERRORS_COUNTER));
  }

  private static long counter(PipelineResult result, String name) {
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(ReadFooterSchema.class, name))
                    .build());
    long total = 0;
    for (MetricResult<Long> counter : metrics.getCounters()) {
      total += counter.getAttempted();
    }
    return total;
  }

  private PCollection<String> run(String... paths) {
    return pipeline.apply(Create.of(Arrays.asList(paths))).apply(ParDo.of(new ReadFooterSchema()));
  }

  /** Asserts the emitted schemas equal the canonical forms of {@code expected}, in any order. */
  private static void assertSchemas(PCollection<String> out, Schema... expected) {
    List<String> expectedJson = new ArrayList<>();
    for (Schema schema : expected) {
      expectedJson.add(SchemaParser.toJson(FileSchemas.canonical(schema)));
    }
    PAssert.that(out)
        .satisfies(
            actual -> {
              List<String> remaining = new ArrayList<>(expectedJson);
              for (String json : actual) {
                Schema schema = SchemaParser.fromJson(json);
                boolean matched = false;
                for (int i = 0; i < remaining.size(); i++) {
                  if (SchemaParser.fromJson(remaining.get(i)).sameSchema(schema)) {
                    remaining.remove(i);
                    matched = true;
                    break;
                  }
                }
                assertTrue("Unexpected schema: " + json, matched);
              }
              assertEquals("Missing schemas: " + remaining, 0, remaining.size());
              return null;
            });
  }

  private String writeParquet(String name, Schema schema, Record... records) throws IOException {
    String file = new File(temporaryFolder.getRoot(), name).getAbsolutePath();
    DataWriter<Record> writer =
        Parquet.writeData(org.apache.iceberg.Files.localOutput(file))
            .schema(schema)
            .withSpec(PartitionSpec.unpartitioned())
            .createWriterFunc(GenericParquetWriter::create)
            .build();
    try {
      for (Record record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }
    return file;
  }

  private String writeBytes(String name, byte[] bytes) throws IOException {
    File file = new File(temporaryFolder.getRoot(), name);
    Files.write(file.toPath(), bytes);
    return file.getAbsolutePath();
  }

  private static Record record(Schema schema, String field, Object value) {
    return GenericRecord.create(schema).copy(field, value);
  }
}
