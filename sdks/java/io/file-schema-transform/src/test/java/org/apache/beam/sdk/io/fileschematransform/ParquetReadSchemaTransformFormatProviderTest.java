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
package org.apache.beam.sdk.io.fileschematransform;

import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.AVRO_ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.AVRO_NESTED_REPEATED_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.AVRO_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TIME_CONTAINING_SCHEMA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviderTestData.DATA;
import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;
import static org.apache.beam.sdk.transforms.Contextful.fn;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.extensions.avro.io.DynamicAvroDestinations;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.apache.beam.sdk.io.FileIO;

public class ParquetReadSchemaTransformFormatProviderTest {

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  private String filePath(String testName) {
    return folder(testName) + "/test";
  }

  private String folder(String testName) {
    try {
      return tmpFolder.newFolder("parquet", testName).getAbsolutePath();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public void runWriteAndReadTest(Schema schema, List<Row> rows, String folderPath) {
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
    String stringSchema = avroSchema.toString();

    writePipeline
        .apply(Create.of(rows).withRowSchema(schema))
        .apply(
            MapElements.into(TypeDescriptor.of(GenericRecord.class))
                .via(AvroUtils.getRowToGenericRecordFunction(avroSchema)))
        .setCoder(AvroGenericCoder.of(avroSchema))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(avroSchema))
                .to(folderPath)
                .withSuffix(".parquet"));
    writePipeline.run().waitUntilFinish();

    FileReadSchemaTransformConfiguration config =
        FileReadSchemaTransformConfiguration.builder()
            .setFormat("parquet")
            .setSchema(stringSchema)
            // FileIO write with sink writes to a directory
            .setFilepattern(folderPath + "/*")
            .build();

    SchemaTransform readTransform = new FileReadSchemaTransformProvider().from(config);
    PCollectionRowTuple output =
        PCollectionRowTuple.empty(readPipeline).apply(readTransform.buildTransform());

    PAssert.that(output.get(FileReadSchemaTransformProvider.OUTPUT_TAG)).containsInAnyOrder(rows);
    readPipeline.run();
  }

  @Test
  public void testAvroPrimitiveDataTypes() {
    Schema schema = AVRO_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.avroPrimitiveDataTypesRows;
    String filePath = filePath("testAvroPrimitiveDataTypes");

    runWriteAndReadTest(schema, rows, filePath);
  }

  @Test
  public void testNullableAllPrimitiveDataTypes() {
    Schema schema = NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.nullableAllPrimitiveDataTypesRows;
    String filePath = filePath("testNullableAllPrimitiveDataTypes");

    runWriteAndReadTest(schema, rows, filePath);
  }

  @Test
  public void testTimeContaining() {
    Schema schema = TIME_CONTAINING_SCHEMA;
    List<Row> rows = DATA.timeContainingRows;
    String filePath = filePath("testTimeContaining");

    runWriteAndReadTest(schema, rows, filePath);
  }

  @Test
  public void testArrayAvroPrimitiveDataTypes() {
    Schema schema = AVRO_ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.avroArrayPrimitiveDataTypesRows;
    String filePath = filePath("testArrayAvroPrimitiveDataTypes");

    runWriteAndReadTest(schema, rows, filePath);
  }

  @Test
  public void testAvroNestedDataTypes() {
    Schema schema = AVRO_NESTED_REPEATED_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.avroNestedRepeatedRows;
    String filePath = filePath("testAvroNestedDataTypes");

    runWriteAndReadTest(schema, rows, filePath);
  }

  private static class CreateAvroPrimitiveGenericRecord
      extends SimpleFunction<Long, GenericRecord> {
    Schema schema;

    CreateAvroPrimitiveGenericRecord(Schema schema) {
      this.schema = schema;
    }

    @Override
    public GenericRecord apply(Long l) {
      Row row = DATA.avroPrimitiveDataTypesRows.get(l.intValue());
      return AvroUtils.getRowToGenericRecordFunction(AvroUtils.toAvroSchema(schema)).apply(row);
    }
  }

  @Test
  public void testStreamingRead() {
    Schema schema = AVRO_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.avroPrimitiveDataTypesRows;

    String folder = folder("testStreamingRead");

    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
    String stringSchema = avroSchema.toString();
    FileReadSchemaTransformConfiguration config =
        FileReadSchemaTransformConfiguration.builder()
            .setFormat("parquet")
            .setFilepattern(folder + "/test_*")
            .setSchema(stringSchema)
            .setPollIntervalMillis(100)
            .setTerminateAfterSecondsSinceNewOutput(3)
            .build();
    SchemaTransform readTransform = new FileReadSchemaTransformProvider().from(config);

    PCollectionRowTuple output =
        PCollectionRowTuple.empty(readPipeline).apply(readTransform.buildTransform());

    // Write to three different files (test_1..., test_2..., test_3)
    // All three new files should be picked up and read.
    readPipeline
        .apply(GenerateSequence.from(0).to(3).withRate(1, Duration.millis(300)))
        .apply(
            Window.<Long>into(FixedWindows.of(Duration.millis(100)))
                .withAllowedLateness(Duration.ZERO)
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .discardingFiredPanes())
        .apply(MapElements.via(new CreateAvroPrimitiveGenericRecord(schema)))
        .setCoder(AvroGenericCoder.of(avroSchema))
        .apply(
            FileIO.<String, GenericRecord>writeDynamic()
                .by(fn((GenericRecord element) -> element.get("anInteger").toString()))
                .via(ParquetIO.sink(avroSchema))
                .to(folder)
                .withNaming(integer -> FileIO.Write.defaultNaming("test_" + integer, ".parquet"))
                .withDestinationCoder(StringUtf8Coder.of())
                .withNumShards(1));

    // Check output matches the expected rows
    PAssert.that(output.get(FileReadSchemaTransformProvider.OUTPUT_TAG)).containsInAnyOrder(rows);
    readPipeline.run();
  }

  @Test
  public void testReadWithPCollectionOfFilepatterns() {
    Schema schema = AVRO_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.avroPrimitiveDataTypesRows;

    String folder = folder("testReadWithPCollectionOfFilepatterns");

    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
    // Write rows to dynamic destinations (test_1.., test_2.., test_3..)
    writePipeline
        .apply(Create.of(rows).withRowSchema(schema))
        .apply(
            MapElements.into(TypeDescriptor.of(GenericRecord.class))
                .via(AvroUtils.getRowToGenericRecordFunction(avroSchema)))
        .setCoder(AvroGenericCoder.of(avroSchema))
        .apply(
            FileIO.<String, GenericRecord>writeDynamic()
                .by(fn((GenericRecord element) -> element.get("anInteger").toString()))
                .via(ParquetIO.sink(avroSchema))
                .to(folder)
                .withNaming(integer -> FileIO.Write.defaultNaming("test_" + integer, ".parquet"))
                .withDestinationCoder(StringUtf8Coder.of()));
    writePipeline.run().waitUntilFinish();

    // We will get filepatterns from the input PCollection, so don't set filepattern field here
    String stringSchema = avroSchema.toString();
    FileReadSchemaTransformConfiguration config =
        FileReadSchemaTransformConfiguration.builder()
            .setFormat("parquet")
            .setSchema(stringSchema)
            .build();
    SchemaTransform readTransform = new FileReadSchemaTransformProvider().from(config);

    // Create an PCollection<Row> of filepatterns and feed into the read transform
    Schema patternSchema = Schema.of(Field.of("filepattern", FieldType.STRING));
    PCollection<Row> filepatterns =
        readPipeline
            .apply(
                Create.of(
                    Arrays.asList(
                        folder + "/test_1-*", folder + "/test_2-*", folder + "/test_3-*")))
            .apply(
                "Create Rows of filepatterns",
                MapElements.into(TypeDescriptors.rows())
                    .via(
                        pattern ->
                            Row.withSchema(patternSchema)
                                .withFieldValue("filepattern", pattern)
                                .build()))
            .setRowSchema(patternSchema);

    PCollectionRowTuple output =
        PCollectionRowTuple.of(FileReadSchemaTransformProvider.INPUT_TAG, filepatterns)
            .apply(readTransform.buildTransform());

    // Check output matches with expected rows
    PAssert.that(output.get(FileReadSchemaTransformProvider.OUTPUT_TAG)).containsInAnyOrder(rows);
    readPipeline.run();
  }
}
