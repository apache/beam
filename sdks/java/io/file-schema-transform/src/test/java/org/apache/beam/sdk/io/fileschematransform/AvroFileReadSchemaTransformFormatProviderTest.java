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

import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.SINGLY_NESTED_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TIME_CONTAINING_SCHEMA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviderTestData.DATA;
import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.extensions.avro.io.DynamicAvroDestinations;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.fs.ResourceId;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AvroFileReadSchemaTransformFormatProviderTest {

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  private String filePath(String testName) {
    return folder(testName) + "/test";
  }

  private String folder(String testName) {
    try {
      return tmpFolder.newFolder("avro", testName).getAbsolutePath();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public void runWriteAndReadTest(Schema schema, List<Row> rows, String filePath) {
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
    String stringSchema = avroSchema.toString();

    writePipeline
        .apply(Create.of(rows).withRowSchema(schema))
        .apply(
            MapElements.into(TypeDescriptor.of(GenericRecord.class))
                .via(AvroUtils.getRowToGenericRecordFunction(avroSchema)))
        .setCoder(AvroGenericCoder.of(avroSchema))
        .apply(AvroIO.writeGenericRecords(avroSchema).to(filePath));
    writePipeline.run().waitUntilFinish();

    FileReadSchemaTransformConfiguration config =
        FileReadSchemaTransformConfiguration.builder()
            .setFormat("avro")
            .setSchema(stringSchema)
            .setFilepattern(filePath + "*")
            .build();

    SchemaTransform readTransform = new FileReadSchemaTransformProvider().from(config);
    PCollectionRowTuple output =
        PCollectionRowTuple.empty(readPipeline).apply(readTransform.buildTransform());

    PAssert.that(output.get(FileReadSchemaTransformProvider.OUTPUT_TAG)).containsInAnyOrder(rows);
    readPipeline.run();
  }

  @Test
  public void testAllPrimitiveDataTypes() {
    Schema schema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.allPrimitiveDataTypesRows;
    String filePath = filePath("testAllPrimitiveDataTypes");

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
  public void testArrayPrimitiveDataTypes() {
    Schema schema = ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.arrayPrimitiveDataTypesRows;
    String filePath = filePath("testArrayPrimitiveDataTypes");

    runWriteAndReadTest(schema, rows, filePath);
  }

  @Test
  public void testNestedRepeatedDataTypes() {
    Schema schema = SINGLY_NESTED_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.singlyNestedDataTypesRepeatedRows;
    String filePath = filePath("testNestedRepeatedDataTypes");

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
      Row row = DATA.allPrimitiveDataTypesRows.get(l.intValue());
      return AvroUtils.getRowToGenericRecordFunction(AvroUtils.toAvroSchema(schema)).apply(row);
    }
  }

  @Test
  public void testStreamingRead() {
    Schema schema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.allPrimitiveDataTypesRows;

    String folder = folder("testStreamingRead");
    ResourceId dir = FileSystems.matchNewResource(folder, true);

    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
    String stringSchema = avroSchema.toString();
    FileReadSchemaTransformConfiguration config =
        FileReadSchemaTransformConfiguration.builder()
            .setFormat("avro")
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
            AvroIO.writeGenericRecords(avroSchema)
                .to(new TestDynamicDestinations(dir))
                .withTempDirectory(dir)
                .withNumShards(1)
                .withWindowedWrites());

    // Check output matches the expected rows
    PAssert.that(output.get(FileReadSchemaTransformProvider.OUTPUT_TAG)).containsInAnyOrder(rows);
    readPipeline.run();
  }

  @Test
  public void testReadWithPCollectionOfFilepatterns() {
    Schema schema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.allPrimitiveDataTypesRows;

    String folder = folder("testReadWithPCollectionOfFilepatterns");

    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
    ResourceId dir = FileSystems.matchNewResource(folder, true);
    // Write rows to dynamic destinations (test_1.., test_2.., test_3..)
    writePipeline
        .apply(Create.of(rows).withRowSchema(schema))
        .apply(
            MapElements.into(TypeDescriptor.of(GenericRecord.class))
                .via(AvroUtils.getRowToGenericRecordFunction(avroSchema)))
        .setCoder(AvroGenericCoder.of(avroSchema))
        .apply(
            AvroIO.writeGenericRecords(avroSchema)
                .to(new TestDynamicDestinations(dir))
                .withTempDirectory(dir));
    writePipeline.run().waitUntilFinish();

    // We will get filepatterns from the input PCollection, so don't set filepattern field here
    String stringSchema = avroSchema.toString();
    FileReadSchemaTransformConfiguration config =
        FileReadSchemaTransformConfiguration.builder()
            .setFormat("avro")
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

  private static class TestDynamicDestinations
      extends DynamicAvroDestinations<GenericRecord, String, GenericRecord> {
    final ResourceId baseDir;

    TestDynamicDestinations(ResourceId baseDir) {
      this.baseDir = baseDir;
    }

    @Override
    public org.apache.avro.Schema getSchema(String destination) {
      return AvroUtils.toAvroSchema(ALL_PRIMITIVE_DATA_TYPES_SCHEMA);
    }

    @Override
    public GenericRecord formatRecord(GenericRecord record) {
      return record;
    }

    @Override
    public String getDestination(GenericRecord element) {
      // Destination will be either test_1, test_2, or test_3 depending on the value of
      // anInteger field.
      return element.get("anInteger").toString();
    }

    @Override
    public String getDefaultDestination() {
      return "";
    }

    @Override
    public FileBasedSink.FilenamePolicy getFilenamePolicy(String destination) {
      return DefaultFilenamePolicy.fromStandardParameters(
          StaticValueProvider.of(baseDir.resolve("test_" + destination, RESOLVE_FILE)),
          "-SSSSS-of-NNNNN",
          ".avro",
          false);
    }
  }
}
