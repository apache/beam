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
import static org.apache.beam.sdk.io.fileschematransform.FileReadSchemaTransformProvider.FILEPATTERN_ROW_FIELD_NAME;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviderTestData.DATA;
import static org.apache.beam.sdk.transforms.Contextful.fn;

import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.testing.PAssert;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.joda.time.Duration;
import org.junit.Test;

public class ParquetReadSchemaTransformFormatProviderTest
    extends FileReadSchemaTransformFormatProviderTest {

  @Override
  protected String getFormat() {
    return new ParquetReadSchemaTransformFormatProvider().identifier();
  }

  @Override
  public String getStringSchemaFromBeamSchema(Schema beamSchema) {
    return AvroUtils.toAvroSchema(beamSchema).toString();
  }

  @Override
  public void runWriteAndReadTest(
      Schema schema, List<Row> rows, String folderPath, String schemaFilePath) {
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
    String stringSchema =
        Strings.isNullOrEmpty(schemaFilePath) ? avroSchema.toString() : schemaFilePath;

    writePipeline
        .apply(Create.of(rows).withRowSchema(schema))
        .apply(
            MapElements.into(TypeDescriptor.of(GenericRecord.class))
                .via(AvroUtils.getRowToGenericRecordFunction(avroSchema)))
        .setCoder(AvroCoder.of(avroSchema))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(avroSchema))
                .to(folderPath)
                .withSuffix(".parquet"));
    writePipeline.run().waitUntilFinish();

    FileReadSchemaTransformConfiguration config =
        FileReadSchemaTransformConfiguration.builder()
            .setFormat(getFormat())
            .setSchema(stringSchema)
            // FileIO write with sink writes to a directory, so we read everything in that directory
            .setFilepattern(folderPath + "/*")
            .build();

    SchemaTransform readTransform = new FileReadSchemaTransformProvider().from(config);
    PCollectionRowTuple output = PCollectionRowTuple.empty(readPipeline).apply(readTransform);

    PAssert.that(output.get(FileReadSchemaTransformProvider.OUTPUT_TAG)).containsInAnyOrder(rows);
    readPipeline.run();
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

    String folder = getFolder();

    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
    String stringSchema = avroSchema.toString();
    FileReadSchemaTransformConfiguration config =
        FileReadSchemaTransformConfiguration.builder()
            .setFormat(getFormat())
            .setFilepattern(folder + "/test_*")
            .setSchema(stringSchema)
            .setPollIntervalMillis(100L)
            .setTerminateAfterSecondsSinceNewOutput(3L)
            .build();
    SchemaTransform readTransform = new FileReadSchemaTransformProvider().from(config);

    PCollectionRowTuple output = PCollectionRowTuple.empty(readPipeline).apply(readTransform);

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
        .setCoder(AvroCoder.of(avroSchema))
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
    Schema schema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.allPrimitiveDataTypesRows;

    String folder = getFolder();

    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
    // Write rows to dynamic destinations (test_1.., test_2.., test_3..)
    writePipeline
        .apply(Create.of(rows).withRowSchema(schema))
        .apply(
            MapElements.into(TypeDescriptor.of(GenericRecord.class))
                .via(AvroUtils.getRowToGenericRecordFunction(avroSchema)))
        .setCoder(AvroCoder.of(avroSchema))
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
            .setFormat(getFormat())
            .setSchema(stringSchema)
            .build();
    SchemaTransform readTransform = new FileReadSchemaTransformProvider().from(config);

    // Create an PCollection<Row> of filepatterns and feed into the read transform
    Schema patternSchema = getFilepatternSchema();
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
                                .withFieldValue(FILEPATTERN_ROW_FIELD_NAME, pattern)
                                .build()))
            .setRowSchema(patternSchema);

    PCollectionRowTuple output =
        PCollectionRowTuple.of(FileReadSchemaTransformProvider.INPUT_TAG, filepatterns)
            .apply(readTransform);

    // Check output matches with expected rows
    PAssert.that(output.get(FileReadSchemaTransformProvider.OUTPUT_TAG)).containsInAnyOrder(rows);
    readPipeline.run();
  }
}
