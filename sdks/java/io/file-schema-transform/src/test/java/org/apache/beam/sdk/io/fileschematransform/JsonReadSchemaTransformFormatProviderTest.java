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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.payloads.JsonPayloadSerializerProvider;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.junit.Test;

public class JsonReadSchemaTransformFormatProviderTest
    extends FileReadSchemaTransformFormatProviderTest {

  @Override
  protected String getFormat() {
    return new JsonReadSchemaTransformFormatProvider().identifier();
  }

  @Override
  public String getStringSchemaFromBeamSchema(Schema beamSchema) {
    return JsonUtils.jsonSchemaStringFromBeamSchema(beamSchema);
  }

  // Formats input schema to have the expected Beam FieldTypes that JSON schema supports
  private static Schema getExpectedSchema(Schema inputRowSchema) {
    Schema.Builder outputSchemaBuilder = Schema.builder();
    for (Field field : inputRowSchema.getFields()) {
      FieldType outputType;
      switch (field.getType().getTypeName()) {
        case ROW:
          outputType = FieldType.row(getExpectedSchema(field.getType().getRowSchema()));
          break;
        case ARRAY:
        case ITERABLE:
          FieldType arrayType =
              getExpectedPrimitiveType(field.getType().getCollectionElementType());
          outputType = FieldType.array(arrayType);
          break;
        default:
          outputType = getExpectedPrimitiveType(field.getType());
      }
      outputSchemaBuilder.addField(field.getName(), outputType);
    }
    return outputSchemaBuilder.build();
  }

  // Returns the expected Beam FieldType that a JSON property type supports.
  // e.g. JsonUtils turns all JSON ints to INT64, all other numbers to DOUBLE
  private static FieldType getExpectedPrimitiveType(FieldType inputType) {
    FieldType outputType;
    switch (inputType.getTypeName()) {
      case BYTE:
      case INT32:
      case INT64:
        outputType = FieldType.INT64.withNullable(inputType.getNullable());
        break;
      case DECIMAL:
      case FLOAT:
      case DOUBLE:
        outputType = FieldType.DOUBLE.withNullable(inputType.getNullable());
        break;
      default:
        outputType = inputType;
    }
    return outputType;
  }

  // Returns a Row with values that are in accordance with the expected Beam FieldTypes that JSON
  // schema supports.
  // JSON schemas don't make a distinction between different integer types (ie int32 vs int64)
  // or different floating-point types (ie float vs double).
  // When converting to Beam Rows, we default to choosing the type that is less likely to overflow.
  // The following line reformats the expected rows to account for this.
  public static Row getExpectedRow(Row inputRow) {
    Schema outputSchema = getExpectedSchema(inputRow.getSchema());
    Map<String, Object> outputRowFields = new HashMap<>();

    for (Field inputField : inputRow.getSchema().getFields()) {
      String fieldName = inputField.getName();
      Object outputValue = getExpectedValue(inputField.getType(), inputRow.getValue(fieldName));

      outputRowFields.put(fieldName, outputValue);
    }
    return Row.withSchema(outputSchema).withFieldValues(outputRowFields).build();
  }

  // Returns the expected value in accordance with the expected type.
  private static Object getExpectedValue(FieldType inputType, Object inputValue) {
    FieldType expectedType = getExpectedPrimitiveType(inputType);
    Object outputValue;
    switch (expectedType.getTypeName()) {
      case INT64:
        outputValue = inputValue == null ? null : Long.parseLong(String.valueOf(inputValue));
        break;
      case DOUBLE:
        outputValue = inputValue == null ? null : Double.parseDouble(String.valueOf(inputValue));
        break;
      case ROW:
        outputValue = getExpectedRow((Row) inputValue);
        break;
      case ARRAY:
        outputValue =
            ((List<Object>) inputValue)
                .stream()
                    .map(val -> getExpectedValue(inputType.getCollectionElementType(), val))
                    .collect(Collectors.toList());
        break;
      default:
        outputValue = inputValue;
    }
    return outputValue;
  }

  @Override
  public void runWriteAndReadTest(
      Schema schema, List<Row> rows, String filePath, String schemaFilePath) {
    String jsonStringSchema =
        Strings.isNullOrEmpty(schemaFilePath)
            ? JsonUtils.jsonSchemaStringFromBeamSchema(schema)
            : schemaFilePath;

    PayloadSerializer payloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(schema, ImmutableMap.of());
    writePipeline
        .apply(Create.of(rows).withRowSchema(schema))
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(row -> new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8)))
        .setCoder(StringUtf8Coder.of())
        .apply(TextIO.write().to(filePath));
    writePipeline.run().waitUntilFinish();

    FileReadSchemaTransformConfiguration config =
        FileReadSchemaTransformConfiguration.builder()
            .setFormat(getFormat())
            .setSchema(jsonStringSchema)
            .setFilepattern(filePath + "*")
            .build();

    SchemaTransform readTransform = new FileReadSchemaTransformProvider().from(config);
    PCollectionRowTuple output = PCollectionRowTuple.empty(readPipeline).apply(readTransform);

    List<Row> expectedRows =
        rows.stream().map(row -> getExpectedRow(row)).collect(Collectors.toList());

    PAssert.that(output.get(FileReadSchemaTransformProvider.OUTPUT_TAG))
        .containsInAnyOrder(expectedRows);
    readPipeline.run();
  }

  private static class CreateKVJsonString extends SimpleFunction<Long, KV<Integer, String>> {
    Schema schema;
    PayloadSerializer payloadSerializer;

    CreateKVJsonString(Schema schema, PayloadSerializer payloadSerializer) {
      this.schema = schema;
      this.payloadSerializer = payloadSerializer;
    }

    @Override
    public KV<Integer, String> apply(Long l) {
      Row row = DATA.allPrimitiveDataTypesRows.get(l.intValue());
      String jsonString = new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8);

      return KV.of(l.intValue() + 1, jsonString);
    }
  }

  @Test
  public void testStreamingRead() {
    Schema schema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.allPrimitiveDataTypesRows;

    String folder = getFolder();

    String jsonStringSchema = JsonUtils.jsonSchemaStringFromBeamSchema(schema);
    FileReadSchemaTransformConfiguration config =
        FileReadSchemaTransformConfiguration.builder()
            .setFormat(getFormat())
            .setFilepattern(folder + "/test_*")
            .setSchema(jsonStringSchema)
            .setPollIntervalMillis(100L)
            .setTerminateAfterSecondsSinceNewOutput(3L)
            .build();
    SchemaTransform readTransform = new FileReadSchemaTransformProvider().from(config);

    PCollectionRowTuple output = PCollectionRowTuple.empty(readPipeline).apply(readTransform);

    PayloadSerializer payloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(schema, ImmutableMap.of());
    // Write to three different files (test_1..., test_2..., test_3)
    // All three new files should be picked up and read.
    readPipeline
        .apply(GenerateSequence.from(0).to(3).withRate(1, Duration.millis(300)))
        .apply(
            Window.<Long>into(FixedWindows.of(Duration.millis(100)))
                .withAllowedLateness(Duration.ZERO)
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .discardingFiredPanes())
        .apply(MapElements.via(new CreateKVJsonString(schema, payloadSerializer)))
        .setCoder(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()))
        .apply(
            FileIO.<Integer, KV<Integer, String>>writeDynamic()
                .by(KV::getKey)
                .via(fn(KV<Integer, String>::getValue), TextIO.sink())
                .to(folder)
                .withNaming(integer -> FileIO.Write.defaultNaming("test_" + integer, ".json"))
                .withDestinationCoder(VarIntCoder.of())
                .withNumShards(1));

    // Check output matches the expected rows
    List<Row> expectedRows =
        rows.stream().map(row -> getExpectedRow(row)).collect(Collectors.toList());

    PAssert.that(output.get(FileReadSchemaTransformProvider.OUTPUT_TAG))
        .containsInAnyOrder(expectedRows);
    readPipeline.run();
  }

  @Test
  public void testReadWithPCollectionOfFilepatterns() {
    Schema schema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.allPrimitiveDataTypesRows;

    String folder = getFolder();

    PayloadSerializer payloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(schema, ImmutableMap.of());
    // Write rows to dynamic destinations (test_1.., test_2.., test_3..)
    writePipeline
        .apply(Create.of(Arrays.asList(0L, 1L, 2L)))
        .apply(MapElements.via(new CreateKVJsonString(schema, payloadSerializer)))
        .setCoder(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()))
        .apply(
            FileIO.<Integer, KV<Integer, String>>writeDynamic()
                .by(KV::getKey)
                .via(fn(KV<Integer, String>::getValue), TextIO.sink())
                .to(folder)
                .withNaming(integer -> FileIO.Write.defaultNaming("test_" + integer, ".json"))
                .withDestinationCoder(VarIntCoder.of())
                .withNumShards(1));
    writePipeline.run().waitUntilFinish();

    // We will get filepatterns from the input PCollection, so don't set filepattern field here
    String jsonStringSchema = JsonUtils.jsonSchemaStringFromBeamSchema(schema);
    FileReadSchemaTransformConfiguration config =
        FileReadSchemaTransformConfiguration.builder()
            .setFormat(getFormat())
            .setSchema(jsonStringSchema)
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
    List<Row> expectedRows =
        rows.stream().map(row -> getExpectedRow(row)).collect(Collectors.toList());

    PAssert.that(output.get(FileReadSchemaTransformProvider.OUTPUT_TAG))
        .containsInAnyOrder(expectedRows);
    readPipeline.run();
  }
}
