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
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.DOUBLY_NESTED_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.SINGLY_NESTED_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TIME_CONTAINING_SCHEMA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviderTestHelpers.DATA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviderTestHelpers.prefix;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.PARQUET;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ParquetWriteSchemaTransformFormatProvider}. */
@RunWith(JUnit4.class)
public class ParquetFileWriteSchemaTransformFormatProviderTest {

  private static final FileWriteSchemaTransformFormatProvider PROVIDER =
      FileWriteSchemaTransformFormatProviders.loadProviders().get(PARQUET);

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void allPrimitiveDataTypes() {
    Schema beamSchema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);

    List<GenericRecord> expected =
        DATA.allPrimitiveDataTypesRows.stream()
            .map(AvroUtils.getRowToGenericRecordFunction(avroSchema)::apply)
            .collect(Collectors.toList());

    String folder = "allPrimitiveDataTypes";

    String to = String.format("%s_%s", PARQUET, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(Create.of(DATA.allPrimitiveDataTypesRows).withRowSchema(beamSchema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), beamSchema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<GenericRecord> actual =
        readPipeline.apply(
            ParquetIO.read(avroSchema)
                .from(prefixTo + "/*")
                .withProjection(avroSchema, avroSchema));

    PAssert.that(actual).containsInAnyOrder(expected);

    readPipeline.run();
  }

  @Test
  public void nullableAllPrimitiveDataTypes() {
    Schema beamSchema = NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);

    List<GenericRecord> expected =
        DATA.nullableAllPrimitiveDataTypesRows.stream()
            .map(AvroUtils.getRowToGenericRecordFunction(avroSchema)::apply)
            .collect(Collectors.toList());

    String folder = "nullableAllPrimitiveDataTypes";

    String to = String.format("%s_%s", PARQUET, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.nullableAllPrimitiveDataTypesRows).withRowSchema(beamSchema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), beamSchema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<GenericRecord> actual =
        readPipeline.apply(
            ParquetIO.read(avroSchema)
                .from(prefixTo + "/*")
                .withProjection(avroSchema, avroSchema));

    PAssert.that(actual).containsInAnyOrder(expected);

    readPipeline.run();
  }

  @Test
  public void timeContaining() {
    Schema beamSchema = TIME_CONTAINING_SCHEMA;
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);

    List<GenericRecord> expected =
        DATA.timeContainingRows.stream()
            .map(AvroUtils.getRowToGenericRecordFunction(avroSchema)::apply)
            .collect(Collectors.toList());

    String folder = "timeContaining";

    String to = String.format("%s_%s", PARQUET, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(Create.of(DATA.timeContainingRows).withRowSchema(beamSchema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), beamSchema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<GenericRecord> actual =
        readPipeline.apply(
            ParquetIO.read(avroSchema)
                .from(prefixTo + "/*")
                .withProjection(avroSchema, avroSchema));

    PAssert.that(actual).containsInAnyOrder(expected);

    readPipeline.run();
  }

  @Test
  public void arrayPrimitiveDataTypes() {
    Schema beamSchema = ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);

    List<GenericRecord> expected =
        DATA.arrayPrimitiveDataTypesRows.stream()
            .map(AvroUtils.getRowToGenericRecordFunction(avroSchema)::apply)
            .collect(Collectors.toList());

    String folder = "arrayPrimitiveDataTypes";

    String to = String.format("%s_%s", PARQUET, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(Create.of(DATA.arrayPrimitiveDataTypesRows).withRowSchema(beamSchema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), beamSchema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<GenericRecord> actual =
        readPipeline.apply(
            ParquetIO.read(avroSchema)
                .from(prefixTo + "/*")
                .withProjection(avroSchema, avroSchema));

    PAssert.that(actual).containsInAnyOrder(expected);

    readPipeline.run();
  }

  @Test
  public void singlyNestedDataTypesNoRepeat() {
    Schema beamSchema = SINGLY_NESTED_DATA_TYPES_SCHEMA;
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);

    List<GenericRecord> expected =
        DATA.singlyNestedDataTypesNoRepeatRows.stream()
            .map(AvroUtils.getRowToGenericRecordFunction(avroSchema)::apply)
            .collect(Collectors.toList());

    String folder = "singlyNestedDataTypesNoRepeat";

    String to = String.format("%s_%s", PARQUET, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.singlyNestedDataTypesNoRepeatRows).withRowSchema(beamSchema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), beamSchema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<GenericRecord> actual =
        readPipeline.apply(
            ParquetIO.read(avroSchema)
                .from(prefixTo + "/*")
                .withProjection(avroSchema, avroSchema));

    PAssert.that(actual).containsInAnyOrder(expected);

    readPipeline.run();
  }

  @Test
  public void singlyNestedDataTypesRepeated() {
    Schema beamSchema = SINGLY_NESTED_DATA_TYPES_SCHEMA;
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);

    List<GenericRecord> expected =
        DATA.singlyNestedDataTypesRepeatedRows.stream()
            .map(AvroUtils.getRowToGenericRecordFunction(avroSchema)::apply)
            .collect(Collectors.toList());

    String folder = "singlyNestedDataTypesRepeated";

    String to = String.format("%s_%s", PARQUET, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.singlyNestedDataTypesRepeatedRows).withRowSchema(beamSchema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), beamSchema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<GenericRecord> actual =
        readPipeline.apply(
            ParquetIO.read(avroSchema)
                .from(prefixTo + "/*")
                .withProjection(avroSchema, avroSchema));

    PAssert.that(actual).containsInAnyOrder(expected);

    readPipeline.run();
  }

  @Test
  public void doublyNestedDataTypesNoRepeat() {
    Schema beamSchema = DOUBLY_NESTED_DATA_TYPES_SCHEMA;
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);

    List<GenericRecord> expected =
        DATA.doublyNestedDataTypesNoRepeatRows.stream()
            .map(AvroUtils.getRowToGenericRecordFunction(avroSchema)::apply)
            .collect(Collectors.toList());

    String folder = "doublyNestedDataTypesNoRepeat";

    String to = String.format("%s_%s", PARQUET, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.doublyNestedDataTypesNoRepeatRows).withRowSchema(beamSchema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), beamSchema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<GenericRecord> actual =
        readPipeline.apply(
            ParquetIO.read(avroSchema)
                .from(prefixTo + "/*")
                .withProjection(avroSchema, avroSchema));

    PAssert.that(actual).containsInAnyOrder(expected);

    readPipeline.run();
  }

  @Test
  public void doublyNestedDataTypesRepeat() {
    Schema beamSchema = DOUBLY_NESTED_DATA_TYPES_SCHEMA;
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);

    List<GenericRecord> expected =
        DATA.doublyNestedDataTypesRepeatRows.stream()
            .map(AvroUtils.getRowToGenericRecordFunction(avroSchema)::apply)
            .collect(Collectors.toList());

    String folder = "doublyNestedDataTypesRepeat";

    String to = String.format("%s_%s", PARQUET, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.doublyNestedDataTypesRepeatRows).withRowSchema(beamSchema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), beamSchema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<GenericRecord> actual =
        readPipeline.apply(
            ParquetIO.read(avroSchema)
                .from(prefixTo + "/*")
                .withProjection(avroSchema, avroSchema));

    PAssert.that(actual).containsInAnyOrder(expected);

    readPipeline.run();
  }

  private static FileWriteSchemaTransformConfiguration configuration(String to) {
    return FileWriteSchemaTransformConfiguration.builder()
        .setFormat(PARQUET)
        .setParquetConfiguration(
            FileWriteSchemaTransformConfiguration.parquetConfigurationBuilder()
                .setCompressionCodecName(CompressionCodecName.SNAPPY.name())
                .setRowGroupSize(10)
                .build())
        .setFilenamePrefix(to)
        .build();
  }
}
