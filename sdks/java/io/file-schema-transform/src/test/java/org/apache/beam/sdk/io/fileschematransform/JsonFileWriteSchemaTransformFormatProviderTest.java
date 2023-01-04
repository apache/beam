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
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.JSON;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.JsonPayloadSerializerProvider;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link JsonWriteSchemaTransformFormatProvider}. */
@RunWith(JUnit4.class)
public class JsonFileWriteSchemaTransformFormatProviderTest {

  private static final FileWriteSchemaTransformFormatProvider PROVIDER =
      FileWriteSchemaTransformFormatProviders.loadProviders().get(JSON);

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void allPrimitiveDataTypes() {
    Schema schema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    String folder = "allPrimitiveDataTypes";

    String to = String.format("%s_%s", JSON, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(Create.of(DATA.allPrimitiveDataTypesRows).withRowSchema(schema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), schema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<String> actual = readPipeline.apply(TextIO.read().from(prefixTo + "*"));

    PayloadSerializer payloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(schema, ImmutableMap.of());

    PAssert.that(actual)
        .containsInAnyOrder(
            DATA.allPrimitiveDataTypesRows.stream()
                .map(
                    (Row row) ->
                        new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8))
                .collect(Collectors.toList()));

    readPipeline.run();
  }

  @Test
  public void nullableAllPrimitiveDataTypes() {
    Schema schema = NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    String folder = "nullableAllPrimitiveDataTypes";

    String to = String.format("%s_%s", JSON, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.nullableAllPrimitiveDataTypesRows).withRowSchema(schema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), schema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<String> actual = readPipeline.apply(TextIO.read().from(prefixTo + "*"));

    PayloadSerializer payloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(schema, ImmutableMap.of());

    PAssert.that(actual)
        .containsInAnyOrder(
            DATA.nullableAllPrimitiveDataTypesRows.stream()
                .map(
                    (Row row) ->
                        new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8))
                .collect(Collectors.toList()));

    readPipeline.run();
  }

  @Test
  public void timeContaining() {
    Schema schema = TIME_CONTAINING_SCHEMA;
    String folder = "timeContaining";

    String to = String.format("%s_%s", JSON, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(Create.of(DATA.timeContainingRows).withRowSchema(schema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), schema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<String> actual = readPipeline.apply(TextIO.read().from(prefixTo + "*"));

    PayloadSerializer payloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(schema, ImmutableMap.of());

    PAssert.that(actual)
        .containsInAnyOrder(
            DATA.timeContainingRows.stream()
                .map(
                    (Row row) ->
                        new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8))
                .collect(Collectors.toList()));

    readPipeline.run();
  }

  @Test
  public void arrayPrimitiveDataTypes() {
    Schema schema = ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
    String folder = "arrayPrimitiveDataTypes";

    String to = String.format("%s_%s", JSON, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(Create.of(DATA.arrayPrimitiveDataTypesRows).withRowSchema(schema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), schema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<String> actual = readPipeline.apply(TextIO.read().from(prefixTo + "*"));

    PayloadSerializer payloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(schema, ImmutableMap.of());

    PAssert.that(actual)
        .containsInAnyOrder(
            DATA.arrayPrimitiveDataTypesRows.stream()
                .map(
                    (Row row) ->
                        new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8))
                .collect(Collectors.toList()));

    readPipeline.run();
  }

  @Test
  public void singlyNestedDataTypesNoRepeat() {
    Schema schema = SINGLY_NESTED_DATA_TYPES_SCHEMA;
    String folder = "singlyNestedDataTypesNoRepeat";

    String to = String.format("%s_%s", JSON, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.singlyNestedDataTypesNoRepeatRows).withRowSchema(schema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), schema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<String> actual = readPipeline.apply(TextIO.read().from(prefixTo + "*"));

    PayloadSerializer payloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(schema, ImmutableMap.of());

    PAssert.that(actual)
        .containsInAnyOrder(
            DATA.singlyNestedDataTypesNoRepeatRows.stream()
                .map(
                    (Row row) ->
                        new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8))
                .collect(Collectors.toList()));

    readPipeline.run();
  }

  @Test
  public void singlyNestedDataTypesRepeated() {
    Schema schema = SINGLY_NESTED_DATA_TYPES_SCHEMA;
    String folder = "singlyNestedDataTypesRepeated";

    String to = String.format("%s_%s", JSON, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.singlyNestedDataTypesRepeatedRows).withRowSchema(schema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), schema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<String> actual = readPipeline.apply(TextIO.read().from(prefixTo + "*"));

    PayloadSerializer payloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(schema, ImmutableMap.of());

    PAssert.that(actual)
        .containsInAnyOrder(
            DATA.singlyNestedDataTypesRepeatedRows.stream()
                .map(
                    (Row row) ->
                        new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8))
                .collect(Collectors.toList()));

    readPipeline.run();
  }

  @Test
  public void doublyNestedDataTypesNoRepeat() {
    Schema schema = DOUBLY_NESTED_DATA_TYPES_SCHEMA;
    String folder = "doublyNestedDataTypesNoRepeat";

    String to = String.format("%s_%s", JSON, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.doublyNestedDataTypesNoRepeatRows).withRowSchema(schema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), schema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<String> actual = readPipeline.apply(TextIO.read().from(prefixTo + "*"));

    PayloadSerializer payloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(schema, ImmutableMap.of());

    PAssert.that(actual)
        .containsInAnyOrder(
            DATA.doublyNestedDataTypesNoRepeatRows.stream()
                .map(
                    (Row row) ->
                        new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8))
                .collect(Collectors.toList()));

    readPipeline.run();
  }

  @Test
  public void doublyNestedDataTypesRepeat() {
    Schema schema = DOUBLY_NESTED_DATA_TYPES_SCHEMA;
    String folder = "doublyNestedDataTypesRepeat";

    String to = String.format("%s_%s", JSON, folder);
    String prefixTo = prefix(tmpFolder, to);
    PCollection<Row> input =
        writePipeline.apply(Create.of(DATA.doublyNestedDataTypesRepeatRows).withRowSchema(schema));

    PCollection<String> files =
        input.apply(PROVIDER.buildTransform(configuration(prefixTo), schema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });

    writePipeline.run().waitUntilFinish();

    PCollection<String> actual = readPipeline.apply(TextIO.read().from(prefixTo + "*"));

    PayloadSerializer payloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(schema, ImmutableMap.of());

    PAssert.that(actual)
        .containsInAnyOrder(
            DATA.doublyNestedDataTypesRepeatRows.stream()
                .map(
                    (Row row) ->
                        new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8))
                .collect(Collectors.toList()));

    readPipeline.run();
  }

  private static FileWriteSchemaTransformConfiguration configuration(String to) {
    return FileWriteSchemaTransformConfiguration.builder()
        .setFormat(JSON)
        .setFilenamePrefix(to)
        .build();
  }
}
