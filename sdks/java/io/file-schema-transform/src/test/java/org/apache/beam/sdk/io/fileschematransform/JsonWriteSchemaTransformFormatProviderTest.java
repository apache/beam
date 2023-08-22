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

import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.JSON;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.BeamRowMapperWithDlq;
import org.apache.beam.sdk.io.fileschematransform.JsonWriteSchemaTransformFormatProvider.RowToJsonFn;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.JsonPayloadSerializerProvider;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link JsonWriteSchemaTransformFormatProvider}. */
@RunWith(JUnit4.class)
public class JsonWriteSchemaTransformFormatProviderTest
    extends FileWriteSchemaTransformFormatProviderTest {
  @Override
  protected String getFormat() {
    return JSON;
  }

  @Override
  protected String getFilenamePrefix() {
    return "/out";
  }

  @Override
  protected void assertFolderContainsInAnyOrder(String folder, List<Row> rows, Schema beamSchema) {
    PCollection<String> actual = readPipeline.apply(TextIO.read().from(folder + "*"));

    PayloadSerializer payloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(beamSchema, ImmutableMap.of());

    PAssert.that(actual)
        .containsInAnyOrder(
            rows.stream()
                .map(
                    (Row row) ->
                        new String(payloadSerializer.serialize(row), StandardCharsets.UTF_8))
                .collect(Collectors.toList()));
  }

  @Override
  protected FileWriteSchemaTransformConfiguration buildConfiguration(String folder) {
    return defaultConfiguration(folder);
  }

  @Override
  protected Optional<String> expectedErrorWhenCompressionSet() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> expectedErrorWhenParquetConfigurationSet() {
    return Optional.of(
        "configuration with org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration$ParquetConfiguration is not compatible with a json format");
  }

  @Override
  protected Optional<String> expectedErrorWhenXmlConfigurationSet() {
    return Optional.of(
        "configuration with org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration$XmlConfiguration is not compatible with a json format");
  }

  @Override
  protected Optional<String> expectedErrorWhenNumShardsSet() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> expectedErrorWhenShardNameTemplateSet() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> expectedErrorWhenCsvConfigurationSet() {
    return Optional.of(
        "configuration with org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration$CsvConfiguration is not compatible with a json format");
  }

  @Test
  public void testJsonErrorCounterSuccess() {
    SerializableFunction<Row, String> mapFn = new RowToJsonFn(BEAM_SCHEMA);

    PCollection<Row> input = writePipeline.apply(Create.of(ROWS));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new BeamRowMapperWithDlq<String>("Json-write-error-counter", mapFn, OUTPUT_TAG))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));
    output.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA);
    PCollection<Long> count = output.get(OUTPUT_TAG).apply(Count.globally());
    PAssert.that(count).containsInAnyOrder(Collections.singleton(3L));
    writePipeline.run().waitUntilFinish();
  }

  @Test
  public void testJsonErrorCounterFailure() {
    SerializableFunction<Row, String> mapFn = new RowToJsonFn(BEAM_SCHEMA_DLQ);

    PCollection<Row> input = writePipeline.apply(Create.of(ROWS));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new BeamRowMapperWithDlq<String>("Json-write-error-counter", mapFn, OUTPUT_TAG))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));
    output.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA);
    PCollection<Long> count = output.get(ERROR_TAG).apply(Count.globally());
    PAssert.that(count).containsInAnyOrder(Collections.singletonList(3L));
    writePipeline.run().waitUntilFinish();
  }

  private static final TupleTag<String> OUTPUT_TAG =
      JsonWriteSchemaTransformFormatProvider.ERROR_FN_OUPUT_TAG;
  private static final TupleTag<Row> ERROR_TAG = FileWriteSchemaTransformProvider.ERROR_TAG;

  private static final Schema BEAM_SCHEMA =
      Schema.of(Schema.Field.of("name", Schema.FieldType.STRING));
  private static final Schema BEAM_SCHEMA_DLQ =
      Schema.of(Schema.Field.of("error", Schema.FieldType.INT32));
  private static final Schema ERROR_SCHEMA = FileWriteSchemaTransformProvider.ERROR_SCHEMA;

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "a").build(),
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "b").build(),
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "c").build());
}
