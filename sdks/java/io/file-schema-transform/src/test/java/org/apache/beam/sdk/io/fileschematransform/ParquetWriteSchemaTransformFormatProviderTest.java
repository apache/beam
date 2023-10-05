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

import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration.parquetConfigurationBuilder;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.PARQUET;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.BeamRowMapperWithDlq;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.Schema;
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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ParquetWriteSchemaTransformFormatProvider}. */
@RunWith(JUnit4.class)
public class ParquetWriteSchemaTransformFormatProviderTest
    extends FileWriteSchemaTransformFormatProviderTest {
  @Override
  protected String getFormat() {
    return PARQUET;
  }

  @Override
  protected String getFilenamePrefix() {
    return "";
  }

  @Override
  protected void assertFolderContainsInAnyOrder(String folder, List<Row> rows, Schema beamSchema) {
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);

    List<GenericRecord> expected =
        rows.stream()
            .map(AvroUtils.getRowToGenericRecordFunction(avroSchema)::apply)
            .collect(Collectors.toList());

    PCollection<GenericRecord> actual =
        readPipeline.apply(
            ParquetIO.read(avroSchema)
                .from(folder + "/" + getFilenamePrefix() + "*")
                .withProjection(avroSchema, avroSchema));

    PAssert.that(actual).containsInAnyOrder(expected);
  }

  @Override
  protected FileWriteSchemaTransformConfiguration buildConfiguration(String folder) {
    return FileWriteSchemaTransformConfiguration.builder()
        .setParquetConfiguration(
            parquetConfigurationBuilder()
                .setCompressionCodecName(CompressionCodecName.GZIP.name())
                .build())
        .setFormat(getFormat())
        .setFilenamePrefix(folder + getFilenamePrefix())
        .build();
  }

  @Override
  protected Optional<String> expectedErrorWhenCompressionSet() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> expectedErrorWhenParquetConfigurationSet() {
    return Optional.empty();
  }

  @Override
  protected Optional<String> expectedErrorWhenXmlConfigurationSet() {
    return Optional.of(
        "configuration with org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration$XmlConfiguration is not compatible with a parquet format");
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
        "configuration with org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration$CsvConfiguration is not compatible with a parquet format");
  }

  @Test
  public void testParquetErrorCounterSuccess() {
    SerializableFunction<Row, GenericRecord> mapFn =
        AvroUtils.getRowToGenericRecordFunction(AvroUtils.toAvroSchema(BEAM_SCHEMA));

    List<GenericRecord> records =
        Arrays.asList(
            new GenericRecordBuilder(AVRO_SCHEMA).set("name", "a").build(),
            new GenericRecordBuilder(AVRO_SCHEMA).set("name", "b").build(),
            new GenericRecordBuilder(AVRO_SCHEMA).set("name", "c").build());

    PCollection<Row> input = writePipeline.apply(Create.of(ROWS));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new BeamRowMapperWithDlq<GenericRecord>(
                        "Avro-write-error-counter", mapFn, OUTPUT_TAG))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));
    output.get(OUTPUT_TAG).setCoder(CODER);
    output.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA);
    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(records);
    writePipeline.run().waitUntilFinish();
  }

  @Test
  public void testParquetErrorCounterFailure() {
    SerializableFunction<Row, GenericRecord> mapFn =
        AvroUtils.getRowToGenericRecordFunction(AvroUtils.toAvroSchema(BEAM_SCHEMA_DLQ));

    PCollection<Row> input = writePipeline.apply(Create.of(ROWS));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new BeamRowMapperWithDlq<GenericRecord>(
                        "Avro-write-error-counter", mapFn, OUTPUT_TAG))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));
    output.get(OUTPUT_TAG).setCoder(CODER);
    output.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA);
    PCollection<Long> count = output.get(ERROR_TAG).apply(Count.globally());
    PAssert.that(count).containsInAnyOrder(Collections.singletonList(3L));
    writePipeline.run().waitUntilFinish();
  }

  private static final TupleTag<GenericRecord> OUTPUT_TAG =
      ParquetWriteSchemaTransformFormatProvider.ERROR_FN_OUPUT_TAG;
  private static final TupleTag<Row> ERROR_TAG = FileWriteSchemaTransformProvider.ERROR_TAG;

  private static final Schema BEAM_SCHEMA =
      Schema.of(Schema.Field.of("name", Schema.FieldType.STRING));
  private static final Schema BEAM_SCHEMA_DLQ =
      Schema.of(Schema.Field.of("error", Schema.FieldType.INT32));
  private static final Schema ERROR_SCHEMA = FileWriteSchemaTransformProvider.ERROR_SCHEMA;
  private static final org.apache.avro.Schema AVRO_SCHEMA = AvroUtils.toAvroSchema(BEAM_SCHEMA);

  private static final AvroCoder<GenericRecord> CODER = AvroCoder.of(AVRO_SCHEMA);

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "a").build(),
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "b").build(),
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "c").build());
}
