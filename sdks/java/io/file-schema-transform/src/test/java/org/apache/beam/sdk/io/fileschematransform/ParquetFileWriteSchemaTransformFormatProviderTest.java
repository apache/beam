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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ParquetWriteSchemaTransformFormatProvider}. */
@RunWith(JUnit4.class)
public class ParquetFileWriteSchemaTransformFormatProviderTest
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
}
