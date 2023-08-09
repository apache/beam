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
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TIME_CONTAINING_SCHEMA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviderTestData.DATA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.AVRO;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.JSON;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.PARQUET;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.XML;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformProvider.INPUT_TAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformProvider.FileWriteSchemaTransform;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FileWriteSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class FileWriteSchemaTransformProviderTest {
  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();
  private static final TypeDescriptor<FileWriteSchemaTransformConfiguration> TYPE_DESCRIPTOR =
      TypeDescriptor.of(FileWriteSchemaTransformConfiguration.class);
  private static final SerializableFunction<FileWriteSchemaTransformConfiguration, Row> TO_ROW_FN =
      AUTO_VALUE_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);
  private static final FileWriteSchemaTransformProvider PROVIDER =
      new FileWriteSchemaTransformProvider();

  @Rule
  public TestPipeline errorPipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void receivedUnexpectedInputTagsThrowsAnError() {
    SchemaTransform transform =
        PROVIDER.from(rowConfiguration(defaultConfiguration().setFormat(JSON).build()));
    PCollectionRowTuple empty = PCollectionRowTuple.empty(errorPipeline);
    IllegalArgumentException emptyInputError =
        assertThrows(IllegalArgumentException.class, () -> empty.apply(transform));

    assertEquals(
        "org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformProvider$FileWriteSchemaTransform expects a single input tagged PCollection<Row> input",
        emptyInputError.getMessage());

    PCollection<Row> rows1 =
        errorPipeline.apply(
            Create.of(DATA.allPrimitiveDataTypesRows)
                .withRowSchema(ALL_PRIMITIVE_DATA_TYPES_SCHEMA));
    PCollection<Row> rows2 =
        errorPipeline.apply(
            Create.of(DATA.timeContainingRows).withRowSchema(TIME_CONTAINING_SCHEMA));

    PCollectionRowTuple tooManyTags =
        PCollectionRowTuple.of(INPUT_TAG, rows1).and("another", rows2);
    IllegalArgumentException tooManyTagsError =
        assertThrows(IllegalArgumentException.class, () -> tooManyTags.apply(transform));

    assertEquals(
        "org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformProvider$FileWriteSchemaTransform expects a single input tagged PCollection<Row> input",
        tooManyTagsError.getMessage());

    // should not throw an error
    PCollectionRowTuple input = PCollectionRowTuple.of(INPUT_TAG, rows1);
    input.apply(transform);
  }

  @Test
  public void formatMapsToFileWriteSchemaFormatTransform() {
    Row avro = rowConfiguration(defaultConfiguration().setFormat(AVRO).build());
    FileWriteSchemaTransformFormatProvider avroFormatProvider =
        ((FileWriteSchemaTransform) PROVIDER.from(avro)).getProvider();
    assertTrue(avroFormatProvider instanceof AvroWriteSchemaTransformFormatProvider);

    Row json = rowConfiguration(defaultConfiguration().setFormat(JSON).build());
    FileWriteSchemaTransformFormatProvider jsonFormatProvider =
        ((FileWriteSchemaTransform) PROVIDER.from(json)).getProvider();
    assertTrue(jsonFormatProvider instanceof JsonWriteSchemaTransformFormatProvider);

    Row parquet = rowConfiguration(defaultConfiguration().setFormat(PARQUET).build());
    FileWriteSchemaTransformFormatProvider parquetFormatProvider =
        ((FileWriteSchemaTransform) PROVIDER.from(parquet)).getProvider();
    assertTrue(parquetFormatProvider instanceof ParquetWriteSchemaTransformFormatProvider);

    Row xml = rowConfiguration(defaultConfiguration().setFormat(XML).build());
    FileWriteSchemaTransformFormatProvider xmlFormatProvider =
        ((FileWriteSchemaTransform) PROVIDER.from(xml)).getProvider();
    assertTrue(xmlFormatProvider instanceof XmlWriteSchemaTransformFormatProvider);
  }

  private static Row rowConfiguration(FileWriteSchemaTransformConfiguration configuration) {
    return TO_ROW_FN.apply(configuration);
  }

  private static FileWriteSchemaTransformConfiguration.Builder defaultConfiguration() {
    return FileWriteSchemaTransformConfiguration.builder()
        .setFilenamePrefix(FileWriteSchemaTransformProviderTest.class.getSimpleName());
  }
}
