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
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FileWriteSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class FileWriteSchemaTransformProviderTest {
  private static final FileWriteSchemaTransformProvider PROVIDER =
      new FileWriteSchemaTransformProvider();

  @Rule
  public TestPipeline errorPipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void receivedUnexpectedInputTagsThrowsAnError() {
    SchemaTransform transform = PROVIDER.from(defaultConfiguration().setFormat(JSON).build());
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
    FileWriteSchemaTransformFormatProvider avroFormatProvider =
        ((FileWriteSchemaTransform) PROVIDER.from(defaultConfiguration().setFormat(AVRO).build()))
            .getProvider();
    assertTrue(avroFormatProvider instanceof AvroWriteSchemaTransformFormatProvider);

    FileWriteSchemaTransformFormatProvider jsonFormatProvider =
        ((FileWriteSchemaTransform) PROVIDER.from(defaultConfiguration().setFormat(JSON).build()))
            .getProvider();
    assertTrue(jsonFormatProvider instanceof JsonWriteSchemaTransformFormatProvider);

    FileWriteSchemaTransformFormatProvider parquetFormatProvider =
        ((FileWriteSchemaTransform)
                PROVIDER.from(defaultConfiguration().setFormat(PARQUET).build()))
            .getProvider();
    assertTrue(parquetFormatProvider instanceof ParquetWriteSchemaTransformFormatProvider);

    FileWriteSchemaTransformFormatProvider xmlFormatProvider =
        ((FileWriteSchemaTransform) PROVIDER.from(defaultConfiguration().setFormat(XML).build()))
            .getProvider();
    assertTrue(xmlFormatProvider instanceof XmlWriteSchemaTransformFormatProvider);
  }

  private static FileWriteSchemaTransformConfiguration.Builder defaultConfiguration() {
    return FileWriteSchemaTransformConfiguration.builder()
        .setFilenamePrefix(FileWriteSchemaTransformProviderTest.class.getSimpleName());
  }
}
