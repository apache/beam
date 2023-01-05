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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

abstract class FileWriteSchemaTransformFormatProviderTest {

  abstract String getFormat();

  abstract String getFilenamePrefix();

  protected abstract void assertFolderContainsInAnyOrder(
      String folder, List<Row> rows, Schema beamSchema);

  abstract FileWriteSchemaTransformConfiguration buildConfiguration(String folder);

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void allPrimitiveDataTypes() {
    String to = folder(SchemaAwareJavaBeans.AllPrimitiveDataTypes.class);
    Schema schema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.allPrimitiveDataTypesRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    assertFolderContainsInAnyOrder(to, rows, schema);
  }

  @Test
  public void nullableAllPrimitiveDataTypes() {
    String to = folder(SchemaAwareJavaBeans.NullableAllPrimitiveDataTypes.class);
    Schema schema = NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.nullableAllPrimitiveDataTypesRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    assertFolderContainsInAnyOrder(to, rows, schema);
  }

  @Test
  public void timeContaining() {
    String to = folder(SchemaAwareJavaBeans.TimeContaining.class);
    Schema schema = TIME_CONTAINING_SCHEMA;
    List<Row> rows = DATA.timeContainingRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    assertFolderContainsInAnyOrder(to, rows, schema);
  }

  @Test
  public void arrayPrimitiveDataTypes() {
    String to = folder(SchemaAwareJavaBeans.ArrayPrimitiveDataTypes.class);
    Schema schema = ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.arrayPrimitiveDataTypesRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    assertFolderContainsInAnyOrder(to, rows, schema);
  }

  @Test
  public void singlyNestedDataTypesNoRepeat() {
    String to = folder(SchemaAwareJavaBeans.SinglyNestedDataTypes.class, "no_repeat");
    Schema schema = SINGLY_NESTED_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.singlyNestedDataTypesNoRepeatRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    assertFolderContainsInAnyOrder(to, rows, schema);
  }

  @Test
  public void singlyNestedDataTypesRepeated() {
    String to = folder(SchemaAwareJavaBeans.SinglyNestedDataTypes.class, "repeated");
    Schema schema = SINGLY_NESTED_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.singlyNestedDataTypesNoRepeatRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    assertFolderContainsInAnyOrder(to, rows, schema);
  }

  @Test
  public void doublyNestedDataTypesNoRepeat() {
    String to = folder(SchemaAwareJavaBeans.DoublyNestedDataTypes.class, "no_repeat");
    Schema schema = DOUBLY_NESTED_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.doublyNestedDataTypesNoRepeatRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    assertFolderContainsInAnyOrder(to, rows, schema);
  }

  @Test
  public void doublyNestedDataTypesRepeat() {
    String to = folder(SchemaAwareJavaBeans.DoublyNestedDataTypes.class, "repeated");
    Schema schema = DOUBLY_NESTED_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.doublyNestedDataTypesRepeatRows;
    applyProviderAndAssertFilesWritten(to, rows, schema);
    assertFolderContainsInAnyOrder(to, rows, schema);
  }

  private FileWriteSchemaTransformFormatProvider getProvider() {
    return FileWriteSchemaTransformFormatProviders.loadProviders().get(getFormat());
  }

  private <T> String folder(Class<T> clazz, String additionalPath) {
    return folder(getFormat(), clazz.getSimpleName(), additionalPath);
  }

  private <T> String folder(Class<T> clazz) {
    return folder(getFormat(), clazz.getSimpleName());
  }

  private String folder(String... paths) {
    try {
      return tmpFolder.newFolder(paths).getAbsolutePath();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private void applyProviderAndAssertFilesWritten(String folder, List<Row> rows, Schema schema) {
    PCollection<Row> input = writePipeline.apply(Create.of(rows).withRowSchema(schema));
    PCollection<String> files =
        input.apply(
            getProvider()
                .buildTransform(buildConfiguration(folder + "/" + getFilenamePrefix()), schema));
    PAssert.that(files)
        .satisfies(
            (Iterable<String> names) -> {
              assertNotNull(names);
              assertTrue(names.iterator().hasNext());
              return null;
            });
    writePipeline.run().waitUntilFinish();
  }

  protected FileWriteSchemaTransformConfiguration defaultConfiguration(String folder) {
    return FileWriteSchemaTransformConfiguration.builder()
        .setFormat(getFormat())
        .setFilenamePrefix(folder)
        .build();
  }
}
