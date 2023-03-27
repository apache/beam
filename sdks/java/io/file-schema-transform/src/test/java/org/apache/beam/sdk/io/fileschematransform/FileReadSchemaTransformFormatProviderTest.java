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
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.BYTE_SEQUENCE_TYPE_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.BYTE_TYPE_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.SINGLY_NESTED_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TIME_CONTAINING_SCHEMA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviderTestData.DATA;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public abstract class FileReadSchemaTransformFormatProviderTest {

  /** Returns the format of the {@linke FileReadSchemaTransformFormatProviderTest} subclass. */
  protected abstract String getFormat();

  /**
   * Writes {@link Row}s to files then reads from those files. Performs a {@link
   * org.apache.beam.sdk.testing.PAssert} check to validate the written and read {@link Row}s are
   * equal.
   */
  protected abstract void runWriteAndReadTest(Schema schema, List<Row> rows, String filePath);

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public TestName testName = new TestName();

  protected String getFilePath() {
    return getFolder() + "/test";
  }

  protected String getFolder() {
    try {
      return tmpFolder.newFolder(getFormat(), testName.getMethodName()).getAbsolutePath();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  public void testAllPrimitiveDataTypes() {
    Schema schema = ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.allPrimitiveDataTypesRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath);
  }

  @Test
  public void testNullableAllPrimitiveDataTypes() {
    Schema schema = NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.nullableAllPrimitiveDataTypesRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath);
  }

  @Test
  public void testTimeContaining() {
    Schema schema = TIME_CONTAINING_SCHEMA;
    List<Row> rows = DATA.timeContainingRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath);
  }

  @Test
  public void testByteType() {
    List<String> formatsThatSupportSingleByteType = Arrays.asList("csv", "json", "xml");
    assumeTrue(formatsThatSupportSingleByteType.contains(getFormat()));

    Schema schema = BYTE_TYPE_SCHEMA;
    List<Row> rows = DATA.byteTypeRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath);
  }

  @Test
  public void testByteSequenceType() {
    List<String> formatsThatSupportByteSequenceType = Arrays.asList("avro", "parquet");
    assumeTrue(formatsThatSupportByteSequenceType.contains(getFormat()));

    Schema schema = BYTE_SEQUENCE_TYPE_SCHEMA;
    List<Row> rows = DATA.byteSequenceTypeRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath);
  }

  @Test
  public void testArrayPrimitiveDataTypes() {
    Schema schema = ARRAY_PRIMITIVE_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.arrayPrimitiveDataTypesRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath);
  }

  @Test
  public void testNestedRepeatedDataTypes() {
    Schema schema = SINGLY_NESTED_DATA_TYPES_SCHEMA;
    List<Row> rows = DATA.singlyNestedDataTypesRepeatedRows;
    String filePath = getFilePath();

    runWriteAndReadTest(schema, rows, filePath);
  }
}
