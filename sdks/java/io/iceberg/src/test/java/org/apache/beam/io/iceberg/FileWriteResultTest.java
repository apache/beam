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
package org.apache.beam.io.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileWriteResultTest implements Serializable {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  private static final Coder<FileWriteResult> TEST_CODER =
      FileWriteResult.FileWriteResultCoder.of();

  private List<FileWriteResult> getTestValues() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));

    // Create a table so we can have some DataFile objects
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    List<FileWriteResult> values = Lists.newArrayList();

    // A parquet file
    RecordWriter writer =
        new RecordWriter(table, FileFormat.PARQUET, TEMPORARY_FOLDER.newFile().toString());
    writer.write(
        Row.withSchema(SchemaAndRowConversions.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
            .addValues(42L, "bizzle")
            .build());
    writer.close();
    DataFile dataFile = writer.dataFile();
    values.add(
        FileWriteResult.builder()
            .setDataFile(dataFile)
            .setPartitionSpec(table.spec())
            .setTableIdentifier(tableId)
            .build());

    // An avro file
    writer = new RecordWriter(table, FileFormat.AVRO, TEMPORARY_FOLDER.newFile().toString());
    writer.write(
        Row.withSchema(SchemaAndRowConversions.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
            .addValues(42L, "bizzle")
            .build());
    writer.close();
    dataFile = writer.dataFile();
    values.add(
        FileWriteResult.builder()
            .setDataFile(dataFile)
            .setPartitionSpec(table.spec())
            .setTableIdentifier(tableId)
            .build());

    // Parquet file with a different schema
    TableIdentifier tableId2 =
        TableIdentifier.of(
            "default", "othertable" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(
                3,
                "extra",
                Types.StructType.of(
                    Types.NestedField.required(4, "inner", Types.BinaryType.get()))));
    Table table2 = warehouse.createTable(tableId2, schema);

    // A parquet file in this other table
    writer = new RecordWriter(table2, FileFormat.PARQUET, TEMPORARY_FOLDER.newFile().toString());
    writer.write(
        Row.withSchema(SchemaAndRowConversions.icebergSchemaToBeamSchema(schema))
            .addValues(
                42L,
                "bizzle",
                Row.withSchema(
                        org.apache.beam.sdk.schemas.Schema.of(
                            org.apache.beam.sdk.schemas.Schema.Field.of(
                                "inner", org.apache.beam.sdk.schemas.Schema.FieldType.BYTES)))
                    .addValues(new byte[] {0xa})
                    .build())
            .build());
    writer.close();
    DataFile dataFile2 = writer.dataFile();
    values.add(
        FileWriteResult.builder()
            .setDataFile(dataFile2)
            .setPartitionSpec(table2.spec())
            .setTableIdentifier(tableId2)
            .build());

    return values;
  }

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (FileWriteResult value : getTestValues()) {
      CoderProperties.structuralValueDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    assertThat(
        TEST_CODER.getEncodedTypeDescriptor(), equalTo(TypeDescriptor.of(FileWriteResult.class)));
  }
}
