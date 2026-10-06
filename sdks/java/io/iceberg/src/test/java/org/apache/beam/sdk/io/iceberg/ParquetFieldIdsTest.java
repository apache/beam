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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.ID;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.NAME;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.column;
import static org.apache.beam.sdk.io.iceberg.ParquetTestFiles.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Which table column each Parquet column is. Readers resolve a file by the field ids it carries,
 * and by the table's name mapping only when it carries none, so each test also reads the file back
 * the way readers do to show why it is accepted or refused.
 */
@RunWith(JUnit4.class)
public class ParquetFieldIdsTest {
  @Rule public TemporaryFolder temp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final Schema ID_NAME =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()));

  private static final PrimitiveType EMAIL =
      column("email", PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());

  private HadoopCatalog catalog;
  private ParquetTestFiles files;

  @Before
  public void setup() throws IOException {
    catalog = new HadoopCatalog(new Configuration(), temp.newFolder("warehouse").getAbsolutePath());
    files = new ParquetTestFiles(temp.newFolder("files"));
  }

  /** Iceberg assigns fresh ids when it creates a table; tests read them back from the result. */
  private Table table(Schema schema) {
    return catalog.createTable(
        TableIdentifier.of("default", testName.getMethodName()),
        schema,
        PartitionSpec.unpartitioned());
  }

  private static ParquetFieldIds.Resolved resolve(String file, Table table) throws IOException {
    return ParquetFieldIds.resolve(
        ParquetFooters.read(file), table, MappingUtil.create(table.schema()));
  }

  private static String refusal(String file, Table table) {
    ParquetFieldIds.ConflictException e =
        assertThrows(ParquetFieldIds.ConflictException.class, () -> resolve(file, table));
    return String.valueOf(e.getMessage());
  }

  /** The field id the resolved footer gives a top-level column, or null for none. */
  private static @Nullable Integer idOf(ParquetFieldIds.Resolved resolved, String column) {
    org.apache.parquet.schema.Type.ID id =
        resolved.footer().getFileMetaData().getSchema().getType(column).getId();
    return id == null ? null : id.intValue();
  }

  /** The file's rows as readers return them: by the file's ids, or by {@code mapping} without. */
  private static List<String> read(String file, Schema schema, NameMapping mapping)
      throws IOException {
    List<String> rows = new ArrayList<>();
    try (CloseableIterable<Record> records =
        Parquet.read(org.apache.iceberg.Files.localInput(file))
            .project(schema)
            .withNameMapping(mapping)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      for (Record record : records) {
        rows.add(record.toString());
      }
    }
    return rows;
  }

  private static List<String> read(String file, Schema schema) throws IOException {
    return read(file, schema, MappingUtil.create(schema));
  }

  // ---- files without field ids

  @Test
  public void testFileWithoutIdsTakesTheIdsTheMappingGivesItsNames() throws IOException {
    String file =
        files.write("plain.parquet", true, Arrays.asList(ID, NAME, EMAIL), row(1, "a", "a@x.com"));

    ParquetFieldIds.Resolved resolved = ParquetFieldIds.resolve(ParquetFooters.read(file), ID_NAME);

    assertEquals(Integer.valueOf(1), idOf(resolved, "id"));
    assertEquals(Integer.valueOf(2), idOf(resolved, "name"));
    assertNull(idOf(resolved, "email"));
  }

  /**
   * The table's stored mapping keeps a renamed column's old name, and readers use it; a mapping
   * created from the current schema alone would leave the old column without an id.
   */
  @Test
  public void testFileWithoutIdsWrittenBeforeARenameTakesTheRenamedColumnsId() throws IOException {
    Table table = table(ID_NAME);
    table
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING,
            NameMappingParser.toJson(MappingUtil.create(table.schema())))
        .commit();
    table.updateSchema().renameColumn("name", "full_name").commit();
    String file = files.write("older.parquet", true, Arrays.asList(ID, NAME), row(1, "a"));
    ParquetMetadata footer = ParquetFooters.read(file);

    NameMapping forReaders =
        NameMappingUtils.forReaders(
            table.schema(),
            NameMappingUtils.parseOrNull(
                table.properties().get(TableProperties.DEFAULT_NAME_MAPPING)));
    assertEquals(
        Integer.valueOf(2), idOf(ParquetFieldIds.resolve(footer, table, forReaders), "name"));
    assertNull(
        idOf(ParquetFieldIds.resolve(footer, table, MappingUtil.create(table.schema())), "name"));
    assertEquals(Arrays.asList("Record(1, a)"), read(file, table.schema(), forReaders));
  }

  // ---- files with field ids

  /**
   * Ids that agree with the table are what Iceberg's own writers produce for it; an id the table
   * has never used (email's 50) is one readers ignore.
   */
  @Test
  public void testFileWithTheTablesFieldIdsKeepsThem() throws IOException {
    Table table = table(ID_NAME);
    String file =
        files.write(
            "own.parquet",
            true,
            Arrays.asList(ID.withId(1), NAME.withId(2), EMAIL.withId(50)),
            row(1, "a", "a@x.com"));
    ParquetMetadata footer = ParquetFooters.read(file);

    assertSame(
        footer,
        ParquetFieldIds.resolve(footer, table, MappingUtil.create(table.schema())).footer());
    assertEquals(Arrays.asList("Record(1, a)"), read(file, table.schema()));
  }

  /** Readers use a file's own ids, so no name mapping can correct swapped ones. */
  @Test
  public void testSwappedFieldIdsAreRefused() throws IOException {
    Table table = table(ID_NAME);
    String file =
        files.write(
            "swapped.parquet", true, Arrays.asList(NAME.withId(1), ID.withId(2)), row("a", 1));

    assertEquals(
        "column name carries field id 1, which the table uses for column id", refusal(file, table));
  }

  /** Readers find no column with the table's id for name in the file, so they read it as null. */
  @Test
  public void testTableColumnUnderAnotherFieldIdIsRefused() throws IOException {
    Table table = table(ID_NAME);
    String file =
        files.write(
            "renumbered.parquet", true, Arrays.asList(ID.withId(1), NAME.withId(9)), row(1, "a"));

    assertEquals(
        "column name carries field id 9, but the table's column name has field id 2",
        refusal(file, table));
    assertEquals(Arrays.asList("Record(1, null)"), read(file, table.schema()));
  }

  /** A rename keeps the column's id, and an earlier schema version still has the old name. */
  @Test
  public void testFileWrittenBeforeARenameKeepsItsIds() throws IOException {
    Table table = table(ID_NAME);
    table.updateSchema().renameColumn("name", "full_name").commit();
    String file =
        files.write(
            "older.parquet", true, Arrays.asList(ID.withId(1), NAME.withId(2)), row(1, "a"));

    resolve(file, table);

    assertEquals(Arrays.asList("Record(1, a)"), read(file, table.schema()));
  }

  /** Iceberg looks metrics modes up by the file's name, which a rename does not change. */
  @Test
  public void testColumnNameIsTheFilesNameAfterARename() throws IOException {
    Table table = table(ID_NAME);
    table.updateSchema().renameColumn("name", "full_name").commit();
    String file =
        files.write(
            "older.parquet", true, Arrays.asList(ID.withId(1), NAME.withId(2)), row(1, "a"));

    ParquetFieldIds.Resolved resolved = resolve(file, table);

    assertEquals(Arrays.asList("name"), resolved.columnNames(Arrays.asList(2, 3)));
  }

  /** A name the mapping gives an id counts as that column's, like a name from a schema version. */
  @Test
  public void testNameTheMappingGivesTheIdKeepsIt() throws IOException {
    Table table = table(ID_NAME);
    NameMapping aliased =
        NameMapping.of(
            MappedField.of(1, "id"), MappedField.of(2, Arrays.asList("name", "customer")));
    PrimitiveType customer =
        column("customer", PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());
    String file =
        files.write(
            "alias.parquet", true, Arrays.asList(ID.withId(1), customer.withId(2)), row(1, "a"));
    ParquetMetadata footer = ParquetFooters.read(file);

    ParquetFieldIds.resolve(footer, table, aliased);

    assertEquals(
        "column customer carries field id 2, which the table uses for column name",
        refusal(file, table));
  }

  /**
   * The file's shipping.city carries the id of the table's billing.city. Readers find neither
   * column in it, yet trusting the id would store its values as billing.city's statistics.
   */
  @Test
  public void testNestedFieldCarryingTheIdOfAnotherStructsFieldIsRefused() throws IOException {
    Table table =
        table(
            new Schema(
                Types.NestedField.optional(
                    1,
                    "billing",
                    Types.StructType.of(
                        Types.NestedField.optional(2, "city", Types.StringType.get()))),
                Types.NestedField.optional(
                    3,
                    "shipping",
                    Types.StructType.of(
                        Types.NestedField.optional(4, "city", Types.StringType.get())))));
    Schema schema = table.schema();
    int billingCity = schema.findField("billing.city").fieldId();
    Types.StructType shipping =
        Types.StructType.of(
            Types.NestedField.optional(billingCity, "city", Types.StringType.get()));
    Schema fileSchema =
        new Schema(
            Types.NestedField.optional(
                schema.findField("shipping").fieldId(), "shipping", shipping));
    Record city = GenericRecord.create(shipping);
    city.setField("city", "Paris");
    Record record = GenericRecord.create(fileSchema);
    record.setField("shipping", city);
    String file = files.writeWithIds("moved.parquet", fileSchema, record);

    assertEquals(
        "column shipping.city carries field id "
            + billingCity
            + ", which the table uses for column billing.city",
        refusal(file, table));
    assertEquals(Arrays.asList("Record(null, null)"), read(file, schema));
  }

  /**
   * Struct fields, list elements and map entries are compared by full path, as Iceberg names them.
   */
  @Test
  public void testNestedFileWithTheTablesFieldIdsKeepsThem() throws IOException {
    Table table =
        table(
            new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(
                    2,
                    "address",
                    Types.StructType.of(
                        Types.NestedField.optional(3, "city", Types.StringType.get()))),
                Types.NestedField.optional(
                    4, "tags", Types.ListType.ofOptional(5, Types.StringType.get())),
                Types.NestedField.optional(
                    6,
                    "attrs",
                    Types.MapType.ofOptional(
                        7, 8, Types.StringType.get(), Types.StringType.get()))));
    String file =
        files.writeWithIds("nested.parquet", table.schema(), nestedRecord(table.schema()));

    resolve(file, table);

    assertEquals(Arrays.asList("Record(1, Record(Paris), [a], {k=v})"), read(file, table.schema()));
  }

  /**
   * A file written before its struct was renamed matches the schema version it was written with.
   */
  @Test
  public void testFileWrittenBeforeAStructRenameKeepsItsIds() throws IOException {
    Table table =
        table(
            new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(
                    2,
                    "address",
                    Types.StructType.of(
                        Types.NestedField.optional(3, "city", Types.StringType.get())))));
    Schema before = table.schema();
    table.updateSchema().renameColumn("address", "location").commit();
    String file = files.writeWithIds("older.parquet", before, nestedRecord(before));

    resolve(file, table);

    assertEquals(Arrays.asList("Record(1, Record(Paris))"), read(file, table.schema()));
  }

  @Test
  public void testDuplicateFieldIdsAreRefused() throws IOException {
    Table table = table(ID_NAME);
    String file =
        files.write(
            "duplicate.parquet", true, Arrays.asList(ID.withId(1), NAME.withId(1)), row(1, "a"));

    String message = refusal(file, table);

    assertTrue(message, message.startsWith("its field ids cannot be resolved"));
  }

  /** Readers resolve by ids once any column has one, so a column without an id reads as null. */
  @Test
  public void testFileWithIdsOnSomeColumnsIsRefused() throws IOException {
    Table table = table(ID_NAME);
    String file =
        files.write("partial.parquet", true, Arrays.asList(ID.withId(1), NAME), row(1, "a"));

    String message = refusal(file, table);

    assertTrue(message, message.startsWith("column name "));
    assertEquals(Arrays.asList("Record(1, null)"), read(file, table.schema()));
  }

  /** Fills whichever of id, address.city, tags and attrs {@code schema} has. */
  private static Record nestedRecord(Schema schema) {
    Record record = GenericRecord.create(schema);
    record.setField("id", 1);
    Record address = GenericRecord.create(schema.findType("address").asStructType());
    address.setField("city", "Paris");
    record.setField("address", address);
    if (schema.findField("tags") != null) {
      record.setField("tags", Arrays.asList("a"));
      record.setField("attrs", ImmutableMap.of("k", "v"));
    }
    return record;
  }
}
