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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.IntPredicate;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileSchemasTest {
  @Rule public final TemporaryFolder tmp = new TemporaryFolder();

  // ---- tightening

  // root: required id, optional name, optional struct address {city, zip}, optional list tags
  private static final MessageType MIXED =
      org.apache.parquet.schema.Types.buildMessage()
          .required(PrimitiveTypeName.INT64)
          .named("id")
          .optional(PrimitiveTypeName.BINARY)
          .as(LogicalTypeAnnotation.stringType())
          .named("name")
          .addField(
              org.apache.parquet.schema.Types.buildGroup(Repetition.OPTIONAL)
                  .optional(PrimitiveTypeName.BINARY)
                  .as(LogicalTypeAnnotation.stringType())
                  .named("city")
                  .optional(PrimitiveTypeName.INT32)
                  .named("zip")
                  .named("address"))
          .addField(
              org.apache.parquet.schema.Types.buildGroup(Repetition.OPTIONAL)
                  .as(LogicalTypeAnnotation.listType())
                  .addField(
                      org.apache.parquet.schema.Types.repeatedGroup()
                          .optional(PrimitiveTypeName.BINARY)
                          .as(LogicalTypeAnnotation.stringType())
                          .named("element")
                          .named("list"))
                  .named("tags"))
          .named("root");

  private static final class Nulls {
    final IntPredicate name;
    final IntPredicate address;
    final IntPredicate city;
    final IntPredicate zip;

    Nulls(IntPredicate name, IntPredicate address, IntPredicate city, IntPredicate zip) {
      this.name = name;
      this.address = address;
      this.city = city;
      this.zip = zip;
    }

    static final Nulls NONE = new Nulls(r -> false, r -> false, r -> false, r -> false);
  }

  private ParquetMetadata write(int rows, int rowGroups, boolean stats, Nulls nulls)
      throws IOException {
    File file = new File(tmp.getRoot(), "t" + System.nanoTime() + ".parquet");
    ExampleParquetWriter.Builder builder =
        ExampleParquetWriter.builder(new Path(file.getAbsolutePath()))
            .withType(MIXED)
            .withStatisticsEnabled(stats);
    if (rowGroups > 1) {
      int rowsPerGroup = rows / rowGroups;
      builder =
          builder
              .withRowGroupSize(1L)
              .withMinRowCountForPageSizeCheck(rowsPerGroup)
              .withMaxRowCountForPageSizeCheck(rowsPerGroup);
    }
    SimpleGroupFactory factory = new SimpleGroupFactory(MIXED);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (int row = 0; row < rows; row++) {
        Group group = factory.newGroup();
        group.add("id", (long) row);
        if (!nulls.name.test(row)) {
          group.add("name", "n" + row);
        }
        if (!nulls.address.test(row)) {
          Group address = group.addGroup("address");
          if (!nulls.city.test(row)) {
            address.add("city", "c" + row);
          }
          if (!nulls.zip.test(row)) {
            address.add("zip", row);
          }
        }
        Group tags = group.addGroup("tags");
        tags.addGroup("list").add("element", "t" + row);
        writer.write(group);
      }
    }
    return ParquetFooters.read(file.getAbsolutePath());
  }

  private static Schema tightened(ParquetMetadata footer) {
    return FileSchemas.tighten(
        ParquetSchemaUtil.convert(footer.getFileMetaData().getSchema()), footer);
  }

  private static boolean isRequired(Schema schema, String path) {
    return schema.findField(path).isRequired();
  }

  @Test
  public void testProvenNullFreeColumnsBecomeRequired() throws IOException {
    Schema schema = tightened(write(10, 1, true, Nulls.NONE));
    assertTrue(isRequired(schema, "id"));
    assertTrue(isRequired(schema, "name"));
    assertTrue(isRequired(schema, "address"));
    assertTrue(isRequired(schema, "address.city"));
    assertTrue(isRequired(schema, "address.zip"));
  }

  @Test
  public void testListAndElementStayAsDeclared() throws IOException {
    Schema schema = tightened(write(10, 1, true, Nulls.NONE));
    assertFalse(isRequired(schema, "tags"));
    assertFalse(schema.findField("tags").type().asListType().isElementRequired());
  }

  @Test
  public void testSomeNullsStayOptional() throws IOException {
    Schema schema =
        tightened(write(10, 1, true, new Nulls(r -> r == 3, r -> false, r -> false, r -> false)));
    assertFalse(isRequired(schema, "name"));
    assertTrue(isRequired(schema, "address.city"));
  }

  @Test
  public void testAllNullsStayOptional() throws IOException {
    Schema schema =
        tightened(write(10, 1, true, new Nulls(r -> true, r -> false, r -> false, r -> false)));
    assertFalse(isRequired(schema, "name"));
  }

  @Test
  public void testStatsDisabledStaysOptional() throws IOException {
    Schema schema = tightened(write(10, 1, false, Nulls.NONE));
    assertTrue(isRequired(schema, "id"));
    assertFalse(isRequired(schema, "name"));
    assertFalse(isRequired(schema, "address"));
    assertFalse(isRequired(schema, "address.city"));
  }

  @Test
  public void testOneRowGroupWithNullsSpoilsTheProof() throws IOException {
    ParquetMetadata footer =
        write(100, 4, true, new Nulls(r -> r == 60, r -> false, r -> false, r -> false));
    assertTrue("expected several row groups", footer.getBlocks().size() > 1);

    Schema schema = tightened(footer);

    assertFalse(isRequired(schema, "name"));
    assertTrue(isRequired(schema, "address.zip"));
  }

  /** With no rows nothing can violate a required column, so every column counts as proven. */
  @Test
  public void testZeroRowsProveEverything() throws IOException {
    ParquetMetadata footer = write(0, 1, true, Nulls.NONE);
    assertEquals("parquet-mr writes no row group for zero rows", 0, footer.getBlocks().size());

    Schema schema = tightened(footer);

    assertTrue(isRequired(schema, "id"));
    assertTrue(isRequired(schema, "name"));
    assertTrue(isRequired(schema, "address"));
    assertTrue(isRequired(schema, "address.city"));
    assertFalse(isRequired(schema, "tags"));
  }

  /**
   * pyarrow writes an empty table as one row group with zero rows and no statistics. parquet-mr
   * never produces that shape, so the footer is assembled by hand.
   */
  @Test
  public void testEmptyRowGroupWithoutStatsProvesEverything() throws IOException {
    ParquetMetadata footer = withBlocks(write(0, 1, true, Nulls.NONE), emptyBlockWithoutStats());

    Schema schema = tightened(footer);

    assertTrue(isRequired(schema, "id"));
    assertTrue(isRequired(schema, "name"));
    assertTrue(isRequired(schema, "address"));
    assertTrue(isRequired(schema, "address.city"));
    assertFalse(isRequired(schema, "tags"));
  }

  @Test
  public void testEmptyRowGroupDoesNotSpoilTheProof() throws IOException {
    ParquetMetadata written = write(10, 1, true, Nulls.NONE);
    ParquetMetadata footer =
        withBlocks(written, written.getBlocks().get(0), emptyBlockWithoutStats());

    Schema schema = tightened(footer);

    assertTrue(isRequired(schema, "name"));
    assertTrue(isRequired(schema, "address.city"));
  }

  @Test
  public void testEmptyRowGroupDoesNotHideNullsElsewhere() throws IOException {
    ParquetMetadata written =
        write(10, 1, true, new Nulls(r -> r == 3, r -> false, r -> false, r -> false));
    ParquetMetadata footer =
        withBlocks(written, emptyBlockWithoutStats(), written.getBlocks().get(0));

    Schema schema = tightened(footer);

    assertFalse(isRequired(schema, "name"));
    assertTrue(isRequired(schema, "address.city"));
  }

  private static ParquetMetadata withBlocks(ParquetMetadata footer, BlockMetaData... blocks) {
    List<BlockMetaData> list = new ArrayList<>();
    Collections.addAll(list, blocks);
    return new ParquetMetadata(footer.getFileMetaData(), list);
  }

  /** One chunk per leaf of {@link #MIXED}, zero rows, statistics absent as a reader sees them. */
  private static BlockMetaData emptyBlockWithoutStats() {
    BlockMetaData block = new BlockMetaData();
    block.setRowCount(0);
    for (ColumnDescriptor column : MIXED.getColumns()) {
      block.addColumn(
          ColumnChunkMetaData.get(
              ColumnPath.get(column.getPath()),
              column.getPrimitiveType(),
              CompressionCodecName.UNCOMPRESSED,
              new EncodingStats.Builder().build(),
              Collections.emptySet(),
              Statistics.getBuilderForReading(column.getPrimitiveType()).build(),
              0,
              0,
              0,
              0,
              0));
    }
    return block;
  }

  @Test
  public void testCanonicalWithNullFreeColumnsReportsChangedOnly() throws IOException {
    ParquetMetadata footer =
        write(10, 1, true, new Nulls(r -> false, r -> false, r -> false, r -> r == 4));
    CollectDistinctSchemas.SchemaGroup group = FileSchemas.schemaGroup(footer);
    // id is declared required already; zip has a null; the rest flipped
    assertEquals(
        java.util.Arrays.asList("address", "address.city", "name"), group.getNullFreeColumns());
    Schema declared = SchemaParser.fromJson(group.getSchemaJson());
    assertFalse(isRequired(declared, "name"));
  }

  @Test
  public void testMarkRequiredFlipsOnlyNamedColumns() {
    Schema declared =
        new Schema(
            optional(1, "name", Types.StringType.get()),
            optional(
                2,
                "address",
                Types.StructType.of(
                    optional(3, "city", Types.StringType.get()),
                    optional(4, "zip", Types.IntegerType.get()))));
    Schema required =
        FileSchemas.markRequired(
            declared, java.util.Arrays.asList("address", "address.city", "not_a_column"));
    assertFalse(isRequired(required, "name"));
    assertTrue(isRequired(required, "address"));
    assertTrue(isRequired(required, "address.city"));
    assertFalse(isRequired(required, "address.zip"));
    assertEquals(
        declared.asStruct(),
        FileSchemas.markRequired(declared, java.util.Arrays.asList()).asStruct());
  }

  @Test
  public void testNullStructKeepsStructAndLeavesOptional() throws IOException {
    Schema schema =
        tightened(write(10, 1, true, new Nulls(r -> false, r -> r == 5, r -> false, r -> false)));
    assertFalse(isRequired(schema, "address"));
    assertFalse(isRequired(schema, "address.city"));
    assertFalse(isRequired(schema, "address.zip"));
  }

  @Test
  public void testOneProvenLeafProvesTheStruct() throws IOException {
    Schema schema =
        tightened(write(10, 1, true, new Nulls(r -> false, r -> false, r -> r == 2, r -> false)));
    assertTrue(isRequired(schema, "address"));
    assertFalse(isRequired(schema, "address.city"));
    assertTrue(isRequired(schema, "address.zip"));
  }

  @Test
  public void testTightenPreservesIdsNamesAndTypes() throws IOException {
    ParquetMetadata footer = write(10, 1, true, Nulls.NONE);
    Schema converted = ParquetSchemaUtil.convert(footer.getFileMetaData().getSchema());
    Schema schema = FileSchemas.tighten(converted, footer);
    assertEquals(converted.columns().size(), schema.columns().size());
    for (Types.NestedField field : converted.columns()) {
      Types.NestedField after = schema.findField(field.fieldId());
      assertEquals(field.name(), after.name());
      assertEquals(field.type().typeId(), after.type().typeId());
    }
  }

  @Test
  public void testTightenAndCanonicalPreserveDocAndDefaults() {
    Types.NestedField withAttributes =
        Types.NestedField.optional("b")
            .withId(2)
            .ofType(Types.LongType.get())
            .withDoc("the b")
            .withWriteDefault(org.apache.iceberg.expressions.Literal.of(7L))
            .build();
    Schema schema = new Schema(withAttributes, required(1, "a", Types.StringType.get()));
    Schema canonical = FileSchemas.canonical(schema);
    Types.NestedField b = canonical.findField("b");
    assertEquals("the b", b.doc());
    assertEquals(7L, b.writeDefault());
  }

  // ---- canonicalization

  @Test
  public void testSortsTopLevelFieldsAndRenumbers() {
    Schema input =
        new Schema(
            optional(7, "name", Types.StringType.get()), required(3, "id", Types.LongType.get()));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "name", Types.StringType.get()));

    assertSame(expected, FileSchemas.canonical(input));
  }

  @Test
  public void testSortsNestedStructFields() {
    Schema input =
        new Schema(
            optional(
                1,
                "address",
                Types.StructType.of(
                    optional(2, "zip", Types.IntegerType.get()),
                    optional(3, "city", Types.StringType.get()))));
    Schema expected =
        new Schema(
            optional(
                1,
                "address",
                Types.StructType.of(
                    optional(2, "city", Types.StringType.get()),
                    optional(3, "zip", Types.IntegerType.get()))));

    assertSame(expected, FileSchemas.canonical(input));
  }

  @Test
  public void testPermutationsProduceIdenticalJson() {
    Schema a =
        new Schema(
            optional(1, "b", Types.StringType.get()),
            optional(2, "a", Types.StructType.of(optional(3, "y", Types.LongType.get()))),
            optional(4, "c", Types.ListType.ofOptional(5, Types.StringType.get())));
    Schema b =
        new Schema(
            optional(1, "c", Types.ListType.ofOptional(2, Types.StringType.get())),
            optional(3, "a", Types.StructType.of(optional(4, "y", Types.LongType.get()))),
            optional(5, "b", Types.StringType.get()));

    assertEquals(
        SchemaParser.toJson(FileSchemas.canonical(a)),
        SchemaParser.toJson(FileSchemas.canonical(b)));
  }

  /** Ids number every field of a struct before descending into nested types. */
  @Test
  public void testPreservesListMapStructNestingAndOptionality() {
    Schema input =
        new Schema(
            required(
                1,
                "m",
                Types.MapType.ofRequired(
                    2,
                    3,
                    Types.StringType.get(),
                    Types.StructType.of(
                        optional(4, "z", Types.IntegerType.get()),
                        required(5, "a", Types.ListType.ofRequired(6, Types.DoubleType.get()))))));
    Schema expected =
        new Schema(
            required(
                1,
                "m",
                Types.MapType.ofRequired(
                    2,
                    3,
                    Types.StringType.get(),
                    Types.StructType.of(
                        required(4, "a", Types.ListType.ofRequired(6, Types.DoubleType.get())),
                        optional(5, "z", Types.IntegerType.get())))));

    assertSame(expected, FileSchemas.canonical(input));
  }

  @Test
  public void testCanonicalSchemaIsUnchanged() {
    Schema canonical =
        new Schema(
            required(1, "a", Types.LongType.get()),
            optional(2, "b", Types.StructType.of(optional(3, "x", Types.StringType.get()))));

    assertSame(canonical, FileSchemas.canonical(canonical));
  }

  private static void assertSame(Schema expected, Schema actual) {
    assertTrue("expected " + expected + " but was " + actual, expected.sameSchema(actual));
  }
}
