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
package org.apache.beam.sdk.io.delta;

import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DeltaSchemaUtils}. */
@RunWith(JUnit4.class)
public class DeltaSchemaUtilsTest {

  private static final StructField NAME =
      new StructField("name", StringType.STRING, true, columnMappingMetadata(1L, "col-1"));
  private static final StructField AGE =
      new StructField("age", IntegerType.INTEGER, true, columnMappingMetadata(2L, "col-2"));
  private static final StructType SCHEMA = new StructType().add(NAME).add(AGE);

  @Test
  public void testIsReadCompatibleIgnoresMetadataOnlyChanges() {
    Assert.assertTrue(DeltaSchemaUtils.isReadCompatible(SCHEMA, SCHEMA));

    StructType schema =
        new StructType()
            .add("name", StringType.STRING)
            .add("nested", new StructType().add("value", LongType.LONG));

    // Field metadata that does not affect the data, such as column comments, is ignored. This also
    // applies to nested fields.
    FieldMetadata comment = FieldMetadata.builder().putString("comment", "A comment.").build();
    StructType schemaWithComments =
        new StructType()
            .add("name", StringType.STRING, comment)
            .add("nested", new StructType().add("value", LongType.LONG, comment), comment);
    Assert.assertTrue(DeltaSchemaUtils.isReadCompatible(schema, schemaWithComments));

    // Enabling column mapping on an existing table assigns column IDs to the existing columns and
    // sets their physical names to their logical names, so data is still read from the same
    // columns.
    StructType schemaWithColumnMapping =
        new StructType()
            .add("name", StringType.STRING, columnMappingMetadata(1L, "name"))
            .add(
                "nested",
                new StructType().add("value", LongType.LONG, columnMappingMetadata(3L, "value")),
                columnMappingMetadata(2L, "nested"));
    Assert.assertTrue(DeltaSchemaUtils.isReadCompatible(schema, schemaWithColumnMapping));
  }

  @Test
  public void testIsReadCompatibleAllowsAdditiveChanges() {
    StructField city =
        new StructField("city", StringType.STRING, true, columnMappingMetadata(3L, "col-3"));

    // Added nullable columns, at the end or in between existing columns.
    Assert.assertTrue(DeltaSchemaUtils.isReadCompatible(SCHEMA, SCHEMA.add(city)));
    Assert.assertTrue(
        DeltaSchemaUtils.isReadCompatible(SCHEMA, new StructType().add(NAME).add(city).add(AGE)));
    // Reordered columns, since columns are matched by name.
    Assert.assertTrue(
        DeltaSchemaUtils.isReadCompatible(SCHEMA, new StructType().add(AGE).add(NAME)));
    // Relaxed nullability.
    Assert.assertTrue(
        DeltaSchemaUtils.isReadCompatible(
            new StructType().add("name", StringType.STRING, false),
            new StructType().add("name", StringType.STRING, true)));

    // Added nullable nested field.
    Assert.assertTrue(
        DeltaSchemaUtils.isReadCompatible(
            new StructType().add("nested", new StructType().add("value", LongType.LONG)),
            new StructType()
                .add(
                    "nested",
                    new StructType()
                        .add("value", LongType.LONG)
                        .add("other", StringType.STRING, true))));
    // Relaxed nullability of array elements and map values.
    Assert.assertTrue(
        DeltaSchemaUtils.isReadCompatible(
            new StructType().add("array", new ArrayType(StringType.STRING, false)),
            new StructType().add("array", new ArrayType(StringType.STRING, true))));
    Assert.assertTrue(
        DeltaSchemaUtils.isReadCompatible(
            new StructType().add("map", new MapType(StringType.STRING, LongType.LONG, false)),
            new StructType().add("map", new MapType(StringType.STRING, LongType.LONG, true))));
  }

  @Test
  public void testIsReadCompatibleRejectsNonAdditiveChanges() {
    // Dropped column.
    Assert.assertFalse(DeltaSchemaUtils.isReadCompatible(SCHEMA, new StructType().add(NAME)));
    // Renamed column. Renames preserve the column mapping metadata of the column.
    Assert.assertFalse(
        DeltaSchemaUtils.isReadCompatible(
            SCHEMA,
            new StructType().add("full_name", StringType.STRING, NAME.getMetadata()).add(AGE)));
    // Renamed column where only the case of the name changed, since names are case-sensitive.
    Assert.assertFalse(
        DeltaSchemaUtils.isReadCompatible(
            SCHEMA, new StructType().add("Name", StringType.STRING, NAME.getMetadata()).add(AGE)));
    // Changed data type.
    Assert.assertFalse(
        DeltaSchemaUtils.isReadCompatible(
            SCHEMA, new StructType().add(NAME).add("age", LongType.LONG, AGE.getMetadata())));
    // Tightened nullability.
    Assert.assertFalse(
        DeltaSchemaUtils.isReadCompatible(
            SCHEMA,
            new StructType().add(NAME).add("age", IntegerType.INTEGER, false, AGE.getMetadata())));
    // Added non-nullable column.
    Assert.assertFalse(
        DeltaSchemaUtils.isReadCompatible(
            SCHEMA,
            SCHEMA.add("city", StringType.STRING, false, columnMappingMetadata(3L, "col-3"))));

    // A column that was dropped and re-added with the same name and type gets a new column ID and
    // physical name.
    Assert.assertFalse(
        DeltaSchemaUtils.isReadCompatible(
            SCHEMA,
            new StructType()
                .add(NAME)
                .add("age", IntegerType.INTEGER, columnMappingMetadata(3L, "col-3"))));
    // A re-added column is detected through its new column ID even if it reuses the physical name.
    Assert.assertFalse(
        DeltaSchemaUtils.isReadCompatible(
            SCHEMA,
            new StructType()
                .add(NAME)
                .add("age", IntegerType.INTEGER, columnMappingMetadata(3L, "col-2"))));
    // A column that was dropped and re-added after enabling column mapping is detected through its
    // physical name, which differs from the logical name that was used to read the column before.
    Assert.assertFalse(
        DeltaSchemaUtils.isReadCompatible(
            new StructType().add("age", IntegerType.INTEGER),
            new StructType().add("age", IntegerType.INTEGER, columnMappingMetadata(2L, "col-2"))));

    // Non-additive changes of nested types.
    Assert.assertFalse(
        DeltaSchemaUtils.isReadCompatible(
            new StructType()
                .add(
                    "nested",
                    new StructType().add("value", LongType.LONG).add("other", StringType.STRING)),
            new StructType().add("nested", new StructType().add("value", LongType.LONG))));
    Assert.assertFalse(
        DeltaSchemaUtils.isReadCompatible(
            new StructType().add("nested", new StructType().add("value", IntegerType.INTEGER)),
            new StructType().add("nested", new StructType().add("value", LongType.LONG))));
    Assert.assertFalse(
        DeltaSchemaUtils.isReadCompatible(
            new StructType().add("array", new ArrayType(IntegerType.INTEGER, true)),
            new StructType().add("array", new ArrayType(LongType.LONG, true))));
    Assert.assertFalse(
        DeltaSchemaUtils.isReadCompatible(
            new StructType().add("array", new ArrayType(StringType.STRING, true)),
            new StructType().add("array", new ArrayType(StringType.STRING, false))));
    Assert.assertFalse(
        DeltaSchemaUtils.isReadCompatible(
            new StructType().add("map", new MapType(StringType.STRING, LongType.LONG, true)),
            new StructType().add("map", new MapType(StringType.STRING, LongType.LONG, false))));
  }

  @Test
  public void testCheckReadCompatible() {
    // Does not throw for additive changes.
    DeltaSchemaUtils.checkReadCompatible(
        SCHEMA,
        1L,
        SCHEMA.add("city", StringType.STRING, true, columnMappingMetadata(3L, "col-3")),
        0L,
        2L);

    IllegalArgumentException e =
        Assert.assertThrows(
            IllegalArgumentException.class,
            () ->
                DeltaSchemaUtils.checkReadCompatible(
                    SCHEMA, 1L, new StructType().add(NAME), 0L, 2L));
    Assert.assertTrue(
        e.getMessage()
            .startsWith(
                "Cannot read changes between versions 0 and 2 since the table schema at version 1 "
                    + "is not compatible with the table schema at the end version 2"));
  }

  /** Returns field metadata containing the given column mapping ID and physical name. */
  private static FieldMetadata columnMappingMetadata(long columnId, String physicalName) {
    return FieldMetadata.builder()
        .putLong(ColumnMapping.COLUMN_MAPPING_ID_KEY, columnId)
        .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, physicalName)
        .build();
  }
}
