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
package org.apache.beam.sdk.io.iceberg.cdc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link OverlapRange}. */
@RunWith(JUnit4.class)
public class OverlapRangeTest {
  private static final org.apache.iceberg.Schema SINGLE_PK_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.IntegerType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get())),
          ImmutableSet.of(1));

  private static final org.apache.iceberg.Schema COMPOSITE_PK_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.optional(3, "data", Types.StringType.get()),
              Types.NestedField.required(1, "account", Types.StringType.get()),
              Types.NestedField.optional(4, "extra", Types.IntegerType.get()),
              Types.NestedField.required(2, "sequence", Types.IntegerType.get())),
          ImmutableSet.of(1, 2));

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");
  @Rule public final TestName testName = new TestName();

  @Test
  public void containsUsesInclusiveSingleColumnBounds() throws Exception {
    OverlapRange range = overlapRange(SINGLE_PK_SCHEMA);
    StructLike lower = range.toStructLike(pkRow(range.recordIdSchema(), 10));
    StructLike upper = range.toStructLike(pkRow(range.recordIdSchema(), 20));

    assertFalse(range.contains(singlePkRecord(9), lower, upper));
    assertTrue(range.contains(singlePkRecord(10), lower, upper));
    assertTrue(range.contains(singlePkRecord(15), lower, upper));
    assertTrue(range.contains(singlePkRecord(20), lower, upper));
    assertFalse(range.contains(singlePkRecord(21), lower, upper));
  }

  @Test
  public void containsUsesLexicographicCompositeBounds() throws Exception {
    OverlapRange range = overlapRange(COMPOSITE_PK_SCHEMA);
    StructLike lower = range.toStructLike(pkRow(range.recordIdSchema(), "a", 2));
    StructLike upper = range.toStructLike(pkRow(range.recordIdSchema(), "b", 1));

    assertFalse(range.contains(compositePkRecord("a", 1), lower, upper));
    assertTrue(range.contains(compositePkRecord("a", 2), lower, upper));
    assertTrue(range.contains(compositePkRecord("a", 9), lower, upper));
    assertTrue(range.contains(compositePkRecord("b", 0), lower, upper));
    assertTrue(range.contains(compositePkRecord("b", 1), lower, upper));
    assertFalse(range.contains(compositePkRecord("b", 2), lower, upper));
  }

  @Test
  public void nullBoundsAreConservative() throws Exception {
    OverlapRange range = overlapRange(SINGLE_PK_SCHEMA);
    StructLike lower = range.toStructLike(pkRow(range.recordIdSchema(), 10));
    StructLike upper = range.toStructLike(pkRow(range.recordIdSchema(), 20));

    assertNull(range.toStructLike(null));
    assertTrue(range.contains(singlePkRecord(1), null, upper));
    assertTrue(range.contains(singlePkRecord(100), lower, null));
    assertTrue(range.contains(singlePkRecord(100), null, null));
  }

  @Test
  public void recordIdProjectionUsesIdentifierFieldsFromFullRecord() throws Exception {
    OverlapRange range = overlapRange(COMPOSITE_PK_SCHEMA);
    StructLike lower = range.toStructLike(pkRow(range.recordIdSchema(), "acct", 7));
    StructLike upper = range.toStructLike(pkRow(range.recordIdSchema(), "acct", 7));

    assertTrue(range.contains(compositePkRecord("acct", 7), lower, upper));

    assertEquals("acct", range.recordIdProjection().get(0, String.class));
    assertEquals(7, (int) range.recordIdProjection().get(1, Integer.class));
  }

  private OverlapRange overlapRange(org.apache.iceberg.Schema schema) throws IOException {
    TableIdentifier tableId = TableIdentifier.of("default", testName.getMethodName());
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogProperties(
                ImmutableMap.of("type", "hadoop", "warehouse", warehouse.location))
            .build();
    catalogConfig.catalog().createTable(tableId, schema);
    IcebergScanConfig scanConfig =
        IcebergScanConfig.builder()
            .setCatalogConfig(catalogConfig)
            .setTableIdentifier(tableId)
            .setSchema(IcebergUtils.icebergSchemaToBeamSchema(schema))
            .setUseCdc(true)
            .build();
    return OverlapRange.forScanConfig(scanConfig);
  }

  private static Row pkRow(Schema recordIdSchema, Object... values) {
    return Row.withSchema(IcebergUtils.icebergSchemaToBeamSchema(recordIdSchema))
        .addValues(values)
        .build();
  }

  private static Record singlePkRecord(int id) {
    GenericRecord record = GenericRecord.create(SINGLE_PK_SCHEMA);
    record.setField("id", id);
    record.setField("data", "v" + id);
    return record;
  }

  private static Record compositePkRecord(String account, int sequence) {
    GenericRecord record = GenericRecord.create(COMPOSITE_PK_SCHEMA);
    record.setField("data", "payload");
    record.setField("account", account);
    record.setField("extra", 100);
    record.setField("sequence", sequence);
    return record;
  }
}
