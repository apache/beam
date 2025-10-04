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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.Descriptors.Descriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests to reproduce the IllegalArgumentException when setting non-List values to the 'f' field in
 * TableRow objects. This demonstrates the issue where user-created TableRow objects with a column
 * named 'f' cause type conflicts with BigQuery's internal 'f' field.
 *
 * <p>NOTE: These tests should be updated or removed when
 * https://github.com/apache/beam/issues/33531 is fully resolved. Once the issue is fixed, TableRow
 * objects should properly handle user-defined columns named 'f' without throwing
 * IllegalArgumentException.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class TableRowFieldFIssueTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  // Schema with a user-defined column named 'f' (which conflicts with BigQuery's internal 'f'
  // field)
  private static final TableSchema SCHEMA_WITH_F_COLUMN =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema().setType("STRING").setName("name"),
                  new TableFieldSchema().setType("FLOAT64").setName("f")));

  @Test
  public void testTableRowWithDoubleInFFieldCausesIllegalArgumentException() throws Exception {
    // Create a TableRow with a column named 'f' containing a Double value
    // This simulates user code that creates a TableRow with a column coincidentally named 'f'

    // The exception happens when setting the 'f' field, so we need to catch it here
    try {
      TableRow row =
          new TableRow()
              .set("name", "test_record")
              .set("f", 3.14159); // This Double value will cause the issue

      // If we get here without exception, then the issue is in messageFromTableRow
      Descriptor descriptor =
          TableRowToStorageApiProto.getDescriptorFromTableSchema(SCHEMA_WITH_F_COLUMN, true, false);
      TableRowToStorageApiProto.SchemaInformation schemaInformation =
          TableRowToStorageApiProto.SchemaInformation.fromTableSchema(SCHEMA_WITH_F_COLUMN);

      // This should trigger the IllegalArgumentException
      TableRowToStorageApiProto.messageFromTableRow(
          schemaInformation, descriptor, row, false, false, null, null, -1);
      fail("Expected IllegalArgumentException was not thrown");
    } catch (IllegalArgumentException e) {
      // Verify the exception message contains the expected parts
      assertTrue(
          "Exception message should mention 'Can not set java.util.List field'",
          e.getMessage().contains("Can not set java.util.List field"));
      assertTrue(
          "Exception message should mention 'TableRow.f'", e.getMessage().contains("TableRow.f"));
      assertTrue(
          "Exception message should mention 'java.lang.Double'",
          e.getMessage().contains("java.lang.Double"));
    }
  }

  @Test
  public void testTableRowWithStringInFFieldCausesIllegalArgumentException() throws Exception {
    // Create a TableRow with a column named 'f' containing a String value
    // This is another common scenario that would cause the same issue

    // The exception happens when setting the 'f' field, so we need to catch it here
    try {
      TableRow row =
          new TableRow()
              .set("name", "test_record")
              .set("f", "some_string_value"); // This String value will also cause the issue

      // Use a schema where 'f' is a STRING field
      TableSchema schemaWithStringF =
          new TableSchema()
              .setFields(
                  ImmutableList.of(
                      new TableFieldSchema().setType("STRING").setName("name"),
                      new TableFieldSchema().setType("STRING").setName("f")));

      Descriptor descriptor =
          TableRowToStorageApiProto.getDescriptorFromTableSchema(schemaWithStringF, true, false);
      TableRowToStorageApiProto.SchemaInformation schemaInformation =
          TableRowToStorageApiProto.SchemaInformation.fromTableSchema(schemaWithStringF);

      // This should trigger the IllegalArgumentException
      TableRowToStorageApiProto.messageFromTableRow(
          schemaInformation, descriptor, row, false, false, null, null, -1);
      fail("Expected IllegalArgumentException was not thrown");
    } catch (IllegalArgumentException e) {
      // Verify the exception message contains the expected parts
      assertTrue(
          "Exception message should mention 'Can not set java.util.List field'",
          e.getMessage().contains("Can not set java.util.List field"));
      assertTrue(
          "Exception message should mention 'TableRow.f'", e.getMessage().contains("TableRow.f"));
      assertTrue(
          "Exception message should mention 'java.lang.String'",
          e.getMessage().contains("java.lang.String"));
    }
  }

  @Test
  public void testTableRowWithIntegerInFFieldCausesIllegalArgumentException() throws Exception {
    // Create a TableRow with a column named 'f' containing an Integer value
    // This demonstrates the issue with any non-List type in the 'f' field

    // The exception happens when setting the 'f' field, so we need to catch it here
    try {
      TableRow row =
          new TableRow()
              .set("name", "test_record")
              .set("f", 42); // This Integer value will also cause the issue

      // Use a schema where 'f' is an INTEGER field
      TableSchema schemaWithIntegerF =
          new TableSchema()
              .setFields(
                  ImmutableList.of(
                      new TableFieldSchema().setType("STRING").setName("name"),
                      new TableFieldSchema().setType("INTEGER").setName("f")));

      Descriptor descriptor =
          TableRowToStorageApiProto.getDescriptorFromTableSchema(schemaWithIntegerF, true, false);
      TableRowToStorageApiProto.SchemaInformation schemaInformation =
          TableRowToStorageApiProto.SchemaInformation.fromTableSchema(schemaWithIntegerF);

      // This should trigger the IllegalArgumentException
      TableRowToStorageApiProto.messageFromTableRow(
          schemaInformation, descriptor, row, false, false, null, null, -1);
      fail("Expected IllegalArgumentException was not thrown");
    } catch (IllegalArgumentException e) {
      // Verify the exception message contains the expected parts
      assertTrue(
          "Exception message should mention 'Can not set java.util.List field'",
          e.getMessage().contains("Can not set java.util.List field"));
      assertTrue(
          "Exception message should mention 'TableRow.f'", e.getMessage().contains("TableRow.f"));
      assertTrue(
          "Exception message should mention 'java.lang.Integer'",
          e.getMessage().contains("java.lang.Integer"));
    }
  }

  @Test
  public void testCaptureActualExceptionMessage() throws Exception {
    // This test captures the actual exception message to help us understand the exact format
    try {
      TableRow row = new TableRow().set("name", "test_record").set("f", 3.14159);

      Descriptor descriptor =
          TableRowToStorageApiProto.getDescriptorFromTableSchema(SCHEMA_WITH_F_COLUMN, true, false);
      TableRowToStorageApiProto.SchemaInformation schemaInformation =
          TableRowToStorageApiProto.SchemaInformation.fromTableSchema(SCHEMA_WITH_F_COLUMN);

      TableRowToStorageApiProto.messageFromTableRow(
          schemaInformation, descriptor, row, false, false, null, null, -1);
      fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException e) {
      // Print the actual exception message for debugging
      System.out.println("Actual exception message: " + e.getMessage());
      // Verify it contains the expected parts
      assertTrue(
          "Exception message should contain 'Can not set'", e.getMessage().contains("Can not set"));
      assertTrue(
          "Exception message should contain 'java.util.List field'",
          e.getMessage().contains("java.util.List field"));
      assertTrue(
          "Exception message should contain 'TableRow.f'", e.getMessage().contains("TableRow.f"));
      assertTrue(
          "Exception message should contain 'java.lang.Double'",
          e.getMessage().contains("java.lang.Double"));
    }
  }

  @Test
  public void testTableRowWithListTableCellInFFieldWorks() throws Exception {
    // This test demonstrates the proper way to handle a column named 'f' using List<TableCell>
    // as implemented in the fix from https://github.com/apache/beam/pull/16872
    // This test should PASS, showing that the workaround works correctly

    TableRow row = new TableRow().set("name", "test_record");

    // Use setF() with List<TableCell> instead of set("f", value)
    // This is the correct way to handle columns named 'f'
    TableCell cell = new TableCell().setV(3.14159);
    row.setF(ImmutableList.of(cell));

    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(SCHEMA_WITH_F_COLUMN, true, false);
    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(SCHEMA_WITH_F_COLUMN);

    // This should NOT throw an exception because we're using the correct List<TableCell> format
    try {
      TableRowToStorageApiProto.messageFromTableRow(
          schemaInformation, descriptor, row, false, false, null, null, -1);
      // If we reach here, the fix is working correctly
    } catch (Exception e) {
      fail(
          "Expected no exception when using List<TableCell> for 'f' field, but got: "
              + e.getMessage());
    }
  }

  @Test
  public void testDemonstrateWorkaroundWithSetF() throws Exception {
    // This test demonstrates the workaround: using setF() with TableCell objects
    // instead of set("f", value)
    TableRow row = new TableRow().set("name", "test_record");

    // Instead of row.set("f", 3.14159), use setF() with TableCell
    TableCell cell = new TableCell().setV(3.14159);
    row.setF(ImmutableList.of(cell));

    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(SCHEMA_WITH_F_COLUMN, true, false);
    TableRowToStorageApiProto.SchemaInformation schemaInformation =
        TableRowToStorageApiProto.SchemaInformation.fromTableSchema(SCHEMA_WITH_F_COLUMN);

    // This should NOT throw an exception because we're using the correct format
    try {
      TableRowToStorageApiProto.messageFromTableRow(
          schemaInformation, descriptor, row, false, false, null, null, -1);
      // If we reach here, the workaround is successful
    } catch (IllegalArgumentException e) {
      fail("Workaround should not throw IllegalArgumentException, but got: " + e.getMessage());
    }
  }
}
