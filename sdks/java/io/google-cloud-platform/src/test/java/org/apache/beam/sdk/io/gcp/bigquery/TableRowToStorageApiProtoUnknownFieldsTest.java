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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import java.io.ByteArrayOutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto.ErrorCollector;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto.SchemaDoesntMatchException;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto.SchemaInformation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Regression tests for unknown-column collector attachment and pruning. */
@RunWith(JUnit4.class)
@SuppressWarnings("nullness") // TODO(https://github.com/apache/beam/issues/20497)
public class TableRowToStorageApiProtoUnknownFieldsTest {
  private static final TableFieldSchema KNOWN_FIELD =
      new TableFieldSchema().setName("known").setType("STRING");
  private static final TableSchema SCHEMA =
      new TableSchema()
          .setFields(
              Arrays.asList(
                  new TableFieldSchema()
                      .setName("record")
                      .setType("RECORD")
                      .setFields(Collections.singletonList(KNOWN_FIELD)),
                  new TableFieldSchema()
                      .setName("records")
                      .setType("RECORD")
                      .setMode("REPEATED")
                      .setFields(Collections.singletonList(KNOWN_FIELD))));

  private static LinkedHashMap<String, Object> map(Object... entries) {
    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    for (int i = 0; i < entries.length; i += 2) {
      result.put((String) entries[i], entries[i + 1]);
    }
    return result;
  }

  private static DynamicMessage convert(AbstractMap<String, Object> row, TableRow unknown)
      throws Exception {
    return convert(SCHEMA, row, unknown);
  }

  private static DynamicMessage convert(
      TableSchema schema, AbstractMap<String, Object> row, TableRow unknown) throws Exception {
    return TableRowToStorageApiProto.messageFromMap(
        SchemaInformation.fromTableSchema(schema),
        TableRowToStorageApiProto.getDescriptorFromTableSchema(schema, true, false),
        row,
        true,
        true,
        unknown,
        null,
        null,
        ErrorCollector.DONT_COLLECT);
  }

  private static byte[] encoded(TableRow row) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    TableRowJsonCoder.of().encode(row, bytes);
    return bytes.toByteArray();
  }

  private static TableSchema fSchema() {
    return new TableSchema()
        .setFields(
            Collections.singletonList(
                new TableFieldSchema()
                    .setName("f")
                    .setType("RECORD")
                    .setFields(Collections.singletonList(KNOWN_FIELD))));
  }

  @Test
  public void testKnownSingleStructPrunesPlaceholder() throws Exception {
    TableRow unknown = new TableRow();
    DynamicMessage message = convert(map("record", map("known", "value")), unknown);
    Descriptor descriptor = message.getDescriptorForType();
    assertTrue(unknown.isEmpty());
    assertTrue(message.getField(descriptor.findFieldByName("record")).toString().contains("value"));
  }

  @Test
  public void testKnownRepeatedStructPrunesPlaceholders() throws Exception {
    TableRow unknown = new TableRow();
    DynamicMessage message =
        convert(map("records", Arrays.asList(map("known", "a"), map("known", "b"))), unknown);
    Descriptor descriptor = message.getDescriptorForType();
    assertTrue(unknown.isEmpty());
    assertEquals(2, message.getRepeatedFieldCount(descriptor.findFieldByName("records")));
  }

  @Test
  public void testEmptyRepeatedStructPrunesPlaceholder() throws Exception {
    TableRow unknown = new TableRow();
    convert(map("records", Collections.emptyList()), unknown);
    assertTrue(unknown.isEmpty());
  }

  @Test
  public void testSingleStructPreservesUnknownJsonAndProtoBytes() throws Exception {
    TableRow unknown = new TableRow();
    DynamicMessage actual = convert(map("record", map("known", "value", "added", "new")), unknown);
    DynamicMessage expected = convert(map("record", map("known", "value")), new TableRow());
    assertArrayEquals(expected.toByteArray(), actual.toByteArray());
    assertArrayEquals(
        encoded(new TableRow().set("record", new TableRow().set("added", "new"))),
        encoded(unknown));
  }

  @Test
  public void testRepeatedStructPreservesUnknownPositionsAndProtoBytes() throws Exception {
    for (int position = 0; position < 3; position++) {
      List<Object> rows = new ArrayList<>();
      List<Object> knownRows = new ArrayList<>();
      List<TableRow> expectedUnknown = new ArrayList<>();
      for (int index = 0; index < 3; index++) {
        knownRows.add(map("known", "value-" + index));
        rows.add(
            index == position
                ? map("known", "value-" + index, "added", "new")
                : map("known", "value-" + index));
        expectedUnknown.add(
            index == position ? new TableRow().set("added", "new") : new TableRow());
      }
      TableRow unknown = new TableRow();
      DynamicMessage actual = convert(map("records", rows), unknown);
      DynamicMessage expected = convert(map("records", knownRows), new TableRow());
      assertArrayEquals(expected.toByteArray(), actual.toByteArray());
      assertArrayEquals(encoded(new TableRow().set("records", expectedUnknown)), encoded(unknown));
    }
  }

  @Test
  public void testRetainsPreexistingUnknownStruct() throws Exception {
    TableRow nested = new TableRow().set("old", "value");
    TableRow unknown = new TableRow().set("record", nested);
    convert(map("record", map()), unknown);
    assertSame(nested, unknown.get("record"));
    assertArrayEquals(
        encoded(new TableRow().set("record", new TableRow().set("old", "value"))),
        encoded(unknown));
  }

  @Test
  public void testPrunesPreexistingNullAndEmptyRepeatedPlaceholders() throws Exception {
    TableRow unknown = new TableRow().set("records", Arrays.asList(null, new TableRow()));
    convert(map("records", Collections.emptyList()), unknown);
    assertTrue(unknown.isEmpty());
  }

  @Test
  public void testNonNullModelCellListKeepsSingleStructNonempty() throws Exception {
    for (List<TableCell> cells :
        Arrays.asList(
            Collections.<TableCell>emptyList(),
            Collections.singletonList(new TableCell().setV("cell")))) {
      TableRow nested = new TableRow().setF(cells);
      TableRow unknown = new TableRow().set("record", nested);
      convert(map("record", map("known", "value")), unknown);
      assertSame(nested, unknown.get("record"));
      assertSame(cells, nested.getF());
      assertArrayEquals(
          encoded(new TableRow().set("record", new TableRow().setF(cells))), encoded(unknown));
    }
  }

  @Test
  public void testEmptyModelCellListKeepsRepeatedPlaceholderNonempty() throws Exception {
    TableRow nested = new TableRow().setF(Collections.emptyList());
    List<TableRow> rows = Arrays.asList(null, nested, new TableRow());
    TableRow unknown = new TableRow().set("records", rows);
    convert(map("records", Collections.emptyList()), unknown);
    assertSame(rows, unknown.get("records"));
  }

  @Test
  public void testOrdinaryMapsKeepTheirEmptinessBehavior() throws Exception {
    LinkedHashMap<String, Object> nested = map("old", "value");
    TableRow unknown = new TableRow().set("record", nested);
    convert(map("record", null), unknown);
    assertSame(nested, unknown.get("record"));
    TableRow empty = new TableRow().set("record", map());
    convert(map("record", null), empty);
    assertTrue(empty.isEmpty());
  }

  @Test
  public void testUnknownFUsesModelSetter() throws Exception {
    List<TableCell> cells = Collections.singletonList(new TableCell().setV("value"));
    TableRow unknown = new TableRow();
    convert(map("f", cells), unknown);
    assertSame(cells, unknown.getF());
    assertFalse(unknown.getUnknownKeys().containsKey("f"));
  }

  @Test
  public void testUnknownFPreservesShadowedBackingKey() throws Exception {
    List<TableCell> cells = Collections.singletonList(new TableCell().setV("value"));
    TableRow unknown = new TableRow();
    unknown.getUnknownKeys().put("f", "shadow");
    convert(map("f", cells), unknown);
    assertSame(cells, unknown.getF());
    assertEquals("shadow", unknown.getUnknownKeys().get("f"));
  }

  @Test
  public void testUnknownFPreservesSetterTypeError() {
    TableRow unknown = new TableRow();
    assertThrows(
        IllegalArgumentException.class, () -> convert(map("f", map("known", "value")), unknown));
    assertFalse(unknown.getUnknownKeys().containsKey("f"));
  }

  @Test
  public void testKnownFPreservesAttachmentTypeError() {
    SchemaDoesntMatchException error =
        assertThrows(
            SchemaDoesntMatchException.class,
            () -> convert(fSchema(), map("f", map("known", "value")), new TableRow()));
    assertTrue(error.getCause() instanceof IllegalArgumentException);
  }

  @Test
  public void testKnownFPreservesUnsupportedModelRemoval() {
    TableRow unknown = new TableRow().setF(Collections.emptyList());
    SchemaDoesntMatchException error =
        assertThrows(
            SchemaDoesntMatchException.class, () -> convert(fSchema(), map("f", null), unknown));
    assertTrue(error.getCause() instanceof UnsupportedOperationException);
    assertEquals(Collections.emptyList(), unknown.getF());
  }

  @Test
  public void testNullCollectorPreservesProtoBytes() throws Exception {
    AbstractMap<String, Object> row = map("record", map("known", "value", "added", "new"));
    assertArrayEquals(convert(row, new TableRow()).toByteArray(), convert(row, null).toByteArray());
  }

  @Test
  public void testCellFormatUsesPublicConverterPath() throws Exception {
    TableRow row =
        new TableRow()
            .setF(
                Arrays.asList(
                    new TableCell().setV(map("known", "value")),
                    new TableCell().setV(Collections.emptyList())));
    DynamicMessage actual =
        TableRowToStorageApiProto.messageFromTableRow(
            SchemaInformation.fromTableSchema(SCHEMA),
            TableRowToStorageApiProto.getDescriptorFromTableSchema(SCHEMA, true, false),
            row,
            true,
            true,
            new TableRow(),
            null,
            -1,
            ErrorCollector.DONT_COLLECT);
    DynamicMessage expected =
        convert(
            map("record", map("known", "value"), "records", Collections.emptyList()),
            new TableRow());
    assertArrayEquals(expected.toByteArray(), actual.toByteArray());
  }
}
