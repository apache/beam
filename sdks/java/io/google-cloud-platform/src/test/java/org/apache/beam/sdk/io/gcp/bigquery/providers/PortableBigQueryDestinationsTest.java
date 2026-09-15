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
package org.apache.beam.sdk.io.gcp.bigquery.providers;

import static org.apache.beam.sdk.io.gcp.bigquery.providers.PortableBigQueryDestinations.DESTINATION;
import static org.apache.beam.sdk.io.gcp.bigquery.providers.PortableBigQueryDestinations.RECORD;
import static org.apache.beam.sdk.io.gcp.bigquery.providers.PortableBigQueryDestinations.SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link PortableBigQueryDestinations}. */
@RunWith(JUnit4.class)
public class PortableBigQueryDestinationsTest {

  private static final Schema UNION_RECORD_SCHEMA =
      Schema.builder()
          .addNullableStringField("name")
          .addNullableInt64Field("number")
          .addNullableDoubleField("score")
          .build();

  private static final String SCHEMA_JSON_1 =
      BigQueryHelpers.toJsonString(
          new TableSchema()
              .setFields(
                  Arrays.asList(
                      new TableFieldSchema().setName("name").setType("STRING").setMode("NULLABLE"),
                      new TableFieldSchema()
                          .setName("number")
                          .setType("INTEGER")
                          .setMode("NULLABLE"))));

  private static final String SCHEMA_JSON_2 =
      BigQueryHelpers.toJsonString(
          new TableSchema()
              .setFields(
                  Arrays.asList(
                      new TableFieldSchema().setName("name").setType("STRING").setMode("NULLABLE"),
                      new TableFieldSchema()
                          .setName("score")
                          .setType("FLOAT")
                          .setMode("NULLABLE"))));

  @Test
  public void testGetDestinationAndSchema_staticSchema() {
    Schema wrapperSchema =
        Schema.builder()
            .addStringField(DESTINATION)
            .addRowField(RECORD, UNION_RECORD_SCHEMA)
            .build();

    BigQueryWriteConfiguration config =
        BigQueryWriteConfiguration.builder()
            .setTable(BigQueryWriteConfiguration.DYNAMIC_DESTINATIONS)
            .build();

    PortableBigQueryDestinations destinations =
        new PortableBigQueryDestinations(UNION_RECORD_SCHEMA, config);

    Row recordRow =
        Row.withSchema(UNION_RECORD_SCHEMA)
            .withFieldValue("name", "alice")
            .withFieldValue("number", 1L)
            .withFieldValue("score", null)
            .build();
    Row wrapperRow =
        Row.withSchema(wrapperSchema)
            .withFieldValue(DESTINATION, "proj:ds.table1")
            .withFieldValue(RECORD, recordRow)
            .build();

    ValueInSingleWindow<Row> windowedRow =
        ValueInSingleWindow.of(
            wrapperRow, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);

    KV<String, String> dest = destinations.getDestination(windowedRow);
    assertEquals("proj:ds.table1", dest.getKey());
    assertNull(dest.getValue());

    TableSchema resolvedSchema = destinations.getSchema(dest);
    assertNotNull(resolvedSchema);
    assertEquals(3, resolvedSchema.getFields().size());
  }

  @Test
  public void testGetDestinationAndSchema_dynamicSchemaWithJson() {
    Schema wrapperSchema =
        Schema.builder()
            .addStringField(DESTINATION)
            .addStringField(SCHEMA)
            .addRowField(RECORD, UNION_RECORD_SCHEMA)
            .build();

    BigQueryWriteConfiguration config =
        BigQueryWriteConfiguration.builder()
            .setTable(BigQueryWriteConfiguration.DYNAMIC_DESTINATIONS)
            .build();

    PortableBigQueryDestinations destinations =
        new PortableBigQueryDestinations(UNION_RECORD_SCHEMA, config);

    Row recordRow =
        Row.withSchema(UNION_RECORD_SCHEMA)
            .withFieldValue("name", "alice")
            .withFieldValue("number", 1L)
            .withFieldValue("score", null)
            .build();
    Row wrapperRow =
        Row.withSchema(wrapperSchema)
            .withFieldValue(DESTINATION, "proj:ds.table1")
            .withFieldValue(SCHEMA, SCHEMA_JSON_1)
            .withFieldValue(RECORD, recordRow)
            .build();

    ValueInSingleWindow<Row> windowedRow =
        ValueInSingleWindow.of(
            wrapperRow, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);

    KV<String, String> dest = destinations.getDestination(windowedRow);
    assertEquals("proj:ds.table1", dest.getKey());
    assertEquals(SCHEMA_JSON_1, dest.getValue());

    TableSchema resolvedSchema = destinations.getSchema(dest);
    assertNotNull(resolvedSchema);
    assertEquals(2, resolvedSchema.getFields().size());
    assertEquals("name", resolvedSchema.getFields().get(0).getName());
    assertEquals("number", resolvedSchema.getFields().get(1).getName());

    Row wrapperRow2 =
        Row.withSchema(wrapperSchema)
            .withFieldValue(DESTINATION, "proj:ds.table2")
            .withFieldValue(SCHEMA, SCHEMA_JSON_2)
            .withFieldValue(RECORD, recordRow)
            .build();
    ValueInSingleWindow<Row> windowedRow2 =
        ValueInSingleWindow.of(
            wrapperRow2, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
    KV<String, String> dest2 = destinations.getDestination(windowedRow2);
    assertEquals("proj:ds.table2", dest2.getKey());
    assertEquals(SCHEMA_JSON_2, dest2.getValue());

    TableSchema resolvedSchema2 = destinations.getSchema(dest2);
    assertNotNull(resolvedSchema2);
    assertEquals(2, resolvedSchema2.getFields().size());
    assertEquals("name", resolvedSchema2.getFields().get(0).getName());
    assertEquals("score", resolvedSchema2.getFields().get(1).getName());
  }

  @Test
  public void testGetDestinationAndSchema_dynamicSchemaEmptyFallsBackToNull() {
    Schema wrapperSchema =
        Schema.builder()
            .addStringField(DESTINATION)
            .addNullableField(SCHEMA, Schema.FieldType.STRING)
            .addRowField(RECORD, UNION_RECORD_SCHEMA)
            .build();

    BigQueryWriteConfiguration config =
        BigQueryWriteConfiguration.builder()
            .setTable(BigQueryWriteConfiguration.DYNAMIC_DESTINATIONS)
            .build();

    PortableBigQueryDestinations destinations =
        new PortableBigQueryDestinations(UNION_RECORD_SCHEMA, config);

    Row recordRow =
        Row.withSchema(UNION_RECORD_SCHEMA)
            .withFieldValue("name", "alice")
            .withFieldValue("number", 1L)
            .withFieldValue("score", null)
            .build();
    Row wrapperRow =
        Row.withSchema(wrapperSchema)
            .withFieldValue(DESTINATION, "proj:ds.table1")
            .withFieldValue(SCHEMA, "")
            .withFieldValue(RECORD, recordRow)
            .build();

    ValueInSingleWindow<Row> windowedRow =
        ValueInSingleWindow.of(
            wrapperRow, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);

    KV<String, String> dest = destinations.getDestination(windowedRow);
    assertEquals("proj:ds.table1", dest.getKey());
    assertEquals("", dest.getValue());

    // When dynamic schema is empty/unspecified, getSchema should return null so that
    // StorageApiDynamicDestinationsTableRow fetches the existing table schema from SCHEMA_CACHE.
    assertNull(destinations.getSchema(dest));
  }

  @Test
  public void testFilterFormatFunction_filtersExtraUnionFields() {
    Schema wrapperSchema =
        Schema.builder()
            .addStringField(DESTINATION)
            .addStringField(SCHEMA)
            .addRowField(RECORD, UNION_RECORD_SCHEMA)
            .build();

    BigQueryWriteConfiguration config =
        BigQueryWriteConfiguration.builder()
            .setTable(BigQueryWriteConfiguration.DYNAMIC_DESTINATIONS)
            .build();

    PortableBigQueryDestinations destinations =
        new PortableBigQueryDestinations(UNION_RECORD_SCHEMA, config);

    SerializableFunction<Row, TableRow> formatFn = destinations.getFilterFormatFunction(true);

    // Even if 'score' has a non-null default/padding value in the union Row,
    // SCHEMA_JSON_1 only includes ['name', 'number'], so 'score' must be filtered out.
    Row recordRow =
        Row.withSchema(UNION_RECORD_SCHEMA)
            .withFieldValue("name", "alice")
            .withFieldValue("number", 1L)
            .withFieldValue("score", 99.5)
            .build();
    Row wrapperRow =
        Row.withSchema(wrapperSchema)
            .withFieldValue(DESTINATION, "proj:ds.table1")
            .withFieldValue(SCHEMA, SCHEMA_JSON_1)
            .withFieldValue(RECORD, recordRow)
            .build();

    TableRow tableRow = formatFn.apply(wrapperRow);
    assertEquals("alice", tableRow.get("name"));
    assertEquals("1", tableRow.get("number").toString());
    assertFalse(tableRow.containsKey("score"));
  }

  @Test
  public void testFilterTableRowBySchema_nestedAndRepeatedRecords() {
    TableSchema nestedSchema =
        new TableSchema()
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName("id").setType("INTEGER"),
                    new TableFieldSchema()
                        .setName("details")
                        .setType("RECORD")
                        .setFields(
                            Collections.singletonList(
                                new TableFieldSchema().setName("keep_field").setType("STRING"))),
                    new TableFieldSchema()
                        .setName("items")
                        .setType("RECORD")
                        .setMode("REPEATED")
                        .setFields(
                            Collections.singletonList(
                                new TableFieldSchema().setName("item_id").setType("INTEGER")))));

    TableRow rawDetails = new TableRow().set("keep_field", "kept").set("drop_field", "dropped");
    TableRow item1 = new TableRow().set("item_id", 10).set("extra_item_field", "dropped");
    TableRow item2 = new TableRow().set("item_id", 20).set("extra_item_field", "dropped");

    TableRow rawRow =
        new TableRow()
            .set("id", 1)
            .set("extra_top_field", "dropped")
            .set("details", rawDetails)
            .set("items", Arrays.asList(item1, item2));

    TableRow filtered =
        PortableBigQueryDestinations.filterTableRowBySchema(rawRow, nestedSchema.getFields());

    assertEquals(1, filtered.get("id"));
    assertFalse(filtered.containsKey("extra_top_field"));

    TableRow filteredDetails = (TableRow) filtered.get("details");
    assertNotNull(filteredDetails);
    assertEquals("kept", filteredDetails.get("keep_field"));
    assertFalse(filteredDetails.containsKey("drop_field"));

    @SuppressWarnings("unchecked")
    List<TableRow> filteredItems = (List<TableRow>) filtered.get("items");
    assertEquals(2, filteredItems.size());
    assertEquals(10, filteredItems.get(0).get("item_id"));
    assertFalse(filteredItems.get(0).containsKey("extra_item_field"));
    assertEquals(20, filteredItems.get(1).get("item_id"));
    assertFalse(filteredItems.get(1).containsKey("extra_item_field"));
  }

  @Test
  public void testDestinationCoder() throws Exception {
    BigQueryWriteConfiguration config =
        BigQueryWriteConfiguration.builder()
            .setTable(BigQueryWriteConfiguration.DYNAMIC_DESTINATIONS)
            .build();
    PortableBigQueryDestinations destinations =
        new PortableBigQueryDestinations(UNION_RECORD_SCHEMA, config);

    Coder<KV<String, String>> coder = destinations.getDestinationCoder();
    assertNotNull(coder);
    coder.verifyDeterministic();

    KV<String, String> withSchema = KV.of("proj:ds.table1", SCHEMA_JSON_1);
    KV<String, String> withNullSchema = KV.of("proj:ds.table2", null);
    KV<String, String> withEmptySchema = KV.of("proj:ds.table3", "");

    CoderProperties.coderDecodeEncodeEqual(coder, withSchema);
    CoderProperties.coderDecodeEncodeEqual(coder, withNullSchema);
    CoderProperties.coderDecodeEncodeEqual(coder, withEmptySchema);
    CoderProperties.coderDeterministic(coder, withSchema, withSchema);
    CoderProperties.coderDeterministic(coder, withNullSchema, withNullSchema);
  }
}
