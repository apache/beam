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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiDynamicDestinations.MessageConverter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link StorageApiDynamicDestinationsBeamRow}. */
@RunWith(JUnit4.class)
public class StorageApiDynamicDestinationsBeamRowTest {

  private static final Schema UNION_SCHEMA =
      Schema.builder()
          .addField("id", FieldType.INT64)
          .addField("name", FieldType.STRING.withNullable(true))
          .addField("score", FieldType.DOUBLE.withNullable(true))
          .addField("active", FieldType.BOOLEAN.withNullable(true))
          .build();

  private static final Schema SCHEMA_USERS =
      Schema.builder()
          .addField("id", FieldType.INT64)
          .addField("name", FieldType.STRING.withNullable(true))
          .build();

  private static final TableSchema BQ_SCHEMA_USERS =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema().setName("id").setType("INTEGER"),
                  new TableFieldSchema().setName("name").setType("STRING")));

  private static final TableSchema BQ_SCHEMA_SCORES =
      new TableSchema()
          .setFields(
              ImmutableList.of(
                  new TableFieldSchema().setName("id").setType("INTEGER"),
                  new TableFieldSchema().setName("score").setType("FLOAT"),
                  new TableFieldSchema().setName("active").setType("BOOLEAN")));

  private PipelineOptions pipelineOptions;

  @Before
  public void setUp() {
    pipelineOptions = PipelineOptionsFactory.create();
  }

  @After
  public void tearDown() throws Exception {
    StorageApiDynamicDestinationsBeamRow.clearSchemaCache();
  }

  static class FakeDynamicDestinations extends DynamicDestinations<Row, String> {
    private final Map<String, TableSchema> schemas;

    FakeDynamicDestinations(Map<String, TableSchema> schemas) {
      this.schemas = schemas;
    }

    @Override
    public String getDestination(@Nullable ValueInSingleWindow<Row> element) {
      return "";
    }

    @Override
    public TableDestination getTable(String destination) {
      return new TableDestination(destination, null);
    }

    @Override
    public @Nullable TableSchema getSchema(String destination) {
      return schemas.get(destination);
    }
  }

  @Test
  public void testPerDestinationMessageConverterWithInnerSchemas() throws Exception {
    Map<String, TableSchema> schemas = new HashMap<>();
    schemas.put("project:dataset.users", BQ_SCHEMA_USERS);
    schemas.put("project:dataset.scores", BQ_SCHEMA_SCORES);

    FakeDynamicDestinations inner = new FakeDynamicDestinations(schemas);
    StorageApiDynamicDestinationsBeamRow<Row, String> destinations =
        new StorageApiDynamicDestinationsBeamRow<>(inner, UNION_SCHEMA, row -> row, null, false);

    MessageConverter<Row> converterUsers =
        destinations.getMessageConverter("project:dataset.users", pipelineOptions, null, null);
    MessageConverter<Row> converterScores =
        destinations.getMessageConverter("project:dataset.scores", pipelineOptions, null, null);

    assertEquals(2, converterUsers.getTableSchema().getFieldsCount());
    assertEquals(3, converterScores.getTableSchema().getFieldsCount());

    Descriptor descriptorUsers =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            converterUsers.getTableSchema(), true, false);
    Descriptor descriptorScores =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            converterScores.getTableSchema(), true, false);

    assertNotNull(descriptorUsers.findFieldByName("id"));
    assertNotNull(descriptorUsers.findFieldByName("name"));
    assertNull(descriptorUsers.findFieldByName("score"));
    assertNull(descriptorUsers.findFieldByName("active"));

    assertNotNull(descriptorScores.findFieldByName("id"));
    assertNull(descriptorScores.findFieldByName("name"));
    assertNotNull(descriptorScores.findFieldByName("score"));
    assertNotNull(descriptorScores.findFieldByName("active"));

    Row testRow =
        Row.withSchema(UNION_SCHEMA)
            .withFieldValue("id", 1L)
            .withFieldValue("name", "Alice")
            .withFieldValue("score", 95.0)
            .withFieldValue("active", true)
            .build();

    StorageApiWritePayload payloadUsers =
        converterUsers.toMessage(
            testRow, null, TableRowToStorageApiProto.ErrorCollector.DONT_COLLECT);
    DynamicMessage msgUsers = DynamicMessage.parseFrom(descriptorUsers, payloadUsers.getPayload());
    assertEquals(2, msgUsers.getAllFields().size());
    assertEquals(1L, msgUsers.getField(descriptorUsers.findFieldByName("id")));
    assertEquals("Alice", msgUsers.getField(descriptorUsers.findFieldByName("name")));

    StorageApiWritePayload payloadScores =
        converterScores.toMessage(
            testRow, null, TableRowToStorageApiProto.ErrorCollector.DONT_COLLECT);
    DynamicMessage msgScores =
        DynamicMessage.parseFrom(descriptorScores, payloadScores.getPayload());
    assertEquals(3, msgScores.getAllFields().size());
    assertEquals(1L, msgScores.getField(descriptorScores.findFieldByName("id")));
    assertEquals(95.0, msgScores.getField(descriptorScores.findFieldByName("score")));
    assertEquals(true, msgScores.getField(descriptorScores.findFieldByName("active")));
  }

  @Test
  public void testSchemaResolutionFromDatasetService() throws Exception {
    FakeDynamicDestinations inner = new FakeDynamicDestinations(Collections.emptyMap());
    StorageApiDynamicDestinationsBeamRow<Row, String> destinations =
        new StorageApiDynamicDestinationsBeamRow<>(inner, UNION_SCHEMA, row -> row, null, false);

    DatasetService mockDatasetService = mock(DatasetService.class);
    TableReference usersRef = BigQueryHelpers.parseTableSpec("project:dataset.users");
    TableReference scoresRef = BigQueryHelpers.parseTableSpec("project:dataset.scores");

    when(mockDatasetService.getTable(eq(usersRef), any(), any()))
        .thenReturn(new Table().setSchema(BQ_SCHEMA_USERS));
    when(mockDatasetService.getTable(eq(scoresRef), any(), any()))
        .thenReturn(new Table().setSchema(BQ_SCHEMA_SCORES));

    MessageConverter<Row> converterUsers =
        destinations.getMessageConverter(
            "project:dataset.users", pipelineOptions, mockDatasetService, null);
    MessageConverter<Row> converterScores =
        destinations.getMessageConverter(
            "project:dataset.scores", pipelineOptions, mockDatasetService, null);

    assertEquals(2, converterUsers.getTableSchema().getFieldsCount());
    assertEquals(3, converterScores.getTableSchema().getFieldsCount());
  }

  @Test
  public void testFallbackToStaticSchemaWhenResolutionFails() throws Exception {
    FakeDynamicDestinations inner = new FakeDynamicDestinations(Collections.emptyMap());
    StorageApiDynamicDestinationsBeamRow<Row, String> destinations =
        new StorageApiDynamicDestinationsBeamRow<>(inner, SCHEMA_USERS, row -> row, null, false);

    MessageConverter<Row> converter =
        destinations.getMessageConverter("project:dataset.unknown", pipelineOptions, null, null);

    assertEquals(2, converter.getTableSchema().getFieldsCount());
  }

  @Test
  public void testCdcWritesWithDynamicPerDestinationSchemas() throws Exception {
    Map<String, TableSchema> schemas = new HashMap<>();
    schemas.put("project:dataset.users", BQ_SCHEMA_USERS);

    FakeDynamicDestinations inner = new FakeDynamicDestinations(schemas);
    StorageApiDynamicDestinationsBeamRow<Row, String> destinations =
        new StorageApiDynamicDestinationsBeamRow<>(inner, UNION_SCHEMA, row -> row, null, true);

    MessageConverter<Row> converter =
        destinations.getMessageConverter("project:dataset.users", pipelineOptions, null, null);

    DescriptorProtos.DescriptorProto proto = converter.getDescriptor(true);
    assertNotNull(proto);

    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            converter.getTableSchema(), true, true);
    assertNotNull(descriptor.findFieldByName(StorageApiCDC.CHANGE_TYPE_COLUMN));
    assertNotNull(descriptor.findFieldByName(StorageApiCDC.CHANGE_SQN_COLUMN));

    Row testRow =
        Row.withSchema(UNION_SCHEMA)
            .withFieldValue("id", 1L)
            .withFieldValue("name", "Alice")
            .withFieldValue("score", 95.0)
            .withFieldValue("active", true)
            .build();

    RowMutationInformation mutationInfo =
        RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, 42L);

    StorageApiWritePayload payload =
        converter.toMessage(
            testRow, mutationInfo, TableRowToStorageApiProto.ErrorCollector.DONT_COLLECT);
    DynamicMessage msg = DynamicMessage.parseFrom(descriptor, payload.getPayload());

    assertEquals(
        "UPSERT", msg.getField(descriptor.findFieldByName(StorageApiCDC.CHANGE_TYPE_COLUMN)));
    assertEquals(
        Long.toHexString(42L),
        msg.getField(descriptor.findFieldByName(StorageApiCDC.CHANGE_SQN_COLUMN)));
    assertEquals(1L, msg.getField(descriptor.findFieldByName("id")));
    assertEquals("Alice", msg.getField(descriptor.findFieldByName("name")));
  }

  @Test
  public void testDatasetServiceFailureFallsBackGracefully() throws Exception {
    Map<String, TableSchema> schemas = new HashMap<>();
    schemas.put("project:dataset.users", BQ_SCHEMA_USERS);
    FakeDynamicDestinations inner = new FakeDynamicDestinations(schemas);

    StorageApiDynamicDestinationsBeamRow<Row, String> destinations =
        new StorageApiDynamicDestinationsBeamRow<>(inner, UNION_SCHEMA, row -> row, null, false);

    DatasetService mockDatasetService = mock(DatasetService.class);
    TableReference usersRef = BigQueryHelpers.parseTableSpec("project:dataset.users");
    when(mockDatasetService.getTable(eq(usersRef), any(), any()))
        .thenThrow(new IOException("BigQuery quota exceeded"));

    MessageConverter<Row> converter =
        destinations.getMessageConverter(
            "project:dataset.users", pipelineOptions, mockDatasetService, null);

    assertEquals(2, converter.getTableSchema().getFieldsCount());
    Descriptor descriptor =
        TableRowToStorageApiProto.getDescriptorFromTableSchema(
            converter.getTableSchema(), true, false);
    assertNotNull(descriptor.findFieldByName("id"));
    assertNotNull(descriptor.findFieldByName("name"));
  }

  @Test
  public void testToFailsafeTableRow() throws Exception {
    Row testRow =
        Row.withSchema(UNION_SCHEMA)
            .withFieldValue("id", 1L)
            .withFieldValue("name", "Alice")
            .withFieldValue("score", 95.0)
            .withFieldValue("active", true)
            .build();

    FakeDynamicDestinations inner = new FakeDynamicDestinations(Collections.emptyMap());
    StorageApiDynamicDestinationsBeamRow<Row, String> destinationsDefault =
        new StorageApiDynamicDestinationsBeamRow<>(inner, UNION_SCHEMA, row -> row, null, false);

    MessageConverter<Row> converterDefault =
        destinationsDefault.getMessageConverter(
            "project:dataset.users", pipelineOptions, null, null);
    TableRow defaultTableRow = converterDefault.toFailsafeTableRow(testRow);
    assertEquals("1", defaultTableRow.get("id").toString());
    assertEquals("Alice", defaultTableRow.get("name"));

    StorageApiDynamicDestinationsBeamRow<Row, String> destinationsCustom =
        new StorageApiDynamicDestinationsBeamRow<>(
            inner,
            UNION_SCHEMA,
            row -> row,
            (schemaInfo, element) -> new TableRow().set("custom_id", element.getInt64("id")),
            false);

    MessageConverter<Row> converterCustom =
        destinationsCustom.getMessageConverter(
            "project:dataset.users", pipelineOptions, null, null);
    TableRow customTableRow = converterCustom.toFailsafeTableRow(testRow);
    assertEquals(1L, customTableRow.get("custom_id"));
  }
}
