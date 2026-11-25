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
package org.apache.beam.io.debezium;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.gson.JsonArray;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.gson.JsonObject;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.gson.JsonParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SourceRecordJsonTest implements Serializable {
  @Test
  public void testSourceRecordJson() {
    SourceRecord record = buildSourceRecord();
    SourceRecordJson json = new SourceRecordJson(record);

    String jsonString = json.toJson();

    String expectedJson =
        "{\"metadata\":"
            + "{\"connector\":\"test-connector\",\"version\":\"version-connector\","
            + "\"name\":\"test-connector-sql\","
            + "\"database\":\"test-db\",\"schema\":\"test-schema\",\"table\":\"test-table\"},"
            + "\"before\":{\"fields\":{\"country\":null,\"distance\":123.423,\"birthYear\":null,"
            + "\"name\":\"before-name\","
            + "\"temperature\":104.4,\"childrenAndAge\":null,\"age\":16}},"
            + "\"after\":{\"fields\":{\"country\":null,\"distance\":123.423,\"birthYear\":null,"
            + "\"name\":\"after-name\","
            + "\"temperature\":104.4,\"childrenAndAge\":null,\"age\":16}}}";

    assertEquals(expectedJson, jsonString);
  }

  @Test
  public void testSourceRecordJsonWhenSourceRecordIsNull() {
    assertThrows(IllegalArgumentException.class, () -> new SourceRecordJson(null));
  }

  @Test
  public void testFullMetadata() {
    // MySQL has no "schema" source field, so the binlog "file" is also exposed as "schema".
    Schema mySqlSchema =
        sourceSchemaBuilder()
            .field("gtid", Schema.OPTIONAL_STRING_SCHEMA)
            .field("file", Schema.STRING_SCHEMA)
            .field("pos", Schema.INT64_SCHEMA)
            .field("row", Schema.INT32_SCHEMA)
            .build();
    Struct mySql =
        sourceStruct(mySqlSchema, "mysql", "inventory")
            .put("file", "mysql-bin.000003")
            .put("pos", 796L)
            .put("row", 0);

    Schema transactionSchema =
        SchemaBuilder.struct()
            .optional()
            .name("event.block")
            .field("id", Schema.STRING_SCHEMA)
            .field("total_order", Schema.INT64_SCHEMA)
            .field("data_collection_order", Schema.INT64_SCHEMA)
            .build();
    Struct transaction =
        new Struct(transactionSchema)
            .put("id", "796:48630784")
            .put("total_order", 1L)
            .put("data_collection_order", 1L);
    Schema rowSchema = SchemaBuilder.struct().optional().field("id", Schema.INT32_SCHEMA).build();
    Struct source = postgresSource();
    Schema envelopeSchema =
        SchemaBuilder.struct()
            .field("before", rowSchema)
            .field("after", rowSchema)
            .field("source", source.schema())
            .field("op", Schema.STRING_SCHEMA)
            .field("transaction", transactionSchema)
            .build();
    Struct envelope =
        new Struct(envelopeSchema)
            .put("after", new Struct(rowSchema).put("id", 1001))
            .put("source", source)
            .put("op", "c")
            .put("transaction", transaction);
    SourceRecord transactionRecord = new SourceRecord(null, null, null, envelopeSchema, envelope);

    // MySQL has no "schema" source field, so the binlog "file" is also exposed as "schema".
    assertEquals(
        "{\"metadata\":{\"connector\":\"mysql\",\"version\":\"3.5.2.Final\",\"name\":\"n\","
            + "\"database\":\"inventory\",\"schema\":\"mysql-bin.000003\",\"table\":\"customers\","
            + "\"op\":\"c\",\"ts_ms\":1791689147000,\"gtid\":null,\"file\":\"mysql-bin.000003\","
            + "\"pos\":796,\"row\":0},\"before\":null,\"after\":{\"fields\":{\"id\":1001}}}",
        new SourceRecordJson(buildEnvelopeRecord(mySql, "c"), true).toJson());

    // PostgreSQL "schema" is a source field: it stays the existing key and isn't repeated.
    assertEquals(
        "{\"metadata\":{\"connector\":\"postgresql\",\"version\":\"3.5.2.Final\",\"name\":\"n\","
            + "\"database\":\"postgres\",\"schema\":\"inventory\",\"table\":\"customers\","
            + "\"op\":\"u\",\"ts_ms\":1791689147000,\"lsn\":37094192},"
            + "\"before\":null,\"after\":{\"fields\":{\"id\":1001}}}",
        new SourceRecordJson(buildEnvelopeRecord(postgresSource(), "u"), true).toJson());

    // With provide.transaction.metadata, the envelope's transaction block is included.
    assertEquals(
        "{\"metadata\":{\"connector\":\"postgresql\",\"version\":\"3.5.2.Final\",\"name\":\"n\","
            + "\"database\":\"postgres\",\"schema\":\"inventory\",\"table\":\"customers\","
            + "\"op\":\"c\",\"ts_ms\":1791689147000,\"lsn\":37094192},"
            + "\"before\":null,\"after\":{\"fields\":{\"id\":1001}},"
            + "\"transaction\":{\"id\":\"796:48630784\",\"total_order\":1,"
            + "\"data_collection_order\":1}}",
        new SourceRecordJson(transactionRecord, true).toJson());
  }

  @Test
  public void testTransactionMetadataEvent() {
    Schema keySchema =
        SchemaBuilder.struct()
            .name("io.debezium.connector.common.TransactionMetadataKey")
            .field("id", Schema.STRING_SCHEMA)
            .build();
    Schema dataCollectionSchema =
        SchemaBuilder.struct()
            .field("data_collection", Schema.STRING_SCHEMA)
            .field("event_count", Schema.INT64_SCHEMA)
            .build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .name("io.debezium.connector.common.TransactionMetadataValue")
            .field("status", Schema.STRING_SCHEMA)
            .field("id", Schema.STRING_SCHEMA)
            .field("event_count", Schema.OPTIONAL_INT64_SCHEMA)
            .field("data_collections", SchemaBuilder.array(dataCollectionSchema).optional().build())
            .field("ts_ms", Schema.INT64_SCHEMA)
            .build();
    Struct key = new Struct(keySchema).put("id", "781:39950456");

    Struct begin =
        new Struct(valueSchema)
            .put("status", "BEGIN")
            .put("id", "781:39950456")
            .put("ts_ms", 1792926401751L);
    assertEquals(
        "{\"status\":\"BEGIN\",\"id\":\"781:39950456\",\"ts_ms\":1792926401751,"
            + "\"event_count\":null,\"data_collections\":null}",
        new SourceRecordJson(new SourceRecord(null, null, null, keySchema, key, valueSchema, begin))
            .toJson());

    Struct end =
        new Struct(valueSchema)
            .put("status", "END")
            .put("id", "781:39950456")
            .put("event_count", 3L)
            .put(
                "data_collections",
                Collections.singletonList(
                    new Struct(dataCollectionSchema)
                        .put("data_collection", "inventory.customers")
                        .put("event_count", 3L)))
            .put("ts_ms", 1792926401760L);
    assertEquals(
        "{\"status\":\"END\",\"id\":\"781:39950456\",\"ts_ms\":1792926401760,"
            + "\"event_count\":3,\"data_collections\":"
            + "[{\"data_collection\":\"inventory.customers\",\"event_count\":3}]}",
        new SourceRecordJson(
                new SourceRecord(null, null, null, keySchema, key, valueSchema, end), true)
            .toJson());
  }

  @Test
  public void testTransactionWithMissingFields() {
    Schema transactionSchema =
        SchemaBuilder.struct().optional().field("id", Schema.STRING_SCHEMA).build();
    Schema rowSchema = SchemaBuilder.struct().optional().field("id", Schema.INT32_SCHEMA).build();
    Struct source = postgresSource();
    Schema envelopeSchema =
        SchemaBuilder.struct()
            .field("before", rowSchema)
            .field("after", rowSchema)
            .field("source", source.schema())
            .field("transaction", transactionSchema)
            .build();
    Struct envelope =
        new Struct(envelopeSchema)
            .put("after", new Struct(rowSchema).put("id", 1001))
            .put("source", source)
            .put("transaction", new Struct(transactionSchema).put("id", "796:48630784"));
    SourceRecord record = new SourceRecord(null, null, null, envelopeSchema, envelope);

    assertEquals(
        "{\"metadata\":{\"connector\":\"postgresql\",\"version\":\"3.5.2.Final\",\"name\":\"n\","
            + "\"database\":\"postgres\",\"schema\":\"inventory\",\"table\":\"customers\"},"
            + "\"before\":null,\"after\":{\"fields\":{\"id\":1001}},"
            + "\"transaction\":{\"id\":\"796:48630784\",\"total_order\":null,"
            + "\"data_collection_order\":null}}",
        new SourceRecordJson(record).toJson());
  }

  @Test
  public void testTransactionMetadataEventWithMissingFields() {
    Schema keySchema =
        SchemaBuilder.struct()
            .name("io.debezium.connector.common.TransactionMetadataKey")
            .field("id", Schema.STRING_SCHEMA)
            .build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .name("io.debezium.connector.common.TransactionMetadataValue")
            .field("status", Schema.STRING_SCHEMA)
            .field("id", Schema.STRING_SCHEMA)
            .field("event_count", Schema.OPTIONAL_INT64_SCHEMA)
            .build();
    Struct key = new Struct(keySchema).put("id", "781:39950456");
    Struct begin = new Struct(valueSchema).put("status", "BEGIN").put("id", "781:39950456");

    assertEquals(
        "{\"status\":\"BEGIN\",\"id\":\"781:39950456\",\"ts_ms\":null,"
            + "\"event_count\":null,\"data_collections\":null}",
        new SourceRecordJson(new SourceRecord(null, null, null, keySchema, key, valueSchema, begin))
            .toJson());
  }

  @Test
  public void testFullMetadataWithoutOpField() {
    String json = new SourceRecordJson(buildEnvelopeRecord(postgresSource(), null), true).toJson();
    assertFalse(json.contains("\"op\""));
    assertTrue(json.contains("\"lsn\":37094192"));
  }

  @Test
  public void testSchemaChangeEventDisabledKeepsDataChangeShape() throws Exception {
    SourceRecord record =
        buildSchemaChangeRecord(
            "mysql", null, "DROP TABLE inventory.orders", Collections.singletonList(dropOrders()));

    String expectedJson =
        "{\"metadata\":{\"connector\":\"mysql\",\"version\":\"3.5.2.Final\","
            + "\"name\":\"n\",\"database\":\"inventory\",\"schema\":\"mysql-bin.000003\","
            + "\"table\":\"customers\"},\"before\":null,\"after\":null}";

    assertEquals(expectedJson, new SourceRecordJson(record).toJson());
    assertEquals(expectedJson, new SourceRecordJson(record, false, false).toJson());
    assertEquals(
        expectedJson, new SourceRecordJson.SourceRecordJsonMapper().mapSourceRecord(record));
  }

  @Test
  public void testSchemaChangeEvent() throws Exception {
    String ddl =
        "ALTER TABLE inventory.customers MODIFY COLUMN first_name TEXT "
            + "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL";
    SourceRecord record =
        buildSchemaChangeRecord("mysql", null, ddl, Collections.singletonList(alterCustomers()));

    String expectedJson =
        "{\"metadata\":{\"connector\":\"mysql\",\"version\":\"3.5.2.Final\","
            + "\"name\":\"n\",\"database\":\"inventory\",\"schema\":\"mysql-bin.000003\","
            + "\"table\":\"customers\"},\"before\":null,\"after\":null,"
            + "\"databaseName\":\"inventory\",\"schemaName\":null,\"ddl\":\"ALTER TABLE "
            + "inventory.customers MODIFY COLUMN first_name TEXT CHARACTER SET utf8mb4 COLLATE "
            + "utf8mb4_0900_ai_ci NOT NULL\",\"ts_ms\":1794617055223,\"tableChanges\":["
            + "{\"type\":\"ALTER\",\"id\":\"\\\"inventory\\\".\\\"customers\\\"\","
            + "\"table\":{\"defaultCharsetName\":\"utf8mb4\",\"comment\":null,"
            + "\"primaryKeyColumnNames\":[\"id\"],\"columns\":["
            + "{\"name\":\"id\",\"jdbcType\":4,\"nativeType\":null,\"typeName\":\"INT\","
            + "\"typeExpression\":\"INT\",\"charsetName\":null,\"length\":null,\"scale\":null,"
            + "\"position\":1,\"optional\":false,\"autoIncremented\":true,\"generated\":true,"
            + "\"comment\":null,\"defaultValueExpression\":null,\"enumValues\":null},"
            + "{\"name\":\"first_name\",\"jdbcType\":12,\"nativeType\":null,"
            + "\"typeName\":\"TEXT\",\"typeExpression\":\"TEXT\",\"charsetName\":\"utf8mb4\","
            + "\"length\":null,\"scale\":null,\"position\":2,\"optional\":false,"
            + "\"autoIncremented\":false,\"generated\":false,\"comment\":null,"
            + "\"defaultValueExpression\":null,\"enumValues\":null}]}}]}";

    assertEquals(expectedJson, new SourceRecordJson(record, false, true).toJson());
    assertEquals(
        expectedJson,
        new SourceRecordJson.SourceRecordJsonMapper(false, true).mapSourceRecord(record));
  }

  @Test
  public void testSchemaChangeEventWithFullMetadata() {
    SourceRecord record =
        buildSchemaChangeRecord(
            "mysql", null, "DROP TABLE inventory.orders", Collections.singletonList(dropOrders()));

    JsonObject metadata =
        parse(new SourceRecordJson(record, true, true).toJson()).getAsJsonObject("metadata");

    assertEquals("true", metadata.get("snapshot").getAsString());
    assertEquals("mysql-bin.000003", metadata.get("file").getAsString());
    assertEquals(154L, metadata.get("pos").getAsLong());
  }

  @Test
  public void testSchemaChangeEventDropHasNullTable() {
    SourceRecord record =
        buildSchemaChangeRecord(
            "mysql", null, "DROP TABLE inventory.orders", Collections.singletonList(dropOrders()));

    JsonArray tableChanges =
        parse(new SourceRecordJson(record, false, true).toJson()).getAsJsonArray("tableChanges");

    assertEquals(1, tableChanges.size());
    JsonObject drop = tableChanges.get(0).getAsJsonObject();
    assertEquals("DROP", drop.get("type").getAsString());
    assertEquals("\"inventory\".\"orders\"", drop.get("id").getAsString());
    assertTrue(drop.get("table").isJsonNull());
  }

  @Test
  public void testSchemaChangeEventWithSeveralTableChanges() {
    SourceRecord record =
        buildSchemaChangeRecord(
            "mysql",
            null,
            "DROP TABLE inventory.orders; ALTER TABLE inventory.customers ...",
            Arrays.asList(dropOrders(), alterCustomers()));

    JsonArray tableChanges =
        parse(new SourceRecordJson(record, false, true).toJson()).getAsJsonArray("tableChanges");

    assertEquals(2, tableChanges.size());
    assertEquals("DROP", tableChanges.get(0).getAsJsonObject().get("type").getAsString());
    assertEquals("ALTER", tableChanges.get(1).getAsJsonObject().get("type").getAsString());
    assertEquals(
        2,
        tableChanges
            .get(1)
            .getAsJsonObject()
            .getAsJsonObject("table")
            .getAsJsonArray("columns")
            .size());
  }

  @Test
  public void testSchemaChangeEventWithEmptyTableChanges() {
    // A database level statement, as emitted by the MySQL snapshot.
    SourceRecord record =
        buildSchemaChangeRecord(
            "mysql", null, "CREATE DATABASE `inventory`", Collections.emptyList());

    JsonObject json = parse(new SourceRecordJson(record, false, true).toJson());

    assertEquals("CREATE DATABASE `inventory`", json.get("ddl").getAsString());
    assertEquals(0, json.getAsJsonArray("tableChanges").size());
  }

  @Test
  public void testSchemaChangeEventOtherConnectors() {
    for (String connector : Arrays.asList("oracle", "sqlserver", "db2")) {
      SourceRecord record =
          buildSchemaChangeRecord(
              connector,
              "INVENTORY",
              "DROP TABLE INVENTORY.ORDERS",
              Collections.singletonList(dropOrders()));

      JsonObject json = parse(new SourceRecordJson(record, false, true).toJson());

      assertEquals(connector, "INVENTORY", json.get("schemaName").getAsString());
      assertEquals(connector, "DROP TABLE INVENTORY.ORDERS", json.get("ddl").getAsString());
      assertEquals(connector, 1, json.getAsJsonArray("tableChanges").size());
    }
  }

  private static JsonObject parse(String json) {
    return JsonParser.parseString(json).getAsJsonObject();
  }

  /** Debezium's own schemas for a schema change's {@code tableChanges} entries. */
  private static final Schema CHANGE_SCHEMA =
      SchemaFactory.get().schemaHistoryChangeSchema(SchemaNameAdjuster.NO_OP);

  private static final Schema TABLE_SCHEMA = CHANGE_SCHEMA.field("table").schema();

  private static final Schema COLUMN_SCHEMA = TABLE_SCHEMA.field("columns").schema().valueSchema();

  private static Struct dropOrders() {
    // Debezium only sets the id of a dropped table.
    return new Struct(CHANGE_SCHEMA).put("type", "DROP").put("id", "\"inventory\".\"orders\"");
  }

  private static Struct alterCustomers() {
    Struct idColumn =
        new Struct(COLUMN_SCHEMA)
            .put("name", "id")
            .put("jdbcType", 4)
            .put("typeName", "INT")
            .put("typeExpression", "INT")
            .put("position", 1)
            .put("optional", false)
            .put("autoIncremented", true)
            .put("generated", true);
    Struct firstNameColumn =
        new Struct(COLUMN_SCHEMA)
            .put("name", "first_name")
            .put("jdbcType", 12)
            .put("typeName", "TEXT")
            .put("typeExpression", "TEXT")
            .put("charsetName", "utf8mb4")
            .put("position", 2)
            .put("optional", false)
            .put("autoIncremented", false)
            .put("generated", false);
    Struct table =
        new Struct(TABLE_SCHEMA)
            .put("defaultCharsetName", "utf8mb4")
            .put("primaryKeyColumnNames", Collections.singletonList("id"))
            .put("columns", Arrays.asList(idColumn, firstNameColumn));
    return new Struct(CHANGE_SCHEMA)
        .put("type", "ALTER")
        .put("id", "\"inventory\".\"customers\"")
        .put("table", table);
  }

  /** A MySQL schema change event that alters {@code inventory.customers}. */
  static SourceRecord buildSchemaChangeRecord() {
    return buildSchemaChangeRecord(
        "mysql",
        null,
        "ALTER TABLE inventory.customers ...",
        Collections.singletonList(alterCustomers()));
  }

  /**
   * A Debezium-like schema change event of the given connector, keyed by {@code
   * io.debezium.connector.<connector>.SchemaChangeKey}, with a MySQL-like snapshot source.
   */
  private static SourceRecord buildSchemaChangeRecord(
      String connector, @Nullable String schemaName, String ddl, List<Struct> tableChanges) {
    Schema keySchema =
        SchemaBuilder.struct()
            .name("io.debezium.connector." + connector + ".SchemaChangeKey")
            .field("databaseName", Schema.STRING_SCHEMA)
            .build();
    Schema sourceSchema =
        sourceSchemaBuilder()
            .field("snapshot", Schema.OPTIONAL_STRING_SCHEMA)
            .field("file", Schema.STRING_SCHEMA)
            .field("pos", Schema.INT64_SCHEMA)
            .build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .name("io.debezium.connector." + connector + ".SchemaChangeValue")
            .field("source", sourceSchema)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .field("databaseName", Schema.OPTIONAL_STRING_SCHEMA)
            .field("schemaName", Schema.OPTIONAL_STRING_SCHEMA)
            .field("ddl", Schema.OPTIONAL_STRING_SCHEMA)
            .field("tableChanges", SchemaBuilder.array(CHANGE_SCHEMA).build())
            .build();

    Struct source =
        sourceStruct(sourceSchema, connector, "inventory")
            .put("snapshot", "true")
            .put("file", "mysql-bin.000003")
            .put("pos", 154L);
    Struct key = new Struct(keySchema).put("databaseName", "inventory");
    Struct value =
        new Struct(valueSchema)
            .put("source", source)
            .put("ts_ms", 1794617055223L)
            .put("databaseName", "inventory")
            .put("schemaName", schemaName)
            .put("ddl", ddl)
            .put("tableChanges", tableChanges);
    return new SourceRecord(null, null, null, keySchema, key, valueSchema, value);
  }

  private static SchemaBuilder sourceSchemaBuilder() {
    return SchemaBuilder.struct()
        .field("version", Schema.STRING_SCHEMA)
        .field("connector", Schema.STRING_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("ts_ms", Schema.INT64_SCHEMA)
        .field("db", Schema.STRING_SCHEMA)
        .field("table", Schema.STRING_SCHEMA);
  }

  private static Struct sourceStruct(Schema schema, String connector, String db) {
    return new Struct(schema)
        .put("version", "3.5.2.Final")
        .put("connector", connector)
        .put("name", "n")
        .put("ts_ms", 1791689147000L)
        .put("db", db)
        .put("table", "customers");
  }

  static Struct postgresSource() {
    Schema schema =
        sourceSchemaBuilder()
            .field("schema", Schema.STRING_SCHEMA)
            .field("lsn", Schema.INT64_SCHEMA)
            .build();
    return sourceStruct(schema, "postgresql", "postgres")
        .put("schema", "inventory")
        .put("lsn", 37094192L);
  }

  /** A Debezium-like envelope with {@code before} null, {@code after.id} and an optional op. */
  static SourceRecord buildEnvelopeRecord(Struct source, @Nullable String op) {
    Schema row = SchemaBuilder.struct().optional().field("id", Schema.INT32_SCHEMA).build();
    SchemaBuilder envelope =
        SchemaBuilder.struct()
            .field("before", row)
            .field("after", row)
            .field("source", source.schema());
    Schema schema = (op == null ? envelope : envelope.field("op", Schema.STRING_SCHEMA)).build();
    Struct value =
        new Struct(schema).put("after", new Struct(row).put("id", 1001)).put("source", source);
    if (op != null) {
      value.put("op", op);
    }
    return new SourceRecord(null, null, null, schema, value);
  }

  private static Schema buildSourceSchema() {
    return SchemaBuilder.struct()
        .field("connector", Schema.STRING_SCHEMA)
        .field("version", Schema.STRING_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("db", Schema.STRING_SCHEMA)
        .field("schema", Schema.STRING_SCHEMA)
        .field("table", Schema.STRING_SCHEMA)
        .build();
  }

  public static Schema buildTableSchema() {
    return SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("age", SchemaBuilder.int8().doc("age of the person").build())
        .field("temperature", Schema.FLOAT32_SCHEMA)
        .field("distance", Schema.FLOAT64_SCHEMA)
        .field("birthYear", Schema.OPTIONAL_INT64_SCHEMA)
        .field(
            "country",
            SchemaBuilder.struct()
                .optional()
                .field("name", Schema.STRING_SCHEMA)
                .field("population", Schema.OPTIONAL_INT64_SCHEMA)
                .field("latitude", SchemaBuilder.array(Schema.FLOAT32_SCHEMA).optional())
                .field("longitude", SchemaBuilder.array(Schema.FLOAT32_SCHEMA).optional())
                .build())
        .field(
            "childrenAndAge",
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional())
        .build();
  }

  static SourceRecord buildSourceRecord() {
    final Schema sourceSchema = buildSourceSchema();
    final Schema beforeSchema = buildTableSchema();
    final Schema afterSchema = buildTableSchema();

    final Schema schema =
        SchemaBuilder.struct()
            .name("test")
            .field("source", sourceSchema)
            .field("before", beforeSchema)
            .field("after", afterSchema)
            .build();

    final Struct source = new Struct(sourceSchema);
    final Struct before = new Struct(beforeSchema);
    final Struct after = new Struct(afterSchema);
    final Struct value = new Struct(schema);

    source.put("connector", "test-connector");
    source.put("version", "version-connector");
    source.put("name", "test-connector-sql");
    source.put("db", "test-db");
    source.put("schema", "test-schema");
    source.put("table", "test-table");

    before
        .put("name", "before-name")
        .put("age", (byte) 16)
        .put("temperature", (float) 104.4)
        .put("distance", 123.423);
    after
        .put("name", "after-name")
        .put("age", (byte) 16)
        .put("temperature", (float) 104.4)
        .put("distance", 123.423);

    value.put("source", source);
    value.put("before", before);
    value.put("after", after);

    return new SourceRecord(null, null, null, schema, value);
  }
}
