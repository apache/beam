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
package org.apache.beam.sdk.io.mongodb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.bson.BsonNull;
import org.bson.Document;
import org.bson.types.Binary;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MongoDbUtils}. */
@RunWith(JUnit4.class)
public class MongoDbUtilsTest {

  @Test
  public void testToDocumentWithSimplePrimitives() {
    Schema schema =
        Schema.builder()
            .addStringField("stringField")
            .addInt32Field("intField")
            .addBooleanField("booleanField")
            .addDoubleField("doubleField")
            .build();

    Row row = Row.withSchema(schema).addValues("hello", 42, true, 3.14).build();

    Document doc = MongoDbUtils.toDocument(row);

    assertNotNull(doc);
    assertEquals("hello", doc.get("stringField"));
    assertEquals(42, doc.get("intField"));
    assertEquals(true, doc.get("booleanField"));
    assertEquals(3.14, doc.get("doubleField"));
  }

  @Test
  public void testToDocumentWithNestedRow() {
    Schema nestedSchema =
        Schema.builder().addStringField("nestedString").addInt32Field("nestedInt").build();

    Schema parentSchema =
        Schema.builder()
            .addStringField("parentString")
            .addRowField("nestedRow", nestedSchema)
            .build();

    Row nestedRow = Row.withSchema(nestedSchema).addValues("nestedValue", 100).build();
    Row parentRow = Row.withSchema(parentSchema).addValues("parentValue", nestedRow).build();

    Document doc = MongoDbUtils.toDocument(parentRow);

    assertNotNull(doc);
    assertEquals("parentValue", doc.get("parentString"));

    Object nestedObj = doc.get("nestedRow");
    assertTrue(nestedObj instanceof Document);
    Document nestedDoc = (Document) nestedObj;
    assertEquals("nestedValue", nestedDoc.get("nestedString"));
    assertEquals(100, nestedDoc.get("nestedInt"));
  }

  @Test
  public void testToDocumentWithIterable() {
    Schema schema = Schema.builder().addArrayField("listField", FieldType.STRING).build();

    Row row = Row.withSchema(schema).addValue(Arrays.asList("a", "b", "c")).build();

    Document doc = MongoDbUtils.toDocument(row);

    assertNotNull(doc);
    Object listObj = doc.get("listField");
    assertTrue(listObj instanceof List);
    List<?> list = (List<?>) listObj;
    assertEquals(3, list.size());
    assertEquals("a", list.get(0));
    assertEquals("b", list.get(1));
    assertEquals("c", list.get(2));
  }

  @Test
  public void testToDocumentWithMap() {
    Schema schema =
        Schema.builder().addMapField("mapField", FieldType.STRING, FieldType.INT32).build();

    Map<String, Integer> map = Collections.singletonMap("key", 42);
    Row row = Row.withSchema(schema).addValue(map).build();

    Document doc = MongoDbUtils.toDocument(row);

    assertNotNull(doc);
    Object mapObj = doc.get("mapField");
    assertTrue(mapObj instanceof Document);
    Document mapDoc = (Document) mapObj;
    assertEquals(42, mapDoc.get("key"));
  }

  @Test
  public void testToDocumentWithNullValues() {
    Schema schema = Schema.builder().addNullableField("nullableString", FieldType.STRING).build();

    Row row = Row.withSchema(schema).addValue(null).build();

    Document doc = MongoDbUtils.toDocument(row);

    assertNotNull(doc);
    Object val = doc.get("nullableString");
    assertTrue(val instanceof BsonNull);
  }

  @Test
  public void testToRowWithSimplePrimitives() {
    Schema schema =
        Schema.builder()
            .addStringField("stringField")
            .addInt32Field("intField")
            .addInt64Field("longField")
            .addBooleanField("booleanField")
            .addDoubleField("doubleField")
            .build();

    Document doc =
        new Document()
            .append("stringField", "hello")
            .append("intField", 42)
            .append("longField", 123456789L)
            .append("booleanField", true)
            .append("doubleField", 3.14);

    Row row = MongoDbUtils.toRow(doc, schema);

    assertNotNull(row);
    assertEquals("hello", row.getString("stringField"));
    assertEquals(Integer.valueOf(42), row.getInt32("intField"));
    assertEquals(Long.valueOf(123456789L), row.getInt64("longField"));
    assertEquals(Boolean.TRUE, row.getBoolean("booleanField"));
    assertEquals(Double.valueOf(3.14), row.getDouble("doubleField"));
  }

  @Test
  public void testToRowWithNestedRow() {
    Schema nestedSchema =
        Schema.builder().addStringField("nestedString").addInt32Field("nestedInt").build();

    Schema parentSchema =
        Schema.builder()
            .addStringField("parentString")
            .addRowField("nestedRow", nestedSchema)
            .build();

    Document nestedDoc =
        new Document().append("nestedString", "nestedValue").append("nestedInt", 100);
    Document parentDoc =
        new Document().append("parentString", "parentValue").append("nestedRow", nestedDoc);

    Row row = MongoDbUtils.toRow(parentDoc, parentSchema);

    assertNotNull(row);
    assertEquals("parentValue", row.getString("parentString"));

    Row nestedRow = row.getRow("nestedRow");
    assertNotNull(nestedRow);
    assertEquals("nestedValue", nestedRow.getString("nestedString"));
    assertEquals(Integer.valueOf(100), nestedRow.getInt32("nestedInt"));
  }

  @Test
  public void testToRowWithIterable() {
    Schema schema = Schema.builder().addArrayField("listField", FieldType.STRING).build();

    Document doc = new Document().append("listField", Arrays.asList("a", "b", "c"));

    Row row = MongoDbUtils.toRow(doc, schema);

    assertNotNull(row);
    List<?> list = (List<?>) row.getArray("listField");
    assertEquals(3, list.size());
    assertEquals("a", list.get(0));
    assertEquals("b", list.get(1));
    assertEquals("c", list.get(2));
  }

  @Test
  public void testToRowWithMap() {
    Schema schema =
        Schema.builder().addMapField("mapField", FieldType.STRING, FieldType.INT32).build();

    Document nestedMap = new Document().append("key", 42);
    Document doc = new Document().append("mapField", nestedMap);

    Row row = MongoDbUtils.toRow(doc, schema);

    assertNotNull(row);
    Map<?, ?> map = row.getMap("mapField");
    assertEquals(1, map.size());
    assertEquals(Integer.valueOf(42), map.get("key"));
  }

  @Test
  public void testToRowWithNullValues() {
    Schema schema = Schema.builder().addNullableField("nullableString", FieldType.STRING).build();

    Document doc = new Document().append("nullableString", new BsonNull());

    Row row = MongoDbUtils.toRow(doc, schema);

    assertNotNull(row);
    assertNull(row.getString("nullableString"));
  }

  @Test
  @SuppressWarnings("JavaUtilDate")
  public void testToRowWithDateAndBinary() {
    Schema schema =
        Schema.builder().addDateTimeField("dateField").addByteArrayField("binaryField").build();

    Date now = new Date();
    byte[] bytes = "hello binary".getBytes(StandardCharsets.UTF_8);
    Document doc = new Document().append("dateField", now).append("binaryField", new Binary(bytes));

    Row row = MongoDbUtils.toRow(doc, schema);

    assertNotNull(row);
    assertEquals(new Instant(now.getTime()), row.getDateTime("dateField"));
    assertArrayEquals(bytes, row.getBytes("binaryField"));
  }
}
