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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.bson.BsonNull;
import org.bson.Document;
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
}
