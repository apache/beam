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
package org.apache.beam.sdk.schemas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

/** Tests for {@link org.apache.beam.sdk.schemas.SchemaUtils}. */
public class SchemaUtilsTest {
  @Test
  public void testWidenPrimitives() {
    Schema schema1 =
        Schema.builder()
            .addField("field1", FieldType.INT32)
            .addNullableField("field2", FieldType.STRING)
            .build();
    Schema schema2 =
        Schema.builder()
            .addNullableField("field3", FieldType.INT32)
            .addField("field4", FieldType.STRING)
            .build();
    Schema expected =
        Schema.builder()
            .addNullableField("field1", FieldType.INT32)
            .addNullableField("field2", FieldType.STRING)
            .build();
    assertEquals(expected, SchemaUtils.mergeWideningNullable(schema1, schema2));
  }

  @Test
  public void testWidenNested() {
    Schema schema1 =
        Schema.builder()
            .addField("field1", FieldType.INT32)
            .addNullableField("field2", FieldType.STRING)
            .build();
    Schema schema2 =
        Schema.builder()
            .addNullableField("field3", FieldType.INT32)
            .addField("field4", FieldType.STRING)
            .build();
    Schema top1 = Schema.builder().addField("top1", FieldType.row(schema1)).build();
    Schema top2 = Schema.builder().addField("top2", FieldType.row(schema2)).build();
    Schema expected =
        Schema.builder()
            .addNullableField("field1", FieldType.INT32)
            .addNullableField("field2", FieldType.STRING)
            .build();
    Schema expectedTop = Schema.builder().addField("top1", FieldType.row(expected)).build();

    assertEquals(expectedTop, SchemaUtils.mergeWideningNullable(top1, top2));
  }

  @Test
  public void testWidenArray() {
    Schema schema1 = Schema.builder().addArrayField("field1", FieldType.INT32).build();
    Schema schema2 =
        Schema.builder().addArrayField("field1", FieldType.INT32.withNullable(true)).build();
    Schema expected =
        Schema.builder().addArrayField("field1", FieldType.INT32.withNullable(true)).build();
    assertEquals(expected, SchemaUtils.mergeWideningNullable(schema1, schema2));
  }

  @Test
  public void testWidenIterable() {
    Schema schema1 = Schema.builder().addIterableField("field1", FieldType.INT32).build();
    Schema schema2 =
        Schema.builder().addIterableField("field1", FieldType.INT32.withNullable(true)).build();
    Schema expected =
        Schema.builder().addIterableField("field1", FieldType.INT32.withNullable(true)).build();
    assertEquals(expected, SchemaUtils.mergeWideningNullable(schema1, schema2));
  }

  @Test
  public void testWidenMap() {
    Schema schema1 =
        Schema.builder().addMapField("field1", FieldType.INT32, FieldType.INT32).build();
    Schema schema2 =
        Schema.builder()
            .addMapField(
                "field1", FieldType.INT32.withNullable(true), FieldType.INT32.withNullable(true))
            .build();
    Schema expected =
        Schema.builder()
            .addMapField(
                "field1", FieldType.INT32.withNullable(true), FieldType.INT32.withNullable(true))
            .build();
    assertEquals(expected, SchemaUtils.mergeWideningNullable(schema1, schema2));
  }

  @Test
  public void testToPrettyStringRendersNullInsideArray() {
    Schema schema =
        Schema.builder().addArrayField("a", FieldType.STRING.withNullable(true)).build();
    Row row = Row.withSchema(schema).addValue(Arrays.asList("x", null)).build();
    assertTrue(row.toString(), row.toString().contains("null"));
  }

  @Test
  public void testToPrettyStringRendersNullMapValue() {
    Schema schema =
        Schema.builder()
            .addMapField("m", FieldType.STRING, FieldType.STRING.withNullable(true))
            .build();
    Map<String, String> map = new HashMap<>();
    map.put("k", null);
    Row row = Row.withSchema(schema).addValue(map).build();
    assertTrue(row.toString(), row.toString().contains("null"));
  }

  @Test
  public void testToPrettyStringRendersNullRowInsideArray() {
    Schema inner = Schema.builder().addStringField("s").build();
    Schema schema =
        Schema.builder().addArrayField("a", FieldType.row(inner).withNullable(true)).build();
    Row row = Row.withSchema(schema).addValue(Arrays.asList((Row) null)).build();
    assertTrue(row.toString(), row.toString().contains("null"));
  }

  @Test
  public void testToPrettyStringRendersNullInsideArrayOfInts() {
    Schema schema = Schema.builder().addArrayField("a", FieldType.INT32.withNullable(true)).build();
    Row row = Row.withSchema(schema).addValue(Arrays.asList(1, null)).build();
    String rendered = row.toString();
    assertTrue(rendered, rendered.contains("1"));
    assertTrue(rendered, rendered.contains("null"));
  }

  @Test
  public void testToPrettyStringRendersIterableThatIsNotAList() {
    Schema schema =
        Schema.builder().addStringField("k").addIterableField("vals", FieldType.STRING).build();
    // A bare Iterable, which is exactly what an ITERABLE field declares.
    Iterable<String> bare = () -> Arrays.asList("p", "q").iterator();
    Row row = Row.withSchema(schema).attachValues("k1", bare);

    // The precondition that makes this test meaningful: if the value ever starts arriving as a
    // List, the assertion below would pass for the wrong reason.
    assertTrue(!(row.getValue("vals") instanceof java.util.List));

    String rendered = row.toString();
    assertTrue(rendered, rendered.contains("p"));
    assertTrue(rendered, rendered.contains("q"));
  }

  @Test
  public void testToPrettyStringStillRendersAListArray() {
    // Control: the ordinary List case must be unchanged.
    Schema schema = Schema.builder().addArrayField("vals", FieldType.STRING).build();
    Row row = Row.withSchema(schema).attachValues((Object) Arrays.asList("p", "q"));

    String rendered = row.toString();
    assertTrue(rendered, rendered.contains("p"));
    assertTrue(rendered, rendered.contains("q"));
  }

  @Test
  public void testToPrettyStringRendersAnIterableThatCanOnlyBeReadOnce() {
    Schema schema =
        Schema.builder().addStringField("k").addIterableField("vals", FieldType.STRING).build();
    // Hands out its iterator exactly once and fails loudly on a second attempt. Rendering asks
    // the collection for one traversal now; asking for isEmpty(), then size(), then iterating
    // would trip the guard below.
    Iterable<String> onceOnly =
        new Iterable<String>() {
          private boolean taken = false;

          @Override
          public Iterator<String> iterator() {
            if (taken) {
              throw new IllegalStateException("iterated more than once");
            }
            taken = true;
            return Arrays.asList("p", "q").iterator();
          }
        };
    Row row = Row.withSchema(schema).attachValues("k1", onceOnly);

    String rendered = row.toString();
    assertTrue(rendered, rendered.contains("p"));
    assertTrue(rendered, rendered.contains("q"));
  }

  @Test
  public void testToPrettyStringRendersAnEmptyIterableAsEmptyBrackets() {
    Schema schema = Schema.builder().addIterableField("vals", FieldType.STRING).build();
    Iterable<String> empty = Collections::emptyIterator;
    Row row = Row.withSchema(schema).attachValues((Object) empty);

    String rendered = row.toString();
    assertTrue(rendered, rendered.contains("[]"));
  }
}
