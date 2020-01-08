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
package org.apache.beam.sdk.schemas.logicaltypes;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType.Value;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

/** Unit tests for logical types. */
public class LogicalTypesTest {
  @Test
  public void testEnumeration() {
    Map<String, Integer> enumMap = ImmutableMap.of("FIRST", 1, "SECOND", 2);
    EnumerationType enumeration = EnumerationType.create(enumMap);
    assertEquals(enumeration.valueOf(1), enumeration.valueOf("FIRST"));
    assertEquals(enumeration.valueOf(2), enumeration.valueOf("SECOND"));
    assertEquals("FIRST", enumeration.valueOf(1).toString());
    assertEquals(1, enumeration.valueOf("FIRST").getValue());
    assertEquals("SECOND", enumeration.valueOf(2).toString());
    assertEquals(2, enumeration.valueOf("SECOND").getValue());

    Schema schema =
        Schema.builder().addLogicalTypeField("enum", EnumerationType.create(enumMap)).build();
    Row row1 = Row.withSchema(schema).addValue(enumeration.valueOf(1)).build();
    Row row2 = Row.withSchema(schema).addValue(enumeration.valueOf("FIRST")).build();
    assertEquals(row1, row2);
    assertEquals(1, row1.getLogicalTypeValue(0, EnumerationType.Value.class).getValue());

    Row row3 = Row.withSchema(schema).addValue(enumeration.valueOf(2)).build();
    Row row4 = Row.withSchema(schema).addValue(enumeration.valueOf("SECOND")).build();
    assertEquals(row3, row4);
    assertEquals(2, row3.getLogicalTypeValue(0, EnumerationType.Value.class).getValue());
  }

  @Test
  public void testOneOf() {
    OneOfType oneOf =
        OneOfType.create(Field.of("string", FieldType.STRING), Field.of("int32", FieldType.INT32));
    Schema schema = Schema.builder().addLogicalTypeField("union", oneOf).build();

    Row stringOneOf =
        Row.withSchema(schema).addValue(oneOf.createValue("string", "stringValue")).build();
    Value union = stringOneOf.getLogicalTypeValue(0, OneOfType.Value.class);
    assertEquals("string", union.getCaseType().toString());
    assertEquals("stringValue", union.getValue());

    Row intOneOf = Row.withSchema(schema).addValue(oneOf.createValue("int32", 42)).build();
    union = intOneOf.getLogicalTypeValue(0, OneOfType.Value.class);
    assertEquals("int32", union.getCaseType().toString());
    assertEquals(42, (int) union.getValue());
  }
}
