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
package org.apache.beam.sdk.io.singlestore;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test DefaultUserDataMapper. */
@RunWith(JUnit4.class)
public class SingleStoreDefaultUserDataMapperTest {
  @Test
  public void testNullValues() {}

  @Test
  public void testBigNumbers() {}

  @Test
  public void testBigNegativeNumbers() {
    Schema.Builder schemaBuilder = new Schema.Builder();
    schemaBuilder.addField("byte", Schema.FieldType.BYTE);
    schemaBuilder.addField("int16", Schema.FieldType.INT16);
    schemaBuilder.addField("int32", Schema.FieldType.INT32);
    schemaBuilder.addField("int64", Schema.FieldType.INT64);
    schemaBuilder.addField("float", Schema.FieldType.FLOAT);
    schemaBuilder.addField("double", Schema.FieldType.DOUBLE);
    schemaBuilder.addField("decimal", Schema.FieldType.DECIMAL);
    Schema schema = schemaBuilder.build();

    Row.Builder rowBuilder = Row.withSchema(schema);
    rowBuilder.addValue(Byte.MIN_VALUE);
    rowBuilder.addValue(Short.MIN_VALUE);
    rowBuilder.addValue(Integer.MIN_VALUE);
    rowBuilder.addValue(Long.MIN_VALUE);
    rowBuilder.addValue(-Float.MAX_VALUE);
    rowBuilder.addValue(-Double.MAX_VALUE);
    rowBuilder.addValue(new BigDecimal("-10000000000000.1000000000000000000000"));
    Row row = rowBuilder.build();

    SingleStoreDefaultUserDataMapper mapper = new SingleStoreDefaultUserDataMapper();
    List<String> res = mapper.mapRow(row);

    assertEquals(7, res.size());
    assertEquals("-128", res.get(0));
    assertEquals("-32768", res.get(1));
    assertEquals("-2147483648", res.get(2));
    assertEquals("-9223372036854775808", res.get(3));
    assertEquals("-3.4028235E38", res.get(4));
    assertEquals("-1.7976931348623157E308", res.get(5));
    assertEquals("-10000000000000.1000000000000000000000", res.get(6));
  }

  @Test
  public void testEmptyRow() {
    Schema.Builder schemaBuilder = new Schema.Builder();
    Schema schema = schemaBuilder.build();

    Row.Builder rowBuilder = Row.withSchema(schema);
    Row row = rowBuilder.build();

    SingleStoreDefaultUserDataMapper mapper = new SingleStoreDefaultUserDataMapper();
    List<String> res = mapper.mapRow(row);

    assertEquals(0, res.size());
  }

  @Test
  public void testAllDataTypes() {
    Schema.Builder schemaBuilder = new Schema.Builder();
    schemaBuilder.addField("byte", Schema.FieldType.BYTE);
    schemaBuilder.addField("int16", Schema.FieldType.INT16);
    schemaBuilder.addField("int32", Schema.FieldType.INT32);
    schemaBuilder.addField("int64", Schema.FieldType.INT64);
    schemaBuilder.addField("float", Schema.FieldType.FLOAT);
    schemaBuilder.addField("double", Schema.FieldType.DOUBLE);
    schemaBuilder.addField("decimal", Schema.FieldType.DECIMAL);
    schemaBuilder.addField("boolean", Schema.FieldType.BOOLEAN);
    schemaBuilder.addField("datetime", Schema.FieldType.DATETIME);
    schemaBuilder.addField("bytes", Schema.FieldType.BYTES);
    schemaBuilder.addField("string", Schema.FieldType.STRING);
    Schema schema = schemaBuilder.build();

    Row.Builder rowBuilder = Row.withSchema(schema);
    rowBuilder.addValue((byte) 10);
    rowBuilder.addValue((short) 10);
    rowBuilder.addValue(10);
    rowBuilder.addValue((long) 10);
    rowBuilder.addValue((float) 10.1);
    rowBuilder.addValue(10.1);
    rowBuilder.addValue(new BigDecimal("10.1"));
    rowBuilder.addValue(false);
    rowBuilder.addValue(new DateTime("2022-01-01T10:10:10.012Z"));
    rowBuilder.addValue("asd".getBytes(StandardCharsets.UTF_8));
    rowBuilder.addValue("asd");
    Row row = rowBuilder.build();

    SingleStoreDefaultUserDataMapper mapper = new SingleStoreDefaultUserDataMapper();
    List<String> res = mapper.mapRow(row);

    assertEquals(11, res.size());
    assertEquals("10", res.get(0));
    assertEquals("10", res.get(1));
    assertEquals("10", res.get(2));
    assertEquals("10", res.get(3));
    assertEquals("10.1", res.get(4));
    assertEquals("10.1", res.get(5));
    assertEquals("10.1", res.get(6));
    assertEquals("0", res.get(7));
    assertEquals("2022-01-01 10:10:10.012", res.get(8));
    assertEquals("asd", res.get(9));
    assertEquals("asd", res.get(10));
  }
}
