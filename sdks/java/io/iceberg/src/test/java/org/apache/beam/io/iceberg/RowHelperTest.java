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
package org.apache.beam.io.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RowHelperTest implements Serializable {

  /**
   * Checks a value that when converted to Iceberg type is the same value when interpreted in Java.
   */
  private void checkTypeConversion(Schema.FieldType sourceType, Type destType, Object value) {
    checkTypeConversion(sourceType, value, destType, value);
  }

  private void checkTypeConversion(
      Schema.FieldType sourceType, Object sourceValue, Type destType, Object destValue) {
    Schema beamSchema = Schema.of(Schema.Field.of("v", sourceType));
    Row row = Row.withSchema(beamSchema).addValues(sourceValue).build();

    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(required(0, "v", destType));
    Record record = RowHelper.rowToRecord(icebergSchema, row);

    assertThat(record.getField("v"), equalTo(destValue));
  }

  @Test
  public void testBoolean() throws Exception {
    checkTypeConversion(Schema.FieldType.BOOLEAN, Types.BooleanType.get(), true);
    checkTypeConversion(Schema.FieldType.BOOLEAN, Types.BooleanType.get(), false);
  }

  @Test
  public void testInteger() throws Exception {
    checkTypeConversion(Schema.FieldType.INT32, Types.IntegerType.get(), -13);
    checkTypeConversion(Schema.FieldType.INT32, Types.IntegerType.get(), 42);
    checkTypeConversion(Schema.FieldType.INT32, Types.IntegerType.get(), 0);
  }

  @Test
  public void testLong() throws Exception {
    checkTypeConversion(Schema.FieldType.INT64, Types.LongType.get(), 13L);
    checkTypeConversion(Schema.FieldType.INT64, Types.LongType.get(), 42L);
  }

  @Test
  public void testFloat() throws Exception {
    checkTypeConversion(Schema.FieldType.FLOAT, Types.FloatType.get(), 3.14159f);
    checkTypeConversion(Schema.FieldType.FLOAT, Types.FloatType.get(), 42.0f);
  }

  @Test
  public void testDouble() throws Exception {
    checkTypeConversion(Schema.FieldType.DOUBLE, Types.DoubleType.get(), 3.14159);
  }

  @Test
  public void testDate() throws Exception {}

  @Test
  public void testTime() throws Exception {}

  @Test
  public void testTimestamp() throws Exception {
    DateTime dateTime =
        new DateTime().withDate(1979, 03, 14).withTime(1, 2, 3, 4).withZone(DateTimeZone.UTC);

    checkTypeConversion(
        Schema.FieldType.DATETIME,
        dateTime.toInstant(),
        Types.TimestampType.withoutZone(),
        dateTime.getMillis());
  }

  @Test
  public void testFixed() throws Exception {}

  @Test
  public void testBinary() throws Exception {
    byte[] bytes = new byte[] {1, 2, 3, 4};
    checkTypeConversion(
        Schema.FieldType.BYTES, bytes, Types.BinaryType.get(), ByteBuffer.wrap(bytes));
  }

  @Test
  public void testDecimal() throws Exception {}

  @Test
  public void testStruct() throws Exception {}

  @Test
  public void testMap() throws Exception {}

  @Test
  public void testList() throws Exception {}
}
