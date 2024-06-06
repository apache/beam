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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(Enclosed.class)
public class SchemaAndRowConversionsTest {

  @RunWith(JUnit4.class)
  public static class RowToRecordTests {
    /**
     * Checks a value that when converted to Iceberg type is the same value when interpreted in
     * Java.
     */
    private void checkRowValueToRecordValue(
        Schema.FieldType sourceType, Type destType, Object value) {
      checkRowValueToRecordValue(sourceType, value, destType, value);
    }

    private void checkRowValueToRecordValue(
        Schema.FieldType sourceType, Object sourceValue, Type destType, Object destValue) {
      Schema beamSchema = Schema.of(Schema.Field.of("v", sourceType));
      Row row = Row.withSchema(beamSchema).addValues(sourceValue).build();

      org.apache.iceberg.Schema icebergSchema =
          new org.apache.iceberg.Schema(required(0, "v", destType));
      Record record = SchemaAndRowConversions.rowToRecord(icebergSchema, row);

      assertThat(record.getField("v"), equalTo(destValue));
    }

    @Test
    public void testBoolean() throws Exception {
      checkRowValueToRecordValue(Schema.FieldType.BOOLEAN, Types.BooleanType.get(), true);
      checkRowValueToRecordValue(Schema.FieldType.BOOLEAN, Types.BooleanType.get(), false);
    }

    @Test
    public void testInteger() throws Exception {
      checkRowValueToRecordValue(Schema.FieldType.INT32, Types.IntegerType.get(), -13);
      checkRowValueToRecordValue(Schema.FieldType.INT32, Types.IntegerType.get(), 42);
      checkRowValueToRecordValue(Schema.FieldType.INT32, Types.IntegerType.get(), 0);
    }

    @Test
    public void testLong() throws Exception {
      checkRowValueToRecordValue(Schema.FieldType.INT64, Types.LongType.get(), 13L);
      checkRowValueToRecordValue(Schema.FieldType.INT64, Types.LongType.get(), 42L);
    }

    @Test
    public void testFloat() throws Exception {
      checkRowValueToRecordValue(Schema.FieldType.FLOAT, Types.FloatType.get(), 3.14159f);
      checkRowValueToRecordValue(Schema.FieldType.FLOAT, Types.FloatType.get(), 42.0f);
    }

    @Test
    public void testDouble() throws Exception {
      checkRowValueToRecordValue(Schema.FieldType.DOUBLE, Types.DoubleType.get(), 3.14159);
    }

    @Test
    public void testDate() throws Exception {}

    @Test
    public void testTime() throws Exception {}

    @Test
    public void testTimestamp() throws Exception {
      DateTime dateTime =
          new DateTime().withDate(1979, 03, 14).withTime(1, 2, 3, 4).withZone(DateTimeZone.UTC);

      checkRowValueToRecordValue(
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
      checkRowValueToRecordValue(
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

  @RunWith(JUnit4.class)
  public static class RecordToRowTests {
    private void checkRecordValueToRowValue(
        Type sourceType, Schema.FieldType destType, Object value) {
      checkRecordValueToRowValue(sourceType, value, destType, value);
    }

    private void checkRecordValueToRowValue(
        Type sourceType, Object sourceValue, Schema.FieldType destType, Object destValue) {
      Schema beamSchema = Schema.of(Schema.Field.of("v", destType));

      org.apache.iceberg.Schema icebergSchema =
          new org.apache.iceberg.Schema(required(0, "v", sourceType));
      Record record = GenericRecord.create(icebergSchema);
      record.setField("v", sourceValue);

      Row row = SchemaAndRowConversions.recordToRow(beamSchema, record);

      assertThat(row.getBaseValue("v"), equalTo(destValue));
    }

    @Test
    public void testBoolean() throws Exception {
      checkRecordValueToRowValue(Types.BooleanType.get(), Schema.FieldType.BOOLEAN, true);
      checkRecordValueToRowValue(Types.BooleanType.get(), Schema.FieldType.BOOLEAN, false);
    }

    @Test
    public void testInteger() throws Exception {
      checkRecordValueToRowValue(Types.IntegerType.get(), Schema.FieldType.INT32, -13);
      checkRecordValueToRowValue(Types.IntegerType.get(), Schema.FieldType.INT32, 42);
      checkRecordValueToRowValue(Types.IntegerType.get(), Schema.FieldType.INT32, 0);
    }

    @Test
    public void testLong() throws Exception {
      checkRecordValueToRowValue(Types.LongType.get(), Schema.FieldType.INT64, 13L);
      checkRecordValueToRowValue(Types.LongType.get(), Schema.FieldType.INT64, 42L);
    }

    @Test
    public void testFloat() throws Exception {
      checkRecordValueToRowValue(Types.FloatType.get(), Schema.FieldType.FLOAT, 3.14159f);
      checkRecordValueToRowValue(Types.FloatType.get(), Schema.FieldType.FLOAT, 42.0f);
    }

    @Test
    public void testDouble() throws Exception {
      checkRecordValueToRowValue(Types.DoubleType.get(), Schema.FieldType.DOUBLE, 3.14159);
    }

    @Test
    public void testDate() throws Exception {}

    @Test
    public void testTime() throws Exception {}

    @Test
    public void testTimestamp() throws Exception {
      DateTime dateTime =
          new DateTime().withDate(1979, 03, 14).withTime(1, 2, 3, 4).withZone(DateTimeZone.UTC);

      checkRecordValueToRowValue(
          Types.TimestampType.withoutZone(),
          dateTime.getMillis(),
          Schema.FieldType.DATETIME,
          dateTime.toInstant());
    }

    @Test
    public void testFixed() throws Exception {}

    @Test
    public void testBinary() throws Exception {
      byte[] bytes = new byte[] {1, 2, 3, 4};
      checkRecordValueToRowValue(
          Types.BinaryType.get(), ByteBuffer.wrap(bytes), Schema.FieldType.BYTES, bytes);
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

  @RunWith(JUnit4.class)
  public static class SchemaTests {
    static final Schema BEAM_SCHEMA =
        Schema.builder()
            .addInt32Field("int")
            .addFloatField("float")
            .addDoubleField("double")
            .addInt64Field("long")
            .addStringField("str")
            .addBooleanField("bool")
            .addByteArrayField("bytes")
            .build();

    static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "int", Types.IntegerType.get()),
            Types.NestedField.required(2, "float", Types.FloatType.get()),
            Types.NestedField.required(3, "double", Types.DoubleType.get()),
            Types.NestedField.required(4, "long", Types.LongType.get()),
            Types.NestedField.required(5, "str", Types.StringType.get()),
            Types.NestedField.required(6, "bool", Types.BooleanType.get()),
            Types.NestedField.required(7, "bytes", Types.BinaryType.get()));

    @Test
    public void testBeamSchemaToIcebergSchema() {
      org.apache.iceberg.Schema convertedIcebergSchema =
          SchemaAndRowConversions.beamSchemaToIcebergSchema(BEAM_SCHEMA);

      assertTrue(convertedIcebergSchema.sameSchema(ICEBERG_SCHEMA));
    }

    @Test
    public void testIcebergSchemaToBeamSchema() {
      Schema convertedBeamSchema =
          SchemaAndRowConversions.icebergSchemaToBeamSchema(ICEBERG_SCHEMA);

      assertEquals(BEAM_SCHEMA, convertedBeamSchema);
    }
  }
}
