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
package org.apache.beam.sdk.extensions.sql.zetasql;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.DateType;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.TimeType;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** TestInput. */
class TestInput {
  public static final FieldType DATE = FieldType.logicalType(new DateType());
  public static final FieldType TIME = FieldType.logicalType(new TimeType());

  public static final TestBoundedTable BASIC_TABLE_ONE =
      TestBoundedTable.of(
              Schema.builder()
                  .addInt64Field("Key")
                  .addStringField("Value")
                  .addDateTimeField("ts")
                  .build())
          .addRows(
              14L,
              "KeyValue234",
              DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:06"),
              15L,
              "KeyValue235",
              DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:07"));

  public static final TestBoundedTable BASIC_TABLE_TWO =
      TestBoundedTable.of(
              Schema.builder()
                  .addInt64Field("RowKey")
                  .addStringField("Value")
                  .addDateTimeField("ts")
                  .build())
          .addRows(
              15L,
              "BigTable235",
              DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:07"),
              16L,
              "BigTable236",
              DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:08"));

  public static final TestBoundedTable BASIC_TABLE_THREE =
      TestBoundedTable.of(Schema.builder().addInt64Field("ColId").addStringField("Value").build())
          .addRows(15L, "Spanner235", 16L, "Spanner236", 17L, "Spanner237");

  public static final TestBoundedTable AGGREGATE_TABLE_ONE =
      TestBoundedTable.of(
              Schema.builder()
                  .addInt64Field("Key")
                  .addInt64Field("Key2")
                  .addInt64Field("f_int_1")
                  .addStringField("f_str_1")
                  .addDoubleField("f_double_1")
                  .build())
          .addRows(1L, 10L, 1L, "1", 1.0)
          .addRows(1L, 11L, 2L, "2", 2.0)
          .addRows(2L, 11L, 3L, "3", 3.0)
          .addRows(2L, 11L, 4L, "4", 4.0)
          .addRows(2L, 12L, 5L, "5", 5.0)
          .addRows(3L, 13L, 6L, "6", 6.0)
          .addRows(3L, 13L, 7L, "7", 7.0);

  public static final TestBoundedTable AGGREGATE_TABLE_TWO =
      TestBoundedTable.of(
              Schema.builder()
                  .addInt64Field("Key")
                  .addInt64Field("Key2")
                  .addInt64Field("f_int_1")
                  .addStringField("f_str_1")
                  .build())
          .addRows(1L, 10L, 1L, "1")
          .addRows(2L, 11L, 3L, "3")
          .addRows(2L, 11L, 4L, "4")
          .addRows(2L, 12L, 5L, "5")
          .addRows(2L, 13L, 6L, "6")
          .addRows(3L, 13L, 7L, "7");

  public static final TestBoundedTable TABLE_ALL_TYPES =
      TestBoundedTable.of(
              Schema.builder()
                  .addInt64Field("row_id")
                  .addBooleanField("bool_col")
                  .addInt64Field("int64_col")
                  .addDoubleField("double_col")
                  .addStringField("str_col")
                  .addByteArrayField("bytes_col")
                  .build())
          .addRows(1L, true, -1L, 0.125d, "1", stringToBytes("1"))
          .addRows(2L, false, -2L, Math.pow(0.1, 324.0), "2", stringToBytes("2"))
          .addRows(3L, true, -3L, 0.375d, "3", stringToBytes("3"))
          .addRows(4L, false, -4L, 0.5d, "4", stringToBytes("4"))
          .addRows(5L, false, -5L, 0.5d, "5", stringToBytes("5"));

  public static final TestBoundedTable TABLE_ALL_TYPES_2 =
      TestBoundedTable.of(
              Schema.builder()
                  .addInt64Field("row_id")
                  .addBooleanField("bool_col")
                  .addInt64Field("int64_col")
                  .addDoubleField("double_col")
                  .addStringField("str_col")
                  .addByteArrayField("bytes_col")
                  .build())
          .addRows(6L, true, -6L, 0.125d, "6", stringToBytes("6"))
          .addRows(7L, false, -7L, Math.pow(0.1, 324.0), "7", stringToBytes("7"))
          .addRows(8L, true, -8L, 0.375d, "8", stringToBytes("8"))
          .addRows(9L, false, -9L, 0.5d, "9", stringToBytes("9"))
          .addRows(10L, false, -10L, 0.5d, "10", stringToBytes("10"));

  public static final TestBoundedTable TIMESTAMP_TABLE_ONE =
      TestBoundedTable.of(Schema.builder().addDateTimeField("ts").addInt64Field("value").build())
          .addRows(
              DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:06"),
              3L,
              DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:07"),
              4L,
              DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:08"),
              6L,
              DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:09"),
              7L);

  public static final TestBoundedTable TIMESTAMP_TABLE_TWO =
      TestBoundedTable.of(Schema.builder().addDateTimeField("ts").addInt64Field("value").build())
          .addRows(
              DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:06"),
              3L,
              DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:07"),
              4L,
              DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:12"),
              6L,
              DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-01 21:26:13"),
              7L);

  public static final TestBoundedTable TIME_TABLE =
      TestBoundedTable.of(
              Schema.builder()
                  .addNullableField("f_date", DATE)
                  .addNullableField("f_time", TIME)
                  .addNullableField("f_timestamp", FieldType.DATETIME)
                  .addNullableField("f_timestamp_with_time_zone", FieldType.DATETIME)
                  .build())
          .addRows(
              DateTimeUtils.parseTimestampWithUTCTimeZone("2018-07-11 00:00:00"),
              DateTimeUtils.parseTimestampWithUTCTimeZone("1970-01-01 12:33:59.348"),
              DateTimeUtils.parseTimestampWithUTCTimeZone("2018-12-20 23:59:59.999"),
              DateTimeUtils.parseTimestampWithTimeZone("2018-12-10 10:38:59-1000"));

  public static final TestBoundedTable TABLE_ALL_NULL =
      TestBoundedTable.of(
              Schema.builder()
                  .addNullableField("primary_key", FieldType.INT64)
                  .addNullableField("bool_val", FieldType.BOOLEAN)
                  .addNullableField("double_val", FieldType.DOUBLE)
                  .addNullableField("int64_val", FieldType.INT64)
                  .addNullableField("str_val", FieldType.STRING)
                  .build())
          .addRows(1L, null, null, null, null);

  public static final Schema TABLE_WITH_STRUCT_ROW_SCHEMA =
      Schema.builder().addInt32Field("struct_col_int").addStringField("struct_col_str").build();

  public static final TestBoundedTable TABLE_WITH_STRUCT =
      TestBoundedTable.of(
              Schema.builder()
                  .addField("id", FieldType.INT64)
                  .addField("struct_col", FieldType.row(TABLE_WITH_STRUCT_ROW_SCHEMA))
                  .build())
          .addRows(
              1L,
              Row.withSchema(TABLE_WITH_STRUCT_ROW_SCHEMA).addValues(16, "row_one").build(),
              2L,
              Row.withSchema(TABLE_WITH_STRUCT_ROW_SCHEMA).addValues(17, "row_two").build());

  public static final TestBoundedTable TABLE_WITH_STRUCT_TIMESTAMP_STRING =
      TestBoundedTable.of(
              Schema.builder()
                  .addField("struct_col", FieldType.row(TABLE_WITH_STRUCT_ROW_SCHEMA))
                  .build())
          .addRows(
              Row.withSchema(TABLE_WITH_STRUCT_ROW_SCHEMA)
                  .addValues(3, "2019-01-15 13:21:03")
                  .build());

  private static final Schema structSchema =
      Schema.builder().addInt64Field("row_id").addStringField("data").build();
  private static final Schema structTableSchema =
      Schema.builder().addRowField("rowCol", structSchema).build();

  public static final TestBoundedTable TABLE_WITH_STRUCT_TWO =
      TestBoundedTable.of(structTableSchema)
          .addRows(Row.withSchema(structSchema).addValues(1L, "data1").build())
          .addRows(Row.withSchema(structSchema).addValues(2L, "data2").build())
          .addRows(Row.withSchema(structSchema).addValues(3L, "data2").build())
          .addRows(Row.withSchema(structSchema).addValues(3L, "data3").build());

  public static final TestBoundedTable TABLE_WITH_ARRAY =
      TestBoundedTable.of(Schema.builder().addArrayField("array_col", FieldType.STRING).build())
          .addRows(Arrays.asList("1", "2", "3"), ImmutableList.of());

  public static final TestBoundedTable TABLE_WITH_ARRAY_FOR_UNNEST =
      TestBoundedTable.of(
              Schema.builder()
                  .addInt64Field("int_col")
                  .addArrayField("int_array_col", FieldType.INT64)
                  .build())
          .addRows(14L, Arrays.asList(14L, 18L))
          .addRows(18L, Arrays.asList(22L, 24L));

  public static final TestBoundedTable TABLE_FOR_CASE_WHEN =
      TestBoundedTable.of(
              Schema.builder().addInt64Field("f_int").addStringField("f_string").build())
          .addRows(1L, "20181018");

  public static final TestBoundedTable TABLE_EMPTY =
      TestBoundedTable.of(Schema.builder().addInt64Field("ColId").addStringField("Value").build());

  private static final Schema TABLE_WTH_MAP_SCHEMA =
      Schema.builder()
          .addMapField("map_field", FieldType.STRING, FieldType.STRING)
          .addRowField("row_field", structSchema)
          .build();
  public static final TestBoundedTable TABLE_WITH_MAP =
      TestBoundedTable.of(TABLE_WTH_MAP_SCHEMA)
          .addRows(
              ImmutableMap.of("MAP_KEY_1", "MAP_VALUE_1"),
              Row.withSchema(structSchema).addValues(1L, "data1").build());

  public static byte[] stringToBytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}
