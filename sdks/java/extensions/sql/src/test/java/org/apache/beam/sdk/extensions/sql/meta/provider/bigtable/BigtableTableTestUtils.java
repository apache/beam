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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigtable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.KEY;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.LABELS;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.TIMESTAMP_MICROS;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.VALUE;
import static org.apache.beam.sdk.io.gcp.testing.BigtableUtils.booleanToByteArray;
import static org.apache.beam.sdk.io.gcp.testing.BigtableUtils.byteString;
import static org.apache.beam.sdk.io.gcp.testing.BigtableUtils.byteStringUtf8;
import static org.apache.beam.sdk.io.gcp.testing.BigtableUtils.doubleToByteArray;
import static org.apache.beam.sdk.io.gcp.testing.BigtableUtils.longToByteArray;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;
import org.checkerframework.checker.nullness.qual.Nullable;

class BigtableTableTestUtils {

  static final String KEY1 = "key1";
  static final String KEY2 = "key2";

  static final String BOOL_COLUMN = "boolColumn";
  static final String LONG_COLUMN = "longColumn";
  static final String STRING_COLUMN = "stringColumn";
  static final String DOUBLE_COLUMN = "doubleColumn";
  static final String FAMILY_TEST = "familyTest";

  static final Schema LONG_COLUMN_SCHEMA =
      Schema.builder()
          .addInt64Field(VALUE)
          .addInt64Field(TIMESTAMP_MICROS)
          .addArrayField(LABELS, Schema.FieldType.STRING)
          .build();

  static final Schema TEST_FAMILY_SCHEMA =
      Schema.builder()
          .addBooleanField(BOOL_COLUMN)
          .addRowField(LONG_COLUMN, LONG_COLUMN_SCHEMA)
          .addArrayField(STRING_COLUMN, Schema.FieldType.STRING)
          .addDoubleField(DOUBLE_COLUMN)
          .build();

  static final Schema TEST_SCHEMA =
      Schema.builder().addStringField(KEY).addRowField(FAMILY_TEST, TEST_FAMILY_SCHEMA).build();

  static final Schema TEST_FLAT_SCHEMA =
      Schema.builder()
          .addStringField(KEY)
          .addBooleanField(BOOL_COLUMN)
          .addInt64Field(LONG_COLUMN)
          .addStringField(STRING_COLUMN)
          .addDoubleField(DOUBLE_COLUMN)
          .build();

  static final long NOW = 5_000_000_000L;
  static final long LATER = NOW + 1_000L;

  static String createFlatTableString(String table, String location) {
    return String.format(
        "CREATE EXTERNAL TABLE `%s`( \n"
            + "  key VARCHAR NOT NULL, \n"
            + "  boolColumn BOOLEAN NOT NULL, \n"
            + "  longColumn BIGINT NOT NULL, \n"
            + "  stringColumn VARCHAR NOT NULL, \n"
            + "  doubleColumn DOUBLE NOT NULL \n"
            + ") \n"
            + "TYPE bigtable \n"
            + "LOCATION '%s' \n"
            + "TBLPROPERTIES '{ \n"
            + "  \"columnsMapping\": \"%s\"}'",
        table, location, columnsMappingString());
  }

  static String createFullTableString(String tableId, String location) {
    return String.format(
        "CREATE EXTERNAL TABLE `%s`( \n"
            + "  key VARCHAR NOT NULL, \n"
            + "  familyTest ROW< \n"
            + "    boolColumn BOOLEAN NOT NULL, \n"
            + "    longColumn ROW< \n"
            + "      val BIGINT NOT NULL, \n"
            + "      timestampMicros BIGINT NOT NULL, \n"
            + "      labels ARRAY<VARCHAR> NOT NULL \n"
            + "    > NOT NULL, \n"
            + "    stringColumn ARRAY<VARCHAR> NOT NULL, \n"
            + "    doubleColumn DOUBLE NOT NULL \n"
            + "  > NOT NULL \n"
            + ") \n"
            + "TYPE bigtable \n"
            + "LOCATION '%s'",
        tableId, location);
  }

  static Schema expectedFullSchema() {
    return Schema.builder()
        .addStringField(KEY)
        .addBooleanField(BOOL_COLUMN)
        .addInt64Field("longValue")
        .addInt64Field(TIMESTAMP_MICROS)
        .addArrayField(LABELS, Schema.FieldType.STRING)
        .addArrayField(STRING_COLUMN, Schema.FieldType.STRING)
        .addDoubleField(DOUBLE_COLUMN)
        .build();
  }

  static Row expectedFullRow(String key) {
    return Row.withSchema(expectedFullSchema())
        .attachValues(
            key,
            false,
            2L,
            LATER,
            ImmutableList.of(),
            ImmutableList.of("string1", "string2"),
            2.20);
  }

  static Row flatRow(String key) {
    return Row.withSchema(TEST_FLAT_SCHEMA).attachValues(key, false, 2L, "string2", 2.20);
  }

  static String location(
      String project, String instanceId, String tableId, @Nullable Integer emulatorPort) {
    String host = emulatorPort == null ? "googleapis.com" : "localhost:" + emulatorPort;
    return String.format(
        "%s/bigtable/projects/%s/instances/%s/tables/%s", host, project, instanceId, tableId);
  }

  static String columnsMappingString() {
    return "familyTest:boolColumn,familyTest:longColumn,familyTest:doubleColumn,"
        + "familyTest:stringColumn";
  }

  static void createReadTable(String table, BigtableClientWrapper clientWrapper) {
    clientWrapper.createTable(table, FAMILY_TEST);
    writeRow(KEY1, table, clientWrapper);
    writeRow(KEY2, table, clientWrapper);
  }

  static com.google.bigtable.v2.Row bigTableRow() {
    List<Column> columns =
        ImmutableList.of(
            column("boolColumn", booleanToByteArray(true)),
            column("doubleColumn", doubleToByteArray(5.5)),
            column("longColumn", Longs.toByteArray(10L)),
            column("stringColumn", "stringValue".getBytes(UTF_8)));
    Family family = Family.newBuilder().setName("familyTest").addAllColumns(columns).build();
    return com.google.bigtable.v2.Row.newBuilder()
        .setKey(byteStringUtf8("key"))
        .addFamilies(family)
        .build();
  }

  static com.google.bigtable.v2.Row[] bigTableSegmentedRows() {
    com.google.bigtable.v2.Row[] rows = new com.google.bigtable.v2.Row[5];
    List<Column> columns =
        ImmutableList.of(
            column("boolColumn", booleanToByteArray(true)),
            column("doubleColumn", doubleToByteArray(5.5)),
            column("longColumn", Longs.toByteArray(10L)),
            column("stringColumn", "stringValue".getBytes(UTF_8)));
    Family family = Family.newBuilder().setName("familyTest").addAllColumns(columns).build();
    for (int i = 0; i < 5; i++) {
      rows[i] =
          com.google.bigtable.v2.Row.newBuilder()
              .setKey(byteStringUtf8("key" + i))
              .addFamilies(family)
              .build();
    }
    return rows;
  }

  // There is no possibility to insert a value with fixed timestamp so we have to replace it
  // for the testing purpose.
  static com.google.bigtable.v2.Row setFixedTimestamp(com.google.bigtable.v2.Row row) {
    Family family = row.getFamilies(0);

    List<Column> columnsReplaced =
        family.getColumnsList().stream()
            .map(
                column -> {
                  Cell cell = column.getCells(0);
                  return column(
                      column.getQualifier().toStringUtf8(), cell.getValue().toByteArray());
                })
            .collect(toList());
    Family familyReplaced =
        Family.newBuilder().setName(family.getName()).addAllColumns(columnsReplaced).build();
    return com.google.bigtable.v2.Row.newBuilder()
        .setKey(row.getKey())
        .addFamilies(familyReplaced)
        .build();
  }

  static void checkMessage(@Nullable String message, String substring) {
    if (message != null) {
      assertThat(message, containsString(substring));
    } else {
      fail();
    }
  }

  private static Column column(String qualifier, byte[] value) {
    return Column.newBuilder()
        .setQualifier(byteStringUtf8(qualifier))
        .addCells(cell(value))
        .build();
  }

  private static Cell cell(byte[] value) {
    return Cell.newBuilder().setValue(byteString(value)).setTimestampMicros(NOW).build();
  }

  private static void writeRow(String key, String table, BigtableClientWrapper clientWrapper) {
    clientWrapper.writeRow(key, table, FAMILY_TEST, BOOL_COLUMN, booleanToByteArray(true), NOW);
    clientWrapper.writeRow(key, table, FAMILY_TEST, BOOL_COLUMN, booleanToByteArray(false), LATER);
    clientWrapper.writeRow(key, table, FAMILY_TEST, STRING_COLUMN, "string1".getBytes(UTF_8), NOW);
    clientWrapper.writeRow(
        key, table, FAMILY_TEST, STRING_COLUMN, "string2".getBytes(UTF_8), LATER);
    clientWrapper.writeRow(key, table, FAMILY_TEST, LONG_COLUMN, longToByteArray(1L), NOW);
    clientWrapper.writeRow(key, table, FAMILY_TEST, LONG_COLUMN, longToByteArray(2L), LATER);
    clientWrapper.writeRow(key, table, FAMILY_TEST, DOUBLE_COLUMN, doubleToByteArray(1.10), NOW);
    clientWrapper.writeRow(key, table, FAMILY_TEST, DOUBLE_COLUMN, doubleToByteArray(2.20), LATER);
  }
}
