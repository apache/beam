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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toTableRow;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.toTableSchema;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions.TruncateTimestamps;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.chrono.ISOChronology;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BigQueryUtils}. */
@RunWith(JUnit4.class)
public class BigQueryUtilsTest {
  private static final Schema FLAT_TYPE =
      Schema.builder()
          .addNullableField("id", Schema.FieldType.INT64)
          .addNullableField("value", Schema.FieldType.DOUBLE)
          .addNullableField("name", Schema.FieldType.STRING)
          .addNullableField("timestamp", Schema.FieldType.DATETIME)
          .addNullableField("valid", Schema.FieldType.BOOLEAN)
          .build();

  private static final Schema ARRAY_TYPE =
      Schema.builder().addArrayField("ids", Schema.FieldType.INT64).build();

  private static final Schema ROW_TYPE =
      Schema.builder().addNullableField("row", Schema.FieldType.row(FLAT_TYPE)).build();

  private static final Schema ARRAY_ROW_TYPE =
      Schema.builder().addArrayField("rows", Schema.FieldType.row(FLAT_TYPE)).build();

  private static final TableFieldSchema ID =
      new TableFieldSchema().setName("id").setType(StandardSQLTypeName.INT64.toString());

  private static final TableFieldSchema VALUE =
      new TableFieldSchema().setName("value").setType(StandardSQLTypeName.FLOAT64.toString());

  private static final TableFieldSchema NAME =
      new TableFieldSchema().setName("name").setType(StandardSQLTypeName.STRING.toString());

  private static final TableFieldSchema TIMESTAMP =
      new TableFieldSchema().setName("timestamp").setType(StandardSQLTypeName.TIMESTAMP.toString());

  private static final TableFieldSchema VALID =
      new TableFieldSchema().setName("valid").setType(StandardSQLTypeName.BOOL.toString());

  private static final TableFieldSchema IDS =
      new TableFieldSchema()
          .setName("ids")
          .setType(StandardSQLTypeName.INT64.toString())
          .setMode(Mode.REPEATED.toString());

  private static final TableFieldSchema ROW =
      new TableFieldSchema()
          .setName("row")
          .setType(StandardSQLTypeName.STRUCT.toString())
          .setMode(Mode.NULLABLE.toString())
          .setFields(Arrays.asList(ID, VALUE, NAME, TIMESTAMP, VALID));

  private static final TableFieldSchema ROWS =
      new TableFieldSchema()
          .setName("rows")
          .setType(StandardSQLTypeName.STRUCT.toString())
          .setMode(Mode.REPEATED.toString())
          .setFields(Arrays.asList(ID, VALUE, NAME, TIMESTAMP, VALID));

  private static final Row FLAT_ROW =
      Row.withSchema(FLAT_TYPE)
          .addValues(123L, 123.456, "test", new DateTime(123456), false)
          .build();

  private static final TableRow BQ_FLAT_ROW =
      new TableRow()
          .set("id", "123")
          .set("value", "123.456")
          .set("name", "test")
          .set(
              "timestamp",
              String.valueOf(
                  new DateTime(123456L, ISOChronology.getInstanceUTC()).getMillis() / 1000.0D))
          .set("valid", "false");

  private static final Row NULL_FLAT_ROW =
      Row.withSchema(FLAT_TYPE).addValues(null, null, null, null, null).build();

  private static final TableRow BQ_NULL_FLAT_ROW =
      new TableRow()
          .set("id", null)
          .set("value", null)
          .set("name", null)
          .set("timestamp", null)
          .set("valid", null);

  private static final Row ARRAY_ROW =
      Row.withSchema(ARRAY_TYPE).addValues((Object) Arrays.asList(123L, 124L)).build();

  private static final TableRow BQ_ARRAY_ROW =
      new TableRow().set("ids", Arrays.asList("123", "124"));

  private static final Row ROW_ROW = Row.withSchema(ROW_TYPE).addValues(FLAT_ROW).build();

  private static final TableRow BQ_ROW_ROW = new TableRow().set("row", BQ_FLAT_ROW);

  private static final Row ARRAY_ROW_ROW =
      Row.withSchema(ARRAY_ROW_TYPE).addValues((Object) Arrays.asList(FLAT_ROW)).build();

  private static final TableRow BQ_ARRAY_ROW_ROW =
      new TableRow().set("rows", Collections.singletonList(BQ_FLAT_ROW));

  private static final TableSchema BQ_FLAT_TYPE =
      new TableSchema().setFields(Arrays.asList(ID, VALUE, NAME, TIMESTAMP, VALID));

  private static final TableSchema BQ_ARRAY_TYPE = new TableSchema().setFields(Arrays.asList(IDS));

  private static final TableSchema BQ_ROW_TYPE = new TableSchema().setFields(Arrays.asList(ROW));

  private static final TableSchema BQ_ARRAY_ROW_TYPE =
      new TableSchema().setFields(Arrays.asList(ROWS));

  @Test
  public void testToTableSchema_flat() {
    TableSchema schema = toTableSchema(FLAT_TYPE);

    assertThat(schema.getFields(), containsInAnyOrder(ID, VALUE, NAME, TIMESTAMP, VALID));
  }

  @Test
  public void testToTableSchema_array() {
    TableSchema schema = toTableSchema(ARRAY_TYPE);

    assertThat(schema.getFields(), contains(IDS));
  }

  @Test
  public void testToTableSchema_row() {
    TableSchema schema = toTableSchema(ROW_TYPE);

    assertThat(schema.getFields().size(), equalTo(1));
    TableFieldSchema field = schema.getFields().get(0);
    assertThat(field.getName(), equalTo("row"));
    assertThat(field.getType(), equalTo(StandardSQLTypeName.STRUCT.toString()));
    assertThat(field.getMode(), nullValue());
    assertThat(field.getFields(), containsInAnyOrder(ID, VALUE, NAME, TIMESTAMP, VALID));
  }

  @Test
  public void testToTableSchema_array_row() {
    TableSchema schema = toTableSchema(ARRAY_ROW_TYPE);

    assertThat(schema.getFields().size(), equalTo(1));
    TableFieldSchema field = schema.getFields().get(0);
    assertThat(field.getName(), equalTo("rows"));
    assertThat(field.getType(), equalTo(StandardSQLTypeName.STRUCT.toString()));
    assertThat(field.getMode(), equalTo(Mode.REPEATED.toString()));
    assertThat(field.getFields(), containsInAnyOrder(ID, VALUE, NAME, TIMESTAMP, VALID));
  }

  @Test
  public void testToTableRow_flat() {
    TableRow row = toTableRow().apply(FLAT_ROW);
    System.out.println(row);

    assertThat(row.size(), equalTo(5));
    assertThat(row, hasEntry("id", "123"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("name", "test"));
    assertThat(row, hasEntry("valid", "false"));
  }

  @Test
  public void testToTableRow_array() {
    TableRow row = toTableRow().apply(ARRAY_ROW);

    assertThat(row, hasEntry("ids", Arrays.asList("123", "124")));
    assertThat(row.size(), equalTo(1));
  }

  @Test
  public void testToTableRow_row() {
    TableRow row = toTableRow().apply(ROW_ROW);

    assertThat(row.size(), equalTo(1));
    row = (TableRow) row.get("row");
    assertThat(row.size(), equalTo(5));
    assertThat(row, hasEntry("id", "123"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("name", "test"));
    assertThat(row, hasEntry("valid", "false"));
  }

  @Test
  public void testToTableRow_array_row() {
    TableRow row = toTableRow().apply(ARRAY_ROW_ROW);

    assertThat(row.size(), equalTo(1));
    row = ((List<TableRow>) row.get("rows")).get(0);
    assertThat(row.size(), equalTo(5));
    assertThat(row, hasEntry("id", "123"));
    assertThat(row, hasEntry("value", "123.456"));
    assertThat(row, hasEntry("name", "test"));
    assertThat(row, hasEntry("valid", "false"));
  }

  @Test
  public void testToTableRow_null_row() {
    TableRow row = toTableRow().apply(NULL_FLAT_ROW);

    assertThat(row.size(), equalTo(5));
    assertThat(row, hasEntry("id", null));
    assertThat(row, hasEntry("value", null));
    assertThat(row, hasEntry("name", null));
    assertThat(row, hasEntry("timestamp", null));
    assertThat(row, hasEntry("valid", null));
  }

  private static final BigQueryUtils.ConversionOptions TRUNCATE_OPTIONS =
      BigQueryUtils.ConversionOptions.builder()
          .setTruncateTimestamps(TruncateTimestamps.TRUNCATE)
          .build();

  private static final BigQueryUtils.ConversionOptions REJECT_OPTIONS =
      BigQueryUtils.ConversionOptions.builder()
          .setTruncateTimestamps(TruncateTimestamps.REJECT)
          .build();

  @Test
  public void testSubMilliPrecisionRejected() {
    assertThrows(
        "precision",
        IllegalArgumentException.class,
        () ->
            BigQueryUtils.convertAvroFormat(
                Schema.Field.of("dummy", Schema.FieldType.DATETIME), 1000000001L, REJECT_OPTIONS));
  }

  @Test
  public void testMilliPrecisionOk() {
    long millis = 123456789L;
    assertThat(
        BigQueryUtils.convertAvroFormat(
            Schema.Field.of("dummy", Schema.FieldType.DATETIME), millis * 1000, REJECT_OPTIONS),
        equalTo(new Instant(millis)));
  }

  @Test
  public void testSubMilliPrecisionTruncated() {
    long millis = 123456789L;
    assertThat(
        BigQueryUtils.convertAvroFormat(
            Schema.Field.of("dummy", Schema.FieldType.DATETIME),
            millis * 1000 + 123,
            TRUNCATE_OPTIONS),
        equalTo(new Instant(millis)));
  }

  @Test
  public void testSubMilliPrecisionLogicalTypeRejected() {
    assertThrows(
        "precision",
        IllegalArgumentException.class,
        () ->
            BigQueryUtils.convertAvroFormat(
                Schema.Field.of("dummy", Schema.FieldType.logicalType(new FakeSqlTimeType())),
                1000000001L,
                REJECT_OPTIONS));
  }

  @Test
  public void testMilliPrecisionOkLogicaltype() {
    long millis = 123456789L;
    assertThat(
        BigQueryUtils.convertAvroFormat(
            Schema.Field.of("dummy", Schema.FieldType.logicalType(new FakeSqlTimeType())),
            millis * 1000,
            REJECT_OPTIONS),
        equalTo(new Instant(millis)));
  }

  @Test
  public void testMilliPrecisionTruncatedLogicaltype() {
    long millis = 123456789L;
    assertThat(
        BigQueryUtils.convertAvroFormat(
            Schema.Field.of("dummy", Schema.FieldType.logicalType(new FakeSqlTimeType())),
            millis * 1000 + 123,
            TRUNCATE_OPTIONS),
        equalTo(new Instant(millis)));
  }

  private static class FakeSqlTimeType implements Schema.LogicalType<Long, Instant> {
    @Override
    public String getIdentifier() {
      return "SqlTimeType";
    }

    @Override
    public Schema.FieldType getBaseType() {
      return Schema.FieldType.DATETIME;
    }

    @Override
    public Instant toBaseType(Long input) {
      // Already converted to millis outside this constructor
      return new Instant((long) input);
    }

    @Override
    public Long toInputType(Instant base) {
      return base.getMillis();
    }
  }

  @Test
  public void testFromTableSchema_flat() {
    Schema beamSchema = BigQueryUtils.fromTableSchema(BQ_FLAT_TYPE);
    assertEquals(FLAT_TYPE, beamSchema);
  }

  @Test
  public void testFromTableSchema_array() {
    Schema beamSchema = BigQueryUtils.fromTableSchema(BQ_ARRAY_TYPE);
    assertEquals(ARRAY_TYPE, beamSchema);
  }

  @Test
  public void testFromTableSchema_row() {
    Schema beamSchema = BigQueryUtils.fromTableSchema(BQ_ROW_TYPE);
    assertEquals(ROW_TYPE, beamSchema);
  }

  @Test
  public void testFromTableSchema_array_row() {
    Schema beamSchema = BigQueryUtils.fromTableSchema(BQ_ARRAY_ROW_TYPE);
    assertEquals(ARRAY_ROW_TYPE, beamSchema);
  }

  @Test
  public void testToBeamRow_flat() {
    Row beamRow = BigQueryUtils.toBeamRow(FLAT_TYPE, BQ_FLAT_ROW);
    assertEquals(FLAT_ROW, beamRow);
  }

  @Test
  public void testToBeamRow_null() {
    Row beamRow = BigQueryUtils.toBeamRow(FLAT_TYPE, BQ_NULL_FLAT_ROW);
    assertEquals(NULL_FLAT_ROW, beamRow);
  }

  @Test
  public void testToBeamRow_array() {
    Row beamRow = BigQueryUtils.toBeamRow(ARRAY_TYPE, BQ_ARRAY_ROW);
    assertEquals(ARRAY_ROW, beamRow);
  }

  @Test
  public void testToBeamRow_row() {
    Row beamRow = BigQueryUtils.toBeamRow(ROW_TYPE, BQ_ROW_ROW);
    assertEquals(ROW_ROW, beamRow);
  }

  @Test
  public void testToBeamRow_array_row() {
    Row beamRow = BigQueryUtils.toBeamRow(ARRAY_ROW_TYPE, BQ_ARRAY_ROW_ROW);
    assertEquals(ARRAY_ROW_ROW, beamRow);
  }
}
