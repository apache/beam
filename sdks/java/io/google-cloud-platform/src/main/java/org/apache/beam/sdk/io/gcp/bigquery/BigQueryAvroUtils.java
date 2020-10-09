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

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Verify.verify;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Verify.verifyNotNull;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * A set of utilities for working with Avro files.
 *
 * <p>These utilities are based on the <a href="https://avro.apache.org/docs/1.8.1/spec.html">Avro
 * 1.8.1</a> specification.
 */
class BigQueryAvroUtils {

  /**
   * Defines the valid mapping between BigQuery types and native Avro types.
   *
   * <p>Some BigQuery types are duplicated here since slightly different Avro records are produced
   * when exporting data in Avro format and when reading data directly using the read API.
   */
  static final ImmutableMultimap<String, Type> BIG_QUERY_TO_AVRO_TYPES =
      ImmutableMultimap.<String, Type>builder()
          .put("STRING", Type.STRING)
          .put("GEOGRAPHY", Type.STRING)
          .put("BYTES", Type.BYTES)
          .put("INTEGER", Type.LONG)
          .put("FLOAT", Type.DOUBLE)
          .put("NUMERIC", Type.BYTES)
          .put("BOOLEAN", Type.BOOLEAN)
          .put("TIMESTAMP", Type.LONG)
          .put("RECORD", Type.RECORD)
          .put("DATE", Type.STRING)
          .put("DATE", Type.INT)
          .put("DATETIME", Type.STRING)
          .put("TIME", Type.STRING)
          .put("TIME", Type.LONG)
          .build();

  /**
   * Formats BigQuery seconds-since-epoch into String matching JSON export. Thread-safe and
   * immutable.
   */
  private static final DateTimeFormatter DATE_AND_SECONDS_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();

  @VisibleForTesting
  static String formatTimestamp(Long timestampMicro) {
    // timestampMicro is in "microseconds since epoch" format,
    // e.g., 1452062291123456L means "2016-01-06 06:38:11.123456 UTC".
    // Separate into seconds and microseconds.
    long timestampSec = timestampMicro / 1_000_000;
    long micros = timestampMicro % 1_000_000;
    if (micros < 0) {
      micros += 1_000_000;
      timestampSec -= 1;
    }
    String dayAndTime = DATE_AND_SECONDS_FORMATTER.print(timestampSec * 1000);

    if (micros == 0) {
      return String.format("%s UTC", dayAndTime);
    }
    return String.format("%s.%06d UTC", dayAndTime, micros);
  }

  /**
   * This method formats a BigQuery DATE value into a String matching the format used by JSON
   * export. Date records are stored in "days since epoch" format, and BigQuery uses the proleptic
   * Gregorian calendar.
   */
  private static String formatDate(int date) {
    return LocalDate.ofEpochDay(date).format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE);
  }

  private static final java.time.format.DateTimeFormatter ISO_LOCAL_TIME_FORMATTER_MICROS =
      new DateTimeFormatterBuilder()
          .appendValue(HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(MINUTE_OF_HOUR, 2)
          .appendLiteral(':')
          .appendValue(SECOND_OF_MINUTE, 2)
          .appendLiteral('.')
          .appendFraction(NANO_OF_SECOND, 6, 6, false)
          .toFormatter();

  private static final java.time.format.DateTimeFormatter ISO_LOCAL_TIME_FORMATTER_MILLIS =
      new DateTimeFormatterBuilder()
          .appendValue(HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(MINUTE_OF_HOUR, 2)
          .appendLiteral(':')
          .appendValue(SECOND_OF_MINUTE, 2)
          .appendLiteral('.')
          .appendFraction(NANO_OF_SECOND, 3, 3, false)
          .toFormatter();

  private static final java.time.format.DateTimeFormatter ISO_LOCAL_TIME_FORMATTER_SECONDS =
      new DateTimeFormatterBuilder()
          .appendValue(HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(MINUTE_OF_HOUR, 2)
          .appendLiteral(':')
          .appendValue(SECOND_OF_MINUTE, 2)
          .toFormatter();

  /**
   * This method formats a BigQuery TIME value into a String matching the format used by JSON
   * export. Time records are stored in "microseconds since midnight" format.
   */
  private static String formatTime(long timeMicros) {
    java.time.format.DateTimeFormatter formatter;
    if (timeMicros % 1000000 == 0) {
      formatter = ISO_LOCAL_TIME_FORMATTER_SECONDS;
    } else if (timeMicros % 1000 == 0) {
      formatter = ISO_LOCAL_TIME_FORMATTER_MILLIS;
    } else {
      formatter = ISO_LOCAL_TIME_FORMATTER_MICROS;
    }
    return LocalTime.ofNanoOfDay(timeMicros * 1000).format(formatter);
  }

  static TableSchema trimBigQueryTableSchema(TableSchema inputSchema, Schema avroSchema) {
    List<TableFieldSchema> subSchemas =
        inputSchema.getFields().stream()
            .flatMap(fieldSchema -> mapTableFieldSchema(fieldSchema, avroSchema))
            .collect(Collectors.toList());

    return new TableSchema().setFields(subSchemas);
  }

  private static Stream<TableFieldSchema> mapTableFieldSchema(
      TableFieldSchema fieldSchema, Schema avroSchema) {
    Field avroFieldSchema = avroSchema.getField(fieldSchema.getName());
    if (avroFieldSchema == null) {
      return Stream.empty();
    } else if (avroFieldSchema.schema().getType() != Type.RECORD) {
      return Stream.of(fieldSchema);
    }

    List<TableFieldSchema> subSchemas =
        fieldSchema.getFields().stream()
            .flatMap(subSchema -> mapTableFieldSchema(subSchema, avroFieldSchema.schema()))
            .collect(Collectors.toList());

    TableFieldSchema output =
        new TableFieldSchema()
            .setCategories(fieldSchema.getCategories())
            .setDescription(fieldSchema.getDescription())
            .setFields(subSchemas)
            .setMode(fieldSchema.getMode())
            .setName(fieldSchema.getName())
            .setType(fieldSchema.getType());

    return Stream.of(output);
  }

  /**
   * Utility function to convert from an Avro {@link GenericRecord} to a BigQuery {@link TableRow}.
   *
   * <p>See <a href="https://cloud.google.com/bigquery/exporting-data-from-bigquery#config">"Avro
   * format"</a> for more information.
   */
  static TableRow convertGenericRecordToTableRow(GenericRecord record, TableSchema schema) {
    return convertGenericRecordToTableRow(record, schema.getFields());
  }

  private static TableRow convertGenericRecordToTableRow(
      GenericRecord record, List<TableFieldSchema> fields) {
    TableRow row = new TableRow();
    for (TableFieldSchema subSchema : fields) {
      // Per https://cloud.google.com/bigquery/docs/reference/v2/tables#schema, the name field
      // is required, so it may not be null.
      Field field = record.getSchema().getField(subSchema.getName());
      Object convertedValue =
          getTypedCellValue(field.schema(), subSchema, record.get(field.name()));
      if (convertedValue != null) {
        // To match the JSON files exported by BigQuery, do not include null values in the output.
        row.set(field.name(), convertedValue);
      }
    }

    return row;
  }

  private static @Nullable Object getTypedCellValue(
      Schema schema, TableFieldSchema fieldSchema, Object v) {
    // Per https://cloud.google.com/bigquery/docs/reference/v2/tables#schema, the mode field
    // is optional (and so it may be null), but defaults to "NULLABLE".
    String mode = firstNonNull(fieldSchema.getMode(), "NULLABLE");
    switch (mode) {
      case "REQUIRED":
        return convertRequiredField(schema.getType(), schema.getLogicalType(), fieldSchema, v);
      case "REPEATED":
        return convertRepeatedField(schema, fieldSchema, v);
      case "NULLABLE":
        return convertNullableField(schema, fieldSchema, v);
      default:
        throw new UnsupportedOperationException(
            "Parsing a field with BigQuery field schema mode " + fieldSchema.getMode());
    }
  }

  private static List<Object> convertRepeatedField(
      Schema schema, TableFieldSchema fieldSchema, Object v) {
    Type arrayType = schema.getType();
    verify(
        arrayType == Type.ARRAY,
        "BigQuery REPEATED field %s should be Avro ARRAY, not %s",
        fieldSchema.getName(),
        arrayType);
    // REPEATED fields are represented as Avro arrays.
    if (v == null) {
      // Handle the case of an empty repeated field.
      return new ArrayList<>();
    }
    @SuppressWarnings("unchecked")
    List<Object> elements = (List<Object>) v;
    ArrayList<Object> values = new ArrayList<>();
    Type elementType = schema.getElementType().getType();
    LogicalType elementLogicalType = schema.getElementType().getLogicalType();
    for (Object element : elements) {
      values.add(convertRequiredField(elementType, elementLogicalType, fieldSchema, element));
    }
    return values;
  }

  private static Object convertRequiredField(
      Type avroType, LogicalType avroLogicalType, TableFieldSchema fieldSchema, Object v) {
    // REQUIRED fields are represented as the corresponding Avro types. For example, a BigQuery
    // INTEGER type maps to an Avro LONG type.
    checkNotNull(v, "REQUIRED field %s should not be null", fieldSchema.getName());
    // Per https://cloud.google.com/bigquery/docs/reference/v2/tables#schema, the type field
    // is required, so it may not be null.
    String bqType = fieldSchema.getType();
    ImmutableCollection<Type> expectedAvroTypes = BIG_QUERY_TO_AVRO_TYPES.get(bqType);
    verifyNotNull(expectedAvroTypes, "Unsupported BigQuery type: %s", bqType);
    verify(
        expectedAvroTypes.contains(avroType),
        "Expected Avro schema types %s for BigQuery %s field %s, but received %s",
        expectedAvroTypes,
        bqType,
        fieldSchema.getName(),
        avroType);
    // For historical reasons, don't validate avroLogicalType except for with NUMERIC.
    // BigQuery represents NUMERIC in Avro format as BYTES with a DECIMAL logical type.
    switch (bqType) {
      case "STRING":
      case "DATETIME":
      case "GEOGRAPHY":
        // Avro will use a CharSequence to represent String objects, but it may not always use
        // java.lang.String; for example, it may prefer org.apache.avro.util.Utf8.
        verify(v instanceof CharSequence, "Expected CharSequence (String), got %s", v.getClass());
        return v.toString();
      case "DATE":
        if (avroType == Type.INT) {
          verify(v instanceof Integer, "Expected Integer, got %s", v.getClass());
          verifyNotNull(avroLogicalType, "Expected Date logical type");
          verify(avroLogicalType instanceof LogicalTypes.Date, "Expected Date logical type");
          return formatDate((Integer) v);
        } else {
          verify(v instanceof CharSequence, "Expected CharSequence (String), got %s", v.getClass());
          return v.toString();
        }
      case "TIME":
        if (avroType == Type.LONG) {
          verify(v instanceof Long, "Expected Long, got %s", v.getClass());
          verifyNotNull(avroLogicalType, "Expected TimeMicros logical type");
          verify(
              avroLogicalType instanceof LogicalTypes.TimeMicros,
              "Expected TimeMicros logical type");
          return formatTime((Long) v);
        } else {
          verify(v instanceof CharSequence, "Expected CharSequence (String), got %s", v.getClass());
          return v.toString();
        }
      case "INTEGER":
        verify(v instanceof Long, "Expected Long, got %s", v.getClass());
        return (Long) v;
      case "FLOAT":
        verify(v instanceof Double, "Expected Double, got %s", v.getClass());
        return v;
      case "NUMERIC":
        // NUMERIC data types are represented as BYTES with the DECIMAL logical type. They are
        // converted back to Strings with precision and scale determined by the logical type.
        verify(v instanceof ByteBuffer, "Expected ByteBuffer, got %s", v.getClass());
        verifyNotNull(avroLogicalType, "Expected Decimal logical type");
        verify(avroLogicalType instanceof LogicalTypes.Decimal, "Expected Decimal logical type");
        BigDecimal numericValue =
            new Conversions.DecimalConversion()
                .fromBytes((ByteBuffer) v, Schema.create(avroType), avroLogicalType);
        return numericValue.toString();
      case "BOOL":
      case "BOOLEAN":
        verify(v instanceof Boolean, "Expected Boolean, got %s", v.getClass());
        return v;
      case "TIMESTAMP":
        // TIMESTAMP data types are represented as Avro LONG types, microseconds since the epoch.
        // Values may be negative since BigQuery timestamps start at 0001-01-01 00:00:00 UTC.
        verify(v instanceof Long, "Expected Long, got %s", v.getClass());
        return formatTimestamp((Long) v);
      case "RECORD":
        verify(v instanceof GenericRecord, "Expected GenericRecord, got %s", v.getClass());
        return convertGenericRecordToTableRow((GenericRecord) v, fieldSchema.getFields());
      case "BYTES":
        verify(v instanceof ByteBuffer, "Expected ByteBuffer, got %s", v.getClass());
        ByteBuffer byteBuffer = (ByteBuffer) v;
        byte[] bytes = new byte[byteBuffer.limit()];
        byteBuffer.get(bytes);
        return BaseEncoding.base64().encode(bytes);
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Unexpected BigQuery field schema type %s for field named %s",
                fieldSchema.getType(), fieldSchema.getName()));
    }
  }

  private static @Nullable Object convertNullableField(
      Schema avroSchema, TableFieldSchema fieldSchema, Object v) {
    // NULLABLE fields are represented as an Avro Union of the corresponding type and "null".
    verify(
        avroSchema.getType() == Type.UNION,
        "Expected Avro schema type UNION, not %s, for BigQuery NULLABLE field %s",
        avroSchema.getType(),
        fieldSchema.getName());
    List<Schema> unionTypes = avroSchema.getTypes();
    verify(
        unionTypes.size() == 2,
        "BigQuery NULLABLE field %s should be an Avro UNION of NULL and another type, not %s",
        fieldSchema.getName(),
        unionTypes);

    if (v == null) {
      return null;
    }

    Type firstType = unionTypes.get(0).getType();
    if (!firstType.equals(Type.NULL)) {
      return convertRequiredField(firstType, unionTypes.get(0).getLogicalType(), fieldSchema, v);
    }
    return convertRequiredField(
        unionTypes.get(1).getType(), unionTypes.get(1).getLogicalType(), fieldSchema, v);
  }

  static Schema toGenericAvroSchema(String schemaName, List<TableFieldSchema> fieldSchemas) {
    List<Field> avroFields = new ArrayList<>();
    for (TableFieldSchema bigQueryField : fieldSchemas) {
      avroFields.add(convertField(bigQueryField));
    }
    return Schema.createRecord(
        schemaName,
        "Translated Avro Schema for " + schemaName,
        "org.apache.beam.sdk.io.gcp.bigquery",
        false,
        avroFields);
  }

  private static Field convertField(TableFieldSchema bigQueryField) {
    ImmutableCollection<Type> avroTypes = BIG_QUERY_TO_AVRO_TYPES.get(bigQueryField.getType());
    if (avroTypes.isEmpty()) {
      throw new IllegalArgumentException(
          "Unable to map BigQuery field type " + bigQueryField.getType() + " to avro type.");
    }

    Type avroType = avroTypes.iterator().next();
    Schema elementSchema;
    if (avroType == Type.RECORD) {
      elementSchema = toGenericAvroSchema(bigQueryField.getName(), bigQueryField.getFields());
    } else {
      elementSchema = Schema.create(avroType);
    }
    Schema fieldSchema;
    if (bigQueryField.getMode() == null || "NULLABLE".equals(bigQueryField.getMode())) {
      fieldSchema = Schema.createUnion(Schema.create(Type.NULL), elementSchema);
    } else if ("REQUIRED".equals(bigQueryField.getMode())) {
      fieldSchema = elementSchema;
    } else if ("REPEATED".equals(bigQueryField.getMode())) {
      fieldSchema = Schema.createArray(elementSchema);
    } else {
      throw new IllegalArgumentException(
          String.format("Unknown BigQuery Field Mode: %s", bigQueryField.getMode()));
    }
    return new Field(
        bigQueryField.getName(),
        fieldSchema,
        bigQueryField.getDescription(),
        (Object) null /* Cast to avoid deprecated JsonNode constructor. */);
  }
}
