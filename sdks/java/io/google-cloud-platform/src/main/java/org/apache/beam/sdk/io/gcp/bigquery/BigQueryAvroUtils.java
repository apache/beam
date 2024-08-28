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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Verify.verify;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;
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
          .put("INT64", Type.LONG)
          .put("FLOAT", Type.DOUBLE)
          .put("FLOAT64", Type.DOUBLE)
          .put("NUMERIC", Type.BYTES)
          .put("BIGNUMERIC", Type.BYTES)
          .put("BOOLEAN", Type.BOOLEAN)
          .put("BOOL", Type.BOOLEAN)
          .put("TIMESTAMP", Type.LONG)
          .put("RECORD", Type.RECORD)
          .put("STRUCT", Type.RECORD)
          .put("DATE", Type.STRING)
          .put("DATE", Type.INT)
          .put("DATETIME", Type.STRING)
          .put("TIME", Type.STRING)
          .put("TIME", Type.LONG)
          .put("JSON", Type.STRING)
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
  static TableRow convertGenericRecordToTableRow(GenericRecord record) {
    TableRow row = new TableRow();
    Schema schema = record.getSchema();

    for (Field field : schema.getFields()) {
      Object convertedValue =
          getTypedCellValue(field.name(), field.schema(), record.get(field.pos()));
      if (convertedValue != null) {
        // To match the JSON files exported by BigQuery, do not include null values in the output.
        row.set(field.name(), convertedValue);
      }
    }

    return row;
  }

  private static @Nullable Object getTypedCellValue(String name, Schema schema, Object v) {
    // Per https://cloud.google.com/bigquery/docs/reference/v2/tables#schema, the mode field
    // is optional (and so it may be null), but defaults to "NULLABLE".
    Type type = schema.getType();
    switch (type) {
      case ARRAY:
        return convertRepeatedField(name, schema.getElementType(), v);
      case UNION:
        return convertNullableField(name, schema, v);
      case MAP:
        throw new UnsupportedOperationException(
            String.format(
                "Unexpected BigQuery field schema type %s for field named %s", type, name));
      default:
        return convertRequiredField(name, schema, v);
    }
  }

  private static List<Object> convertRepeatedField(String name, Schema elementType, Object v) {
    // REPEATED fields are represented as Avro arrays.
    if (v == null) {
      // Handle the case of an empty repeated field.
      return new ArrayList<>();
    }
    @SuppressWarnings("unchecked")
    List<Object> elements = (List<Object>) v;
    ArrayList<Object> values = new ArrayList<>();
    for (Object element : elements) {
      values.add(convertRequiredField(name, elementType, element));
    }
    return values;
  }

  private static Object convertRequiredField(String name, Schema schema, Object v) {
    // REQUIRED fields are represented as the corresponding Avro types. For example, a BigQuery
    // INTEGER type maps to an Avro LONG type.
    checkNotNull(v, "REQUIRED field %s should not be null", name);

    // For historical reasons, don't validate avroLogicalType except for with NUMERIC.
    // BigQuery represents NUMERIC in Avro format as BYTES with a DECIMAL logical type.
    Type type = schema.getType();
    LogicalType logicalType = schema.getLogicalType();
    switch (type) {
      case BOOLEAN:
        // SQL types BOOL, BOOLEAN
        return v;
      case INT:
        if (logicalType instanceof LogicalTypes.Date) {
          // SQL types DATE
          return formatDate((Integer) v);
        } else {
          throw new UnsupportedOperationException(
              String.format(
                  "Unexpected BigQuery field schema type %s for field named %s", type, name));
        }
      case LONG:
        if (logicalType instanceof LogicalTypes.TimeMicros) {
          // SQL types TIME
          return formatTime((Long) v);
        } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
          // SQL types TIMESTAMP
          return formatTimestamp((Long) v);
        } else {
          // SQL types INT64 (INT, SMALLINT, INTEGER, BIGINT, TINYINT, BYTEINT)
          return ((Long) v).toString();
        }
      case DOUBLE:
        // SQL types FLOAT64
        return v;
      case BYTES:
        if (logicalType instanceof LogicalTypes.Decimal) {
          // SQL tpe NUMERIC, BIGNUMERIC
          return new Conversions.DecimalConversion()
              .fromBytes((ByteBuffer) v, schema, logicalType)
              .toString();
        } else {
          // SQL types BYTES
          return BaseEncoding.base64().encode(((ByteBuffer) v).array());
        }
      case STRING:
        // SQL types STRING, DATETIME, GEOGRAPHY, JSON
        return v.toString();
      case RECORD:
        return convertGenericRecordToTableRow((GenericRecord) v);
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Unexpected BigQuery field schema type %s for field named %s", type, name));
    }
  }

  private static @Nullable Object convertNullableField(String name, Schema union, Object v) {
    // NULLABLE fields are represented as an Avro Union of the corresponding type and "null".
    verify(
        union.getType() == Type.UNION,
        "Expected Avro schema type UNION, not %s, for BigQuery NULLABLE field %s",
        union.getType(),
        name);
    List<Schema> unionTypes = union.getTypes();
    verify(
        unionTypes.size() == 2,
        "BigQuery NULLABLE field %s should be an Avro UNION of NULL and another type, not %s",
        name,
        union);

    Schema type = union.getTypes().get(GenericData.get().resolveUnion(union, v));
    if (type.getType() == Type.NULL) {
      return null;
    } else {
      return convertRequiredField(name, type, v);
    }
  }

  static Schema toGenericAvroSchema(
      String schemaName, List<TableFieldSchema> fieldSchemas, @Nullable String namespace) {

    String nextNamespace = namespace == null ? null : String.format("%s.%s", namespace, schemaName);

    List<Field> avroFields = new ArrayList<>();
    for (TableFieldSchema bigQueryField : fieldSchemas) {
      avroFields.add(convertField(bigQueryField, nextNamespace));
    }
    return Schema.createRecord(
        schemaName,
        "Translated Avro Schema for " + schemaName,
        namespace == null ? "org.apache.beam.sdk.io.gcp.bigquery" : namespace,
        false,
        avroFields);
  }

  static Schema toGenericAvroSchema(String schemaName, List<TableFieldSchema> fieldSchemas) {
    return toGenericAvroSchema(
        schemaName,
        fieldSchemas,
        hasNamespaceCollision(fieldSchemas) ? "org.apache.beam.sdk.io.gcp.bigquery" : null);
  }

  // To maintain backwards compatibility we only disambiguate collisions in the field namespaces as
  // these never worked with this piece of code.
  private static boolean hasNamespaceCollision(List<TableFieldSchema> fieldSchemas) {
    Set<String> recordTypeFieldNames = new HashSet<>();

    List<TableFieldSchema> fieldsToCheck = new ArrayList<>();
    for (fieldsToCheck.addAll(fieldSchemas); !fieldsToCheck.isEmpty(); ) {
      TableFieldSchema field = fieldsToCheck.remove(0);
      if ("STRUCT".equals(field.getType()) || "RECORD".equals(field.getType())) {
        if (recordTypeFieldNames.contains(field.getName())) {
          return true;
        }
        recordTypeFieldNames.add(field.getName());
        fieldsToCheck.addAll(field.getFields());
      }
    }

    // No collisions present
    return false;
  }

  @SuppressWarnings({
    "nullness" // Avro library not annotated
  })
  private static Field convertField(TableFieldSchema bigQueryField, @Nullable String namespace) {
    ImmutableCollection<Type> avroTypes = BIG_QUERY_TO_AVRO_TYPES.get(bigQueryField.getType());
    if (avroTypes.isEmpty()) {
      throw new IllegalArgumentException(
          "Unable to map BigQuery field type " + bigQueryField.getType() + " to avro type.");
    }

    Type avroType = avroTypes.iterator().next();
    Schema elementSchema;
    if (avroType == Type.RECORD) {
      elementSchema =
          toGenericAvroSchema(bigQueryField.getName(), bigQueryField.getFields(), namespace);
    } else {
      elementSchema = handleAvroLogicalTypes(bigQueryField, avroType);
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

  private static Schema handleAvroLogicalTypes(TableFieldSchema bigQueryField, Type avroType) {
    String bqType = bigQueryField.getType();
    switch (bqType) {
      case "NUMERIC":
        // Default value based on
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
        int precision = Optional.ofNullable(bigQueryField.getPrecision()).orElse(38L).intValue();
        int scale = Optional.ofNullable(bigQueryField.getScale()).orElse(9L).intValue();
        return LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Type.BYTES));
      case "BIGNUMERIC":
        // Default value based on
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
        int precisionBigNumeric =
            Optional.ofNullable(bigQueryField.getPrecision()).orElse(77L).intValue();
        int scaleBigNumeric = Optional.ofNullable(bigQueryField.getScale()).orElse(38L).intValue();
        return LogicalTypes.decimal(precisionBigNumeric, scaleBigNumeric)
            .addToSchema(Schema.create(Type.BYTES));
      case "TIMESTAMP":
        return LogicalTypes.timestampMicros().addToSchema(Schema.create(Type.LONG));
      case "GEOGRAPHY":
        Schema geoSchema = Schema.create(Type.STRING);
        geoSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, "geography_wkt");
        return geoSchema;
      default:
        return Schema.create(avroType);
    }
  }
}
