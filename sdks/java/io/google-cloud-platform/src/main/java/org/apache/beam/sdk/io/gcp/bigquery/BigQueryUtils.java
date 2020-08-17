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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.beam.sdk.values.Row.toRow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.PassThroughLogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

/** Utility methods for BigQuery related operations. */
public class BigQueryUtils {

  /** Options for how to convert BigQuery data to Beam data. */
  @AutoValue
  public abstract static class ConversionOptions implements Serializable {

    /**
     * Controls whether to truncate timestamps to millisecond precision lossily, or to crash when
     * truncation would result.
     */
    public enum TruncateTimestamps {
      /** Reject timestamps with greater-than-millisecond precision. */
      REJECT,

      /** Truncate timestamps to millisecond precision. */
      TRUNCATE;
    }

    public abstract TruncateTimestamps getTruncateTimestamps();

    public static Builder builder() {
      return new AutoValue_BigQueryUtils_ConversionOptions.Builder()
          .setTruncateTimestamps(TruncateTimestamps.REJECT);
    }

    /** Builder for {@link ConversionOptions}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTruncateTimestamps(TruncateTimestamps truncateTimestamps);

      public abstract ConversionOptions build();
    }
  }

  private static final DateTimeFormatter BIGQUERY_TIMESTAMP_PRINTER;

  /**
   * Native BigQuery formatter for it's timestamp format, depending on the milliseconds stored in
   * the column, the milli second part will be 6, 3 or absent. Example {@code 2019-08-16
   * 00:52:07[.123]|[.123456] UTC}
   */
  private static final DateTimeFormatter BIGQUERY_TIMESTAMP_PARSER;

  static {
    DateTimeFormatter dateTimePart =
        new DateTimeFormatterBuilder()
            .appendYear(4, 4)
            .appendLiteral('-')
            .appendMonthOfYear(2)
            .appendLiteral('-')
            .appendDayOfMonth(2)
            .appendLiteral(' ')
            .appendHourOfDay(2)
            .appendLiteral(':')
            .appendMinuteOfHour(2)
            .appendLiteral(':')
            .appendSecondOfMinute(2)
            .toFormatter()
            .withZoneUTC();
    BIGQUERY_TIMESTAMP_PARSER =
        new DateTimeFormatterBuilder()
            .append(dateTimePart)
            .appendOptional(
                new DateTimeFormatterBuilder()
                    .appendLiteral('.')
                    .appendFractionOfSecond(3, 6)
                    .toParser())
            .appendLiteral(" UTC")
            .toFormatter()
            .withZoneUTC();
    BIGQUERY_TIMESTAMP_PRINTER =
        new DateTimeFormatterBuilder()
            .append(dateTimePart)
            .appendLiteral('.')
            .appendFractionOfSecond(3, 3)
            .appendLiteral(" UTC")
            .toFormatter();
  }

  private static final Map<TypeName, StandardSQLTypeName> BEAM_TO_BIGQUERY_TYPE_MAPPING =
      ImmutableMap.<TypeName, StandardSQLTypeName>builder()
          .put(TypeName.BYTE, StandardSQLTypeName.INT64)
          .put(TypeName.INT16, StandardSQLTypeName.INT64)
          .put(TypeName.INT32, StandardSQLTypeName.INT64)
          .put(TypeName.INT64, StandardSQLTypeName.INT64)
          .put(TypeName.FLOAT, StandardSQLTypeName.FLOAT64)
          .put(TypeName.DOUBLE, StandardSQLTypeName.FLOAT64)
          .put(TypeName.DECIMAL, StandardSQLTypeName.NUMERIC)
          .put(TypeName.BOOLEAN, StandardSQLTypeName.BOOL)
          .put(TypeName.ARRAY, StandardSQLTypeName.ARRAY)
          .put(TypeName.ITERABLE, StandardSQLTypeName.ARRAY)
          .put(TypeName.ROW, StandardSQLTypeName.STRUCT)
          .put(TypeName.DATETIME, StandardSQLTypeName.TIMESTAMP)
          .put(TypeName.STRING, StandardSQLTypeName.STRING)
          .put(TypeName.BYTES, StandardSQLTypeName.BYTES)
          .build();

  private static final Map<TypeName, Function<String, Object>> JSON_VALUE_PARSERS =
      ImmutableMap.<TypeName, Function<String, Object>>builder()
          .put(TypeName.BYTE, Byte::valueOf)
          .put(TypeName.INT16, Short::valueOf)
          .put(TypeName.INT32, Integer::valueOf)
          .put(TypeName.INT64, Long::valueOf)
          .put(TypeName.FLOAT, Float::valueOf)
          .put(TypeName.DOUBLE, Double::valueOf)
          .put(TypeName.DECIMAL, BigDecimal::new)
          .put(TypeName.BOOLEAN, Boolean::valueOf)
          .put(TypeName.STRING, str -> str)
          .put(
              TypeName.DATETIME,
              str -> {
                if (str == null || str.length() == 0) {
                  return null;
                }
                if (str.endsWith("UTC")) {
                  return BIGQUERY_TIMESTAMP_PARSER.parseDateTime(str).toDateTime(DateTimeZone.UTC);
                } else {
                  return new DateTime(
                      (long) (Double.parseDouble(str) * 1000), ISOChronology.getInstanceUTC());
                }
              })
          .put(TypeName.BYTES, str -> BaseEncoding.base64().decode(str))
          .build();

  // TODO: BigQuery code should not be relying on Calcite metadata fields. If so, this belongs
  // in the SQL package.
  private static final Map<String, StandardSQLTypeName> BEAM_TO_BIGQUERY_LOGICAL_MAPPING =
      ImmutableMap.<String, StandardSQLTypeName>builder()
          .put(SqlTypes.DATE.getIdentifier(), StandardSQLTypeName.DATE)
          .put(SqlTypes.TIME.getIdentifier(), StandardSQLTypeName.TIME)
          .put("SqlTimeWithLocalTzType", StandardSQLTypeName.TIME)
          .put("SqlTimestampWithLocalTzType", StandardSQLTypeName.DATETIME)
          .put("SqlCharType", StandardSQLTypeName.STRING)
          .build();

  private static final String BIGQUERY_MAP_KEY_FIELD_NAME = "key";
  private static final String BIGQUERY_MAP_VALUE_FIELD_NAME = "value";

  /**
   * Get the corresponding BigQuery {@link StandardSQLTypeName} for supported Beam {@link
   * FieldType}.
   */
  private static StandardSQLTypeName toStandardSQLTypeName(FieldType fieldType) {
    StandardSQLTypeName ret;
    if (fieldType.getTypeName().isLogicalType()) {
      ret = BEAM_TO_BIGQUERY_LOGICAL_MAPPING.get(fieldType.getLogicalType().getIdentifier());
      if (ret == null) {
        throw new IllegalArgumentException(
            "Cannot convert Beam logical type: "
                + fieldType.getLogicalType().getIdentifier()
                + " to BigQuery type.");
      }
    } else {
      ret = BEAM_TO_BIGQUERY_TYPE_MAPPING.get(fieldType.getTypeName());
      if (ret == null) {
        throw new IllegalArgumentException(
            "Cannot convert Beam type: " + fieldType.getTypeName() + " to BigQuery type.");
      }
    }
    return ret;
  }

  /**
   * Get the Beam {@link FieldType} from a BigQuery type name.
   *
   * <p>Supports both standard and legacy SQL types.
   *
   * @param typeName Name of the type
   * @param nestedFields Nested fields for the given type (eg. RECORD type)
   * @return Corresponding Beam {@link FieldType}
   */
  @Experimental(Kind.SCHEMAS)
  private static FieldType fromTableFieldSchemaType(
      String typeName, List<TableFieldSchema> nestedFields) {
    switch (typeName) {
      case "STRING":
        return FieldType.STRING;
      case "BYTES":
        return FieldType.BYTES;
      case "INT64":
      case "INTEGER":
        return FieldType.INT64;
      case "FLOAT64":
      case "FLOAT":
        return FieldType.DOUBLE;
      case "BOOL":
      case "BOOLEAN":
        return FieldType.BOOLEAN;
      case "TIMESTAMP":
        return FieldType.DATETIME;
      case "TIME":
        return FieldType.logicalType(SqlTypes.TIME);
      case "DATE":
        return FieldType.logicalType(SqlTypes.DATE);
      case "DATETIME":
        // TODO[BEAM-10240]: map to the new logical type when ZetaSQL DATETIME type is supported
        return FieldType.logicalType(
            new PassThroughLogicalType<Instant>(
                "SqlTimestampWithLocalTzType", FieldType.STRING, "", FieldType.DATETIME) {});
      case "STRUCT":
      case "RECORD":
        // check if record represents a map entry
        if (nestedFields.size() == 2) {
          TableFieldSchema key = nestedFields.get(0);
          TableFieldSchema value = nestedFields.get(1);
          if (BIGQUERY_MAP_KEY_FIELD_NAME.equals(key.getName())
              && BIGQUERY_MAP_VALUE_FIELD_NAME.equals(value.getName())) {
            return FieldType.map(
                fromTableFieldSchemaType(key.getType(), key.getFields()),
                fromTableFieldSchemaType(value.getType(), value.getFields()));
          }
        }

        Schema rowSchema = fromTableFieldSchema(nestedFields);
        return FieldType.row(rowSchema);
      default:
        throw new UnsupportedOperationException(
            "Converting BigQuery type " + typeName + " to Beam type is unsupported");
    }
  }

  private static Schema fromTableFieldSchema(List<TableFieldSchema> tableFieldSchemas) {
    Schema.Builder schemaBuilder = Schema.builder();
    for (TableFieldSchema tableFieldSchema : tableFieldSchemas) {
      FieldType fieldType =
          fromTableFieldSchemaType(tableFieldSchema.getType(), tableFieldSchema.getFields());

      Optional<Mode> fieldMode = Optional.ofNullable(tableFieldSchema.getMode()).map(Mode::valueOf);
      if (fieldMode.filter(m -> m == Mode.REPEATED).isPresent()
          && !fieldType.getTypeName().isMapType()) {
        fieldType = FieldType.array(fieldType);
      }

      // if the mode is not defined or if it is set to NULLABLE, then the field is nullable
      boolean nullable =
          !fieldMode.isPresent() || fieldMode.filter(m -> m == Mode.NULLABLE).isPresent();
      Field field = Field.of(tableFieldSchema.getName(), fieldType).withNullable(nullable);
      if (tableFieldSchema.getDescription() != null
          && !"".equals(tableFieldSchema.getDescription())) {
        field = field.withDescription(tableFieldSchema.getDescription());
      }
      schemaBuilder.addField(field);
    }
    return schemaBuilder.build();
  }

  private static List<TableFieldSchema> toTableFieldSchema(Schema schema) {
    List<TableFieldSchema> fields = new ArrayList<>(schema.getFieldCount());
    for (Field schemaField : schema.getFields()) {
      FieldType type = schemaField.getType();

      TableFieldSchema field = new TableFieldSchema().setName(schemaField.getName());
      if (schemaField.getDescription() != null && !"".equals(schemaField.getDescription())) {
        field.setDescription(schemaField.getDescription());
      }

      if (!schemaField.getType().getNullable()) {
        field.setMode(Mode.REQUIRED.toString());
      }
      if (type.getTypeName().isCollectionType()) {
        type = type.getCollectionElementType();
        if (type.getTypeName().isCollectionType() || type.getTypeName().isMapType()) {
          throw new IllegalArgumentException("Array of collection is not supported in BigQuery.");
        }
        field.setMode(Mode.REPEATED.toString());
      }
      if (TypeName.ROW == type.getTypeName()) {
        Schema subType = type.getRowSchema();
        field.setFields(toTableFieldSchema(subType));
      }
      if (TypeName.MAP == type.getTypeName()) {
        Schema mapSchema =
            Schema.builder()
                .addField(BIGQUERY_MAP_KEY_FIELD_NAME, type.getMapKeyType())
                .addField(BIGQUERY_MAP_VALUE_FIELD_NAME, type.getMapValueType())
                .build();
        type = FieldType.row(mapSchema);
        field.setFields(toTableFieldSchema(mapSchema));
        field.setMode(Mode.REPEATED.toString());
      }
      field.setType(toStandardSQLTypeName(type).toString());

      fields.add(field);
    }
    return fields;
  }

  /** Convert a Beam {@link Schema} to a BigQuery {@link TableSchema}. */
  @Experimental(Kind.SCHEMAS)
  public static TableSchema toTableSchema(Schema schema) {
    return new TableSchema().setFields(toTableFieldSchema(schema));
  }

  /** Convert a BigQuery {@link TableSchema} to a Beam {@link Schema}. */
  @Experimental(Kind.SCHEMAS)
  public static Schema fromTableSchema(TableSchema tableSchema) {
    return fromTableFieldSchema(tableSchema.getFields());
  }

  /** Convert a list of BigQuery {@link TableFieldSchema} to Avro {@link org.apache.avro.Schema}. */
  @Experimental(Kind.SCHEMAS)
  public static org.apache.avro.Schema toGenericAvroSchema(
      String schemaName, List<TableFieldSchema> fieldSchemas) {
    return BigQueryAvroUtils.toGenericAvroSchema(schemaName, fieldSchemas);
  }

  private static final BigQueryIO.TypedRead.ToBeamRowFunction<TableRow>
      TABLE_ROW_TO_BEAM_ROW_FUNCTION = beamSchema -> (TableRow tr) -> toBeamRow(beamSchema, tr);

  public static final BigQueryIO.TypedRead.ToBeamRowFunction<TableRow> tableRowToBeamRow() {
    return TABLE_ROW_TO_BEAM_ROW_FUNCTION;
  }

  private static final BigQueryIO.TypedRead.FromBeamRowFunction<TableRow>
      TABLE_ROW_FROM_BEAM_ROW_FUNCTION = ignored -> BigQueryUtils::toTableRow;

  public static final BigQueryIO.TypedRead.FromBeamRowFunction<TableRow> tableRowFromBeamRow() {
    return TABLE_ROW_FROM_BEAM_ROW_FUNCTION;
  }

  private static final SerializableFunction<Row, TableRow> ROW_TO_TABLE_ROW =
      new ToTableRow<>(SerializableFunctions.identity());

  /** Convert a Beam {@link Row} to a BigQuery {@link TableRow}. */
  public static SerializableFunction<Row, TableRow> toTableRow() {
    return ROW_TO_TABLE_ROW;
  }

  /** Convert a Beam schema type to a BigQuery {@link TableRow}. */
  public static <T> SerializableFunction<T, TableRow> toTableRow(
      SerializableFunction<T, Row> toRow) {
    return new ToTableRow<>(toRow);
  }

  /** Convert a Beam {@link Row} to a BigQuery {@link TableRow}. */
  private static class ToTableRow<T> implements SerializableFunction<T, TableRow> {
    private final SerializableFunction<T, Row> toRow;

    ToTableRow(SerializableFunction<T, Row> toRow) {
      this.toRow = toRow;
    }

    @Override
    public TableRow apply(T input) {
      return toTableRow(toRow.apply(input));
    }
  }

  @Experimental(Kind.SCHEMAS)
  public static Row toBeamRow(GenericRecord record, Schema schema, ConversionOptions options) {
    List<Object> valuesInOrder =
        schema.getFields().stream()
            .map(
                field -> {
                  try {
                    return convertAvroFormat(field.getType(), record.get(field.getName()), options);
                  } catch (Exception cause) {
                    throw new IllegalArgumentException(
                        "Error converting field " + field + ": " + cause.getMessage(), cause);
                  }
                })
            .collect(toList());

    return Row.withSchema(schema).addValues(valuesInOrder).build();
  }

  public static TableRow convertGenericRecordToTableRow(
      GenericRecord record, TableSchema tableSchema) {
    return BigQueryAvroUtils.convertGenericRecordToTableRow(record, tableSchema);
  }

  /** Convert a BigQuery TableRow to a Beam Row. */
  public static TableRow toTableRow(Row row) {
    TableRow output = new TableRow();
    for (int i = 0; i < row.getFieldCount(); i++) {
      Object value = row.getValue(i);
      Field schemaField = row.getSchema().getField(i);
      output = output.set(schemaField.getName(), fromBeamField(schemaField.getType(), value));
    }
    return output;
  }

  private static @Nullable Object fromBeamField(FieldType fieldType, Object fieldValue) {
    if (fieldValue == null) {
      if (!fieldType.getNullable()) {
        throw new IllegalArgumentException("Field is not nullable.");
      }
      return null;
    }

    switch (fieldType.getTypeName()) {
      case ARRAY:
      case ITERABLE:
        FieldType elementType = fieldType.getCollectionElementType();
        Iterable<?> items = (Iterable<?>) fieldValue;
        List<Object> convertedItems = Lists.newArrayListWithCapacity(Iterables.size(items));
        for (Object item : items) {
          convertedItems.add(fromBeamField(elementType, item));
        }
        return convertedItems;

      case MAP:
        FieldType keyElementType = fieldType.getMapKeyType();
        FieldType valueElementType = fieldType.getMapValueType();
        Map<?, ?> pairs = (Map<?, ?>) fieldValue;
        convertedItems = Lists.newArrayListWithCapacity(pairs.size());
        for (Map.Entry<?, ?> pair : pairs.entrySet()) {
          convertedItems.add(
              new TableRow()
                  .set(BIGQUERY_MAP_KEY_FIELD_NAME, fromBeamField(keyElementType, pair.getKey()))
                  .set(
                      BIGQUERY_MAP_VALUE_FIELD_NAME,
                      fromBeamField(valueElementType, pair.getValue())));
        }
        return convertedItems;

      case ROW:
        return toTableRow((Row) fieldValue);

      case DATETIME:
        return ((Instant) fieldValue)
            .toDateTime(DateTimeZone.UTC)
            .toString(BIGQUERY_TIMESTAMP_PRINTER);

      case INT16:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BOOLEAN:
        return fieldValue.toString();

      case DECIMAL:
        return fieldValue.toString();

      case BYTES:
        return BaseEncoding.base64().encode((byte[]) fieldValue);

      case LOGICAL_TYPE:
        // For the JSON formats of DATE/DATETIME/TIME/TIMESTAMP types that BigQuery accepts, see
        // https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json#details_of_loading_json_data
        String identifier = fieldType.getLogicalType().getIdentifier();
        // TODO[BEAM-10240]: unparse (toString() is not enough) LocalDateTime here for DATETIME type
        if (SqlTypes.DATE.getIdentifier().equals(identifier)
            || SqlTypes.TIME.getIdentifier().equals(identifier)) {
          return fieldValue.toString();
        } // fall through

      default:
        return fieldValue;
    }
  }

  /**
   * Tries to convert a JSON {@link TableRow} from BigQuery into a Beam {@link Row}.
   *
   * <p>Only supports basic types and arrays. Doesn't support date types or structs.
   */
  @Experimental(Kind.SCHEMAS)
  public static Row toBeamRow(Schema rowSchema, TableRow jsonBqRow) {
    // TODO deprecate toBeamRow(Schema, TableSchema, TableRow) function in favour of this function.
    // This function attempts to convert TableRows without  having access to the
    // corresponding TableSchema because:
    // 1. TableSchema contains redundant information already available in the Schema object.
    // 2. TableSchema objects are not serializable and are therefore harder to propagate through a
    // pipeline.
    return rowSchema.getFields().stream()
        .map(field -> toBeamRowFieldValue(field, jsonBqRow.get(field.getName())))
        .collect(toRow(rowSchema));
  }

  private static Object toBeamRowFieldValue(Field field, Object bqValue) {
    if (bqValue == null) {
      if (field.getType().getNullable()) {
        return null;
      } else {
        throw new IllegalArgumentException(
            "Received null value for non-nullable field " + field.getName());
      }
    }

    return toBeamValue(field.getType(), bqValue);
  }

  /**
   * Tries to parse the JSON {@link TableRow} from BigQuery.
   *
   * <p>Only supports basic types and arrays. Doesn't support date types.
   */
  @Experimental(Kind.SCHEMAS)
  public static Row toBeamRow(Schema rowSchema, TableSchema bqSchema, TableRow jsonBqRow) {
    List<TableFieldSchema> bqFields = bqSchema.getFields();

    Map<String, Integer> bqFieldIndices =
        IntStream.range(0, bqFields.size())
            .boxed()
            .collect(toMap(i -> bqFields.get(i).getName(), i -> i));

    List<Object> rawJsonValues =
        rowSchema.getFields().stream()
            .map(field -> bqFieldIndices.get(field.getName()))
            .map(index -> jsonBqRow.getF().get(index).getV())
            .collect(toList());

    return IntStream.range(0, rowSchema.getFieldCount())
        .boxed()
        .map(index -> toBeamValue(rowSchema.getField(index).getType(), rawJsonValues.get(index)))
        .collect(toRow(rowSchema));
  }

  private static Object toBeamValue(FieldType fieldType, Object jsonBQValue) {
    if (jsonBQValue instanceof String && JSON_VALUE_PARSERS.containsKey(fieldType.getTypeName())) {
      return JSON_VALUE_PARSERS.get(fieldType.getTypeName()).apply((String) jsonBQValue);
    }

    if (jsonBQValue instanceof List) {
      return ((List<Object>) jsonBQValue)
          .stream()
              .map(v -> ((Map<String, Object>) v).get("v"))
              .map(v -> toBeamValue(fieldType.getCollectionElementType(), v))
              .collect(toList());
    }

    if (jsonBQValue instanceof Map) {
      TableRow tr = new TableRow();
      tr.putAll((Map<String, Object>) jsonBQValue);
      return toBeamRow(fieldType.getRowSchema(), tr);
    }

    throw new UnsupportedOperationException(
        "Converting BigQuery type '"
            + jsonBQValue.getClass()
            + "' to '"
            + fieldType
            + "' is not supported");
  }

  // TODO: BigQuery shouldn't know about SQL internal logical types.
  private static final Set<String> SQL_DATE_TIME_TYPES =
      ImmutableSet.of("SqlTimeWithLocalTzType", "SqlTimestampWithLocalTzType");
  private static final Set<String> SQL_STRING_TYPES = ImmutableSet.of("SqlCharType");

  /**
   * Tries to convert an Avro decoded value to a Beam field value based on the target type of the
   * Beam field.
   */
  public static Object convertAvroFormat(
      FieldType beamFieldType, Object avroValue, BigQueryUtils.ConversionOptions options) {
    TypeName beamFieldTypeName = beamFieldType.getTypeName();
    if (avroValue == null) {
      if (beamFieldType.getNullable()) {
        return null;
      } else {
        throw new IllegalArgumentException(String.format("Field %s not nullable", beamFieldType));
      }
    }
    switch (beamFieldTypeName) {
      case BYTE:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BYTES:
      case BOOLEAN:
        return convertAvroPrimitiveTypes(beamFieldTypeName, avroValue);
      case DATETIME:
        // Expecting value in microseconds.
        switch (options.getTruncateTimestamps()) {
          case TRUNCATE:
            return truncateToMillis(avroValue);
          case REJECT:
            return safeToMillis(avroValue);
          default:
            throw new IllegalArgumentException(
                String.format(
                    "Unknown timestamp truncation option: %s", options.getTruncateTimestamps()));
        }
      case ARRAY:
        return convertAvroArray(beamFieldType, avroValue, options);
      case LOGICAL_TYPE:
        String identifier = beamFieldType.getLogicalType().getIdentifier();
        if (SqlTypes.DATE.getIdentifier().equals(identifier)) {
          return convertAvroDate(avroValue);
        } else if (SqlTypes.TIME.getIdentifier().equals(identifier)) {
          return convertAvroTime(avroValue);
        } else if (SQL_DATE_TIME_TYPES.contains(identifier)) {
          switch (options.getTruncateTimestamps()) {
            case TRUNCATE:
              return truncateToMillis(avroValue);
            case REJECT:
              return safeToMillis(avroValue);
            default:
              throw new IllegalArgumentException(
                  String.format(
                      "Unknown timestamp truncation option: %s", options.getTruncateTimestamps()));
          }
        } else if (SQL_STRING_TYPES.contains(identifier)) {
          return convertAvroPrimitiveTypes(TypeName.STRING, avroValue);
        } else {
          throw new RuntimeException("Unknown logical type " + identifier);
        }
      case ROW:
        Schema rowSchema = beamFieldType.getRowSchema();
        if (rowSchema == null) {
          throw new IllegalArgumentException("Nested ROW missing row schema");
        }
        GenericData.Record record = (GenericData.Record) avroValue;
        return toBeamRow(record, rowSchema, options);
      case DECIMAL:
        throw new RuntimeException("Does not support converting DECIMAL type value");
      case MAP:
        return convertAvroRecordToMap(beamFieldType, avroValue, options);
      default:
        throw new RuntimeException(
            "Does not support converting unknown type value: " + beamFieldTypeName);
    }
  }

  private static ReadableInstant safeToMillis(Object value) {
    long subMilliPrecision = ((long) value) % 1000;
    if (subMilliPrecision != 0) {
      throw new IllegalArgumentException(
          String.format(
              "BigQuery data contained value %s with sub-millisecond precision, which Beam does"
                  + " not currently support."
                  + " You can enable truncating timestamps to millisecond precision"
                  + " by using BigQueryIO.withTruncatedTimestamps",
              value));
    } else {
      return truncateToMillis(value);
    }
  }

  private static ReadableInstant truncateToMillis(Object value) {
    return new Instant((long) value / 1000);
  }

  private static Object convertAvroArray(
      FieldType beamField, Object value, BigQueryUtils.ConversionOptions options) {
    // Check whether the type of array element is equal.
    List<Object> values = (List<Object>) value;
    List<Object> ret = new ArrayList<>();
    FieldType collectionElement = beamField.getCollectionElementType();
    for (Object v : values) {
      ret.add(convertAvroFormat(collectionElement, v, options));
    }
    return ret;
  }

  private static Object convertAvroRecordToMap(
      FieldType beamField, Object value, BigQueryUtils.ConversionOptions options) {
    List<GenericData.Record> records = (List<GenericData.Record>) value;
    ImmutableMap.Builder<Object, Object> ret = ImmutableMap.builder();
    FieldType keyElement = beamField.getMapKeyType();
    FieldType valueElement = beamField.getMapValueType();
    for (GenericData.Record record : records) {
      ret.put(
          convertAvroFormat(keyElement, record.get(0), options),
          convertAvroFormat(valueElement, record.get(1), options));
    }
    return ret.build();
  }

  private static Object convertAvroPrimitiveTypes(TypeName beamType, Object value) {
    switch (beamType) {
      case BYTE:
        return ((Long) value).byteValue();
      case INT16:
        return ((Long) value).shortValue();
      case INT32:
        return ((Long) value).intValue();
      case INT64:
        return value; // Long
      case FLOAT:
        return ((Double) value).floatValue();
      case DOUBLE:
        return value; // Double
      case BOOLEAN:
        return value; // Boolean
      case DECIMAL:
        throw new RuntimeException("Does not support converting DECIMAL type value");
      case STRING:
        return convertAvroString(value);
      case BYTES:
        return convertAvroBytes(value);
      default:
        throw new RuntimeException(beamType + " is not primitive type.");
    }
  }

  private static Object convertAvroString(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof org.apache.avro.util.Utf8) {
      return ((org.apache.avro.util.Utf8) value).toString();
    } else if (value instanceof String) {
      return value;
    } else {
      throw new RuntimeException(
          "Does not support converting avro format: " + value.getClass().getName());
    }
  }

  private static Object convertAvroBytes(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof ByteBuffer) {
      ByteBuffer bf = (ByteBuffer) value;
      byte[] result = new byte[bf.limit()];
      bf.get(result);
      return result;
    } else {
      throw new RuntimeException(
          "Does not support converting avro format: " + value.getClass().getName());
    }
  }

  private static Object convertAvroDate(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Integer) {
      return LocalDate.ofEpochDay((Integer) value);
    } else {
      throw new RuntimeException(
          "Does not support converting avro format: " + value.getClass().getName());
    }
  }

  private static Object convertAvroTime(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Long) {
      return LocalTime.ofNanoOfDay((Long) value * 1000);
    } else {
      throw new RuntimeException(
          "Does not support converting avro format: " + value.getClass().getName());
    }
  }
}
