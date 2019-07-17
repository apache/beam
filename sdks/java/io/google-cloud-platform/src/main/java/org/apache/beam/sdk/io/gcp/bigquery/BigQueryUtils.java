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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.LogicalTypes;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.joda.time.DateTime;
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
              str ->
                  new DateTime(
                      (long) (Double.parseDouble(str) * 1000), ISOChronology.getInstanceUTC()))
          .put(TypeName.BYTES, str -> BaseEncoding.base64().decode(str))
          .build();

  // TODO: BigQuery code should not be relying on Calcite metadata fields. If so, this belongs
  // in the SQL package.
  private static final Map<String, StandardSQLTypeName> BEAM_TO_BIGQUERY_LOGICAL_MAPPING =
      ImmutableMap.<String, StandardSQLTypeName>builder()
          .put("SqlDateType", StandardSQLTypeName.DATE)
          .put("SqlTimeType", StandardSQLTypeName.TIME)
          .put("SqlTimeWithLocalTzType", StandardSQLTypeName.TIME)
          .put("SqlTimestampWithLocalTzType", StandardSQLTypeName.DATETIME)
          .put("SqlCharType", StandardSQLTypeName.STRING)
          .build();

  /**
   * Get the corresponding BigQuery {@link StandardSQLTypeName} for supported Beam {@link
   * FieldType}.
   */
  private static StandardSQLTypeName toStandardSQLTypeName(FieldType fieldType) {
    if (fieldType.getTypeName().isLogicalType()) {
      StandardSQLTypeName foundType =
          BEAM_TO_BIGQUERY_LOGICAL_MAPPING.get(fieldType.getLogicalType().getIdentifier());
      if (foundType != null) {
        return foundType;
      }
    }
    return BEAM_TO_BIGQUERY_TYPE_MAPPING.get(fieldType.getTypeName());
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
        return FieldType.logicalType(
            new LogicalTypes.PassThroughLogicalType<Instant>(
                "SqlTimeType", "", FieldType.DATETIME) {});
      case "DATE":
        return FieldType.logicalType(
            new LogicalTypes.PassThroughLogicalType<Instant>(
                "SqlDateType", "", FieldType.DATETIME) {});
      case "DATETIME":
        return FieldType.logicalType(
            new LogicalTypes.PassThroughLogicalType<Instant>(
                "SqlTimestampWithLocalTzType", "", FieldType.DATETIME) {});
      case "STRUCT":
      case "RECORD":
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
      if (fieldMode.filter(m -> m == Mode.REPEATED).isPresent()) {
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
      if (TypeName.ARRAY == type.getTypeName()) {
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
        throw new IllegalArgumentException("Maps are not supported in BigQuery.");
      }
      field.setType(toStandardSQLTypeName(type).toString());

      fields.add(field);
    }
    return fields;
  }

  /** Convert a Beam {@link Schema} to a BigQuery {@link TableSchema}. */
  public static TableSchema toTableSchema(Schema schema) {
    return new TableSchema().setFields(toTableFieldSchema(schema));
  }

  /** Convert a BigQuery {@link TableSchema} to a Beam {@link Schema}. */
  public static Schema fromTableSchema(TableSchema tableSchema) {
    return fromTableFieldSchema(tableSchema.getFields());
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
      new ToTableRow(SerializableFunctions.identity());

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

  public static Row toBeamRow(GenericRecord record, Schema schema, ConversionOptions options) {
    List<Object> valuesInOrder =
        schema.getFields().stream()
            .map(field -> convertAvroFormat(field, record.get(field.getName()), options))
            .collect(toList());

    return Row.withSchema(schema).addValues(valuesInOrder).build();
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

  private static Object fromBeamField(FieldType fieldType, Object fieldValue) {
    if (fieldValue == null) {
      if (!fieldType.getNullable()) {
        throw new IllegalArgumentException("Field is not nullable.");
      }
      return null;
    }

    switch (fieldType.getTypeName()) {
      case ARRAY:
        FieldType elementType = fieldType.getCollectionElementType();
        List items = (List) fieldValue;
        List convertedItems = Lists.newArrayListWithCapacity(items.size());
        for (Object item : items) {
          convertedItems.add(fromBeamField(elementType, item));
        }
        return convertedItems;

      case ROW:
        return toTableRow((Row) fieldValue);

      case DATETIME:
        DateTimeFormatter patternFormat =
            new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
                .toFormatter();
        return ((Instant) fieldValue).toDateTime().toString(patternFormat);

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

      default:
        return fieldValue;
    }
  }

  /**
   * Tries to convert a JSON {@link TableRow} from BigQuery into a Beam {@link Row}.
   *
   * <p>Only supports basic types and arrays. Doesn't support date types or structs.
   */
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
      ImmutableSet.of(
          "SqlDateType", "SqlTimeType", "SqlTimeWithLocalTzType", "SqlTimestampWithLocalTzType");
  private static final Set<String> SQL_STRING_TYPES = ImmutableSet.of("SqlCharType");

  /**
   * Tries to convert an Avro decoded value to a Beam field value based on the target type of the
   * Beam field.
   */
  public static Object convertAvroFormat(
      Field beamField, Object avroValue, BigQueryUtils.ConversionOptions options) {
    TypeName beamFieldTypeName = beamField.getType().getTypeName();
    if (avroValue == null) {
      if (beamField.getType().getNullable()) {
        return null;
      } else {
        throw new IllegalArgumentException(
            String.format("Field %s not nullable", beamField.getName()));
      }
    }
    switch (beamFieldTypeName) {
      case INT16:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case BYTE:
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
      case STRING:
        return convertAvroPrimitiveTypes(beamFieldTypeName, avroValue);
      case ARRAY:
        return convertAvroArray(beamField, avroValue);
      case LOGICAL_TYPE:
        String identifier = beamField.getType().getLogicalType().getIdentifier();
        if (SQL_DATE_TIME_TYPES.contains(identifier)) {
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
      case DECIMAL:
        throw new RuntimeException("Does not support converting DECIMAL type value");
      case MAP:
        throw new RuntimeException("Does not support converting MAP type value");
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

  private static Object convertAvroArray(Field beamField, Object value) {
    // Check whether the type of array element is equal.
    List<Object> values = (List<Object>) value;
    List<Object> ret = new ArrayList();
    for (Object v : values) {
      ret.add(
          convertAvroPrimitiveTypes(
              beamField.getType().getCollectionElementType().getTypeName(), v));
    }
    return (Object) ret;
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

  private static Object convertAvroPrimitiveTypes(TypeName beamType, Object value) {
    switch (beamType) {
      case BYTE:
        return ((Long) value).byteValue();
      case INT16:
        return ((Long) value).shortValue();
      case INT32:
        return ((Long) value).intValue();
      case INT64:
        return value;
      case FLOAT:
        return ((Double) value).floatValue();
      case DOUBLE:
        return (Double) value;
      case BOOLEAN:
        return (Boolean) value;
      case DECIMAL:
        throw new RuntimeException("Does not support converting DECIMAL type value");
      case STRING:
        return convertAvroString(value);
      default:
        throw new RuntimeException(beamType + " is not primitive type.");
    }
  }
}
