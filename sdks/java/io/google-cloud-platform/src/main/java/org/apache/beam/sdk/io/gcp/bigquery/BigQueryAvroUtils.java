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

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * A set of utilities for working with Avro files.
 *
 * <p>These utilities are based on the <a
 * href="https://avro.apache.org/docs/1.8.1/spec.html">Avro 1.8.1</a> specification.
 */
class BigQueryAvroUtils {

  public static final ImmutableMap<String, Type> BIG_QUERY_TO_AVRO_TYPES =
      ImmutableMap.<String, Type>builder()
          .put("STRING", Type.STRING)
          .put("BYTES", Type.BYTES)
          .put("INTEGER", Type.LONG)
          .put("FLOAT", Type.DOUBLE)
          .put("BOOLEAN", Type.BOOLEAN)
          .put("TIMESTAMP", Type.LONG)
          .put("RECORD", Type.RECORD)
          .put("DATE", Type.STRING)
          .put("DATETIME", Type.STRING)
          .put("TIME", Type.STRING)
          .build();
  /**
   * Formats BigQuery seconds-since-epoch into String matching JSON export. Thread-safe and
   * immutable.
   */
  private static final DateTimeFormatter DATE_AND_SECONDS_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();
  // Package private for BigQueryTableRowIterator to use.
  static String formatTimestamp(String timestamp) {
    // timestamp is in "seconds since epoch" format, with scientific notation.
    // e.g., "1.45206229112345E9" to mean "2016-01-06 06:38:11.123456 UTC".
    // Separate into seconds and microseconds.
    double timestampDoubleMicros = Double.parseDouble(timestamp) * 1000000;
    long timestampMicros = (long) timestampDoubleMicros;
    long seconds = timestampMicros / 1000000;
    int micros = (int) (timestampMicros % 1000000);
    String dayAndTime = DATE_AND_SECONDS_FORMATTER.print(seconds * 1000);

    // No sub-second component.
    if (micros == 0) {
      return String.format("%s UTC", dayAndTime);
    }

    // Sub-second component.
    int digits = 6;
    int subsecond = micros;
    while (subsecond % 10 == 0) {
      digits--;
      subsecond /= 10;
    }
    String formatString = String.format("%%0%dd", digits);
    String fractionalSeconds = String.format(formatString, subsecond);
    return String.format("%s.%s UTC", dayAndTime, fractionalSeconds);
  }

  /**
   * Utility function to convert from an Avro {@link GenericRecord} to a BigQuery {@link TableRow}.
   *
   * <p>See <a href="https://cloud.google.com/bigquery/exporting-data-from-bigquery#config">
   * "Avro format"</a> for more information.
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

  @Nullable
  private static Object getTypedCellValue(Schema schema, TableFieldSchema fieldSchema, Object v) {
    // Per https://cloud.google.com/bigquery/docs/reference/v2/tables#schema, the mode field
    // is optional (and so it may be null), but defaults to "NULLABLE".
    String mode = firstNonNull(fieldSchema.getMode(), "NULLABLE");
    switch (mode) {
      case "REQUIRED":
        return convertRequiredField(schema.getType(), fieldSchema, v);
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
      return ImmutableList.of();
    }
    @SuppressWarnings("unchecked")
    List<Object> elements = (List<Object>) v;
    ImmutableList.Builder<Object> values = ImmutableList.builder();
    Type elementType = schema.getElementType().getType();
    for (Object element : elements) {
      values.add(convertRequiredField(elementType, fieldSchema, element));
    }
    return values.build();
  }

  private static Object convertRequiredField(
      Type avroType, TableFieldSchema fieldSchema, Object v) {
    // REQUIRED fields are represented as the corresponding Avro types. For example, a BigQuery
    // INTEGER type maps to an Avro LONG type.
    checkNotNull(v, "REQUIRED field %s should not be null", fieldSchema.getName());
    // Per https://cloud.google.com/bigquery/docs/reference/v2/tables#schema, the type field
    // is required, so it may not be null.
    String bqType = fieldSchema.getType();
    Type expectedAvroType = BIG_QUERY_TO_AVRO_TYPES.get(bqType);
    verifyNotNull(expectedAvroType, "Unsupported BigQuery type: %s", bqType);
    verify(
        avroType == expectedAvroType,
        "Expected Avro schema type %s, not %s, for BigQuery %s field %s",
        expectedAvroType,
        avroType,
        bqType,
        fieldSchema.getName());
    switch (fieldSchema.getType()) {
      case "STRING":
      case "DATE":
      case "DATETIME":
      case "TIME":
        // Avro will use a CharSequence to represent String objects, but it may not always use
        // java.lang.String; for example, it may prefer org.apache.avro.util.Utf8.
        verify(v instanceof CharSequence, "Expected CharSequence (String), got %s", v.getClass());
        return v.toString();
      case "INTEGER":
        verify(v instanceof Long, "Expected Long, got %s", v.getClass());
        return ((Long) v).toString();
      case "FLOAT":
        verify(v instanceof Double, "Expected Double, got %s", v.getClass());
        return v;
      case "BOOLEAN":
        verify(v instanceof Boolean, "Expected Boolean, got %s", v.getClass());
        return v;
      case "TIMESTAMP":
        // TIMESTAMP data types are represented as Avro LONG types. They are converted back to
        // Strings with variable-precision (up to six digits) to match the JSON files export
        // by BigQuery.
        verify(v instanceof Long, "Expected Long, got %s", v.getClass());
        Double doubleValue = ((Long) v) / 1000000.0;
        return formatTimestamp(doubleValue.toString());
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
                fieldSchema.getType(),
                fieldSchema.getName()));
    }
  }

  @Nullable
  private static Object convertNullableField(
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
      return convertRequiredField(firstType, fieldSchema, v);
    }
    return convertRequiredField(unionTypes.get(1).getType(), fieldSchema, v);
  }

  static Schema toGenericAvroSchema(String schemaName, List<TableFieldSchema> fieldSchemas) {
    List<Field> avroFields = new ArrayList<>();
    for (TableFieldSchema bigQueryField : fieldSchemas) {
      avroFields.add(convertField(bigQueryField));
    }
    return Schema.createRecord(
        schemaName,
        "org.apache.beam.sdk.io.gcp.bigquery",
        "Translated Avro Schema for " + schemaName,
        false,
        avroFields);
  }

  private static Field convertField(TableFieldSchema bigQueryField) {
    Type avroType = BIG_QUERY_TO_AVRO_TYPES.get(bigQueryField.getType());
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
