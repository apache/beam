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
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.DATETIME_SPACE_FORMATTER;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.TIMESTAMP_FORMATTER;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.AnnotationsProto;
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.BigQuerySchemaUtil;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Functions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Days;

/**
 * Utility methods for converting JSON {@link TableRow} objects to dynamic protocol message, for use
 * with the Storage write API.
 */
public class TableRowToStorageApiProto {
  abstract static class SchemaConversionException extends Exception {
    SchemaConversionException(String msg) {
      super(msg);
    }

    SchemaConversionException(String msg, Exception e) {
      super(msg, e);
    }
  }

  public static class SchemaTooNarrowException extends SchemaConversionException {
    SchemaTooNarrowException(String msg) {
      super(msg);
    }
  }

  public static class SchemaDoesntMatchException extends SchemaConversionException {
    SchemaDoesntMatchException(String msg) {
      super(msg);
    }

    SchemaDoesntMatchException(String msg, Exception e) {
      super(msg + ". Exception: " + e, e);
    }
  }

  public static class SingleValueConversionException extends SchemaConversionException {
    SingleValueConversionException(
        Object sourceValue, TableFieldSchema.Type type, String fullName, Exception e) {
      super(
          "Column: "
              + getPrettyFieldName(fullName)
              + " ("
              + type
              + "). "
              + "Value: "
              + sourceValue
              + " ("
              + sourceValue.getClass().getName()
              + "). Reason: "
              + e);
    }

    private static String getPrettyFieldName(String fullName) {
      String rootPrefix = "root.";
      return fullName.startsWith(rootPrefix) ? fullName.substring(rootPrefix.length()) : fullName;
    }
  }

  ///////////////////////////////////
  // Conversion between TableSchema the json class and TableSchema the proto class.

  private static final Map<Mode, TableFieldSchema.Mode> MODE_MAP_JSON_PROTO =
      ImmutableMap.of(
          Mode.NULLABLE, TableFieldSchema.Mode.NULLABLE,
          Mode.REQUIRED, TableFieldSchema.Mode.REQUIRED,
          Mode.REPEATED, TableFieldSchema.Mode.REPEATED);
  private static final Map<TableFieldSchema.Mode, String> MODE_MAP_PROTO_JSON =
      ImmutableMap.of(
          TableFieldSchema.Mode.NULLABLE, "NULLABLE",
          TableFieldSchema.Mode.REQUIRED, "REQUIRED",
          TableFieldSchema.Mode.REPEATED, "REPEATED");

  private static final Map<String, TableFieldSchema.Type> TYPE_MAP_JSON_PROTO =
      ImmutableMap.<String, TableFieldSchema.Type>builder()
          .put("STRUCT", TableFieldSchema.Type.STRUCT)
          .put("RECORD", TableFieldSchema.Type.STRUCT)
          .put("INT64", TableFieldSchema.Type.INT64)
          .put("INTEGER", TableFieldSchema.Type.INT64)
          .put("FLOAT64", TableFieldSchema.Type.DOUBLE)
          .put("FLOAT", TableFieldSchema.Type.DOUBLE)
          .put("STRING", TableFieldSchema.Type.STRING)
          .put("BOOL", TableFieldSchema.Type.BOOL)
          .put("BOOLEAN", TableFieldSchema.Type.BOOL)
          .put("BYTES", TableFieldSchema.Type.BYTES)
          .put("NUMERIC", TableFieldSchema.Type.NUMERIC)
          .put("BIGNUMERIC", TableFieldSchema.Type.BIGNUMERIC)
          .put("GEOGRAPHY", TableFieldSchema.Type.GEOGRAPHY)
          .put("DATE", TableFieldSchema.Type.DATE)
          .put("TIME", TableFieldSchema.Type.TIME)
          .put("DATETIME", TableFieldSchema.Type.DATETIME)
          .put("TIMESTAMP", TableFieldSchema.Type.TIMESTAMP)
          .put("JSON", TableFieldSchema.Type.JSON)
          .build();
  private static final Map<TableFieldSchema.Type, String> TYPE_MAP_PROTO_JSON =
      ImmutableMap.<TableFieldSchema.Type, String>builder()
          .put(TableFieldSchema.Type.STRUCT, "STRUCT")
          .put(TableFieldSchema.Type.INT64, "INT64")
          .put(TableFieldSchema.Type.DOUBLE, "FLOAT64")
          .put(TableFieldSchema.Type.STRING, "STRING")
          .put(TableFieldSchema.Type.BOOL, "BOOL")
          .put(TableFieldSchema.Type.BYTES, "BYTES")
          .put(TableFieldSchema.Type.NUMERIC, "NUMERIC")
          .put(TableFieldSchema.Type.BIGNUMERIC, "BIGNUMERIC")
          .put(TableFieldSchema.Type.GEOGRAPHY, "GEOGRAPHY")
          .put(TableFieldSchema.Type.DATE, "DATE")
          .put(TableFieldSchema.Type.TIME, "TIME")
          .put(TableFieldSchema.Type.DATETIME, "DATETIME")
          .put(TableFieldSchema.Type.TIMESTAMP, "TIMESTAMP")
          .put(TableFieldSchema.Type.JSON, "JSON")
          .build();

  @FunctionalInterface
  public interface ThrowingBiFunction<FirstInputT, SecondInputT, OutputT> {
    OutputT apply(FirstInputT t, SecondInputT u) throws SchemaConversionException;
  }

  static final DecimalFormat DECIMAL_FORMAT =
      new DecimalFormat("0.0###############", DecimalFormatSymbols.getInstance(Locale.ROOT));

  // Map of functions to convert json values into the value expected in the Vortex proto object.
  static final Map<TableFieldSchema.Type, ThrowingBiFunction<String, Object, @Nullable Object>>
      TYPE_MAP_PROTO_CONVERTERS =
          ImmutableMap
              .<TableFieldSchema.Type, ThrowingBiFunction<String, Object, @Nullable Object>>
                  builder()
              .put(
                  TableFieldSchema.Type.INT64,
                  (fullName, value) -> {
                    if (value instanceof String) {
                      try {
                        return Long.valueOf((String) value);
                      } catch (NumberFormatException e) {
                        throw new SingleValueConversionException(
                            value, TableFieldSchema.Type.INT64, fullName, e);
                      }
                    } else if (value instanceof Integer || value instanceof Long) {
                      return ((Number) value).longValue();
                    } else if (value instanceof BigDecimal) {
                      try {
                        return ((BigDecimal) value).longValueExact();
                      } catch (ArithmeticException e) {
                        throw new SingleValueConversionException(
                            value, TableFieldSchema.Type.INT64, fullName, e);
                      }
                    } else if (value instanceof BigInteger) {
                      try {
                        return ((BigInteger) value).longValueExact();
                      } catch (ArithmeticException e) {
                        throw new SingleValueConversionException(
                            value, TableFieldSchema.Type.INT64, fullName, e);
                      }
                    }
                    return null;
                  })
              .put(
                  TableFieldSchema.Type.DOUBLE,
                  (schemaInformation, value) -> {
                    if (value instanceof String) {
                      return Double.valueOf((String) value);
                    } else if (value instanceof Number) {
                      return ((Number) value).doubleValue();
                    }
                    return null;
                  })
              .put(
                  TableFieldSchema.Type.BOOL,
                  (schemaInformation, value) -> {
                    if (value instanceof String) {
                      return Boolean.valueOf((String) value);
                    } else if (value instanceof Boolean) {
                      return value;
                    }
                    return null;
                  })
              .put(
                  TableFieldSchema.Type.BYTES,
                  (schemaInformation, value) -> {
                    if (value instanceof String) {
                      return ByteString.copyFrom(BaseEncoding.base64().decode((String) value));
                    } else if (value instanceof byte[]) {
                      return ByteString.copyFrom((byte[]) value);
                    } else if (value instanceof ByteString) {
                      return value;
                    }
                    return null;
                  })
              .put(
                  TableFieldSchema.Type.TIMESTAMP,
                  (schemaInformation, value) -> {
                    if (value instanceof String) {
                      try {
                        // '2011-12-03T10:15:30Z', '2011-12-03 10:15:30+05:00'
                        // '2011-12-03 10:15:30 UTC', '2011-12-03T10:15:30 America/New_York'
                        Instant timestamp = Instant.from(TIMESTAMP_FORMATTER.parse((String) value));
                        return toEpochMicros(timestamp);
                      } catch (DateTimeException e) {
                        try {
                          // for backwards compatibility, default time zone is UTC for values with
                          // no time-zone
                          // '2011-12-03T10:15:30'
                          Instant timestamp =
                              Instant.from(
                                  TIMESTAMP_FORMATTER
                                      .withZone(ZoneOffset.UTC)
                                      .parse((String) value));
                          return toEpochMicros(timestamp);
                        } catch (DateTimeParseException err) {
                          // "12345667"
                          Instant timestamp = Instant.ofEpochMilli(Long.parseLong((String) value));
                          return toEpochMicros(timestamp);
                        }
                      }
                    } else if (value instanceof Instant) {
                      return toEpochMicros((Instant) value);
                    } else if (value instanceof org.joda.time.Instant) {
                      // joda instant precision is millisecond
                      return ((org.joda.time.Instant) value).getMillis() * 1000L;
                    } else if (value instanceof Integer || value instanceof Long) {
                      return ((Number) value).longValue();
                    } else if (value instanceof Double || value instanceof Float) {
                      // assume value represents number of seconds since epoch
                      return BigDecimal.valueOf(((Number) value).doubleValue())
                          .scaleByPowerOfTen(6)
                          .setScale(0, RoundingMode.HALF_UP)
                          .longValue();
                    }
                    return null;
                  })
              .put(
                  TableFieldSchema.Type.DATE,
                  (schemaInformation, value) -> {
                    if (value instanceof String) {
                      return ((Long) LocalDate.parse((String) value).toEpochDay()).intValue();
                    } else if (value instanceof LocalDate) {
                      return ((Long) ((LocalDate) value).toEpochDay()).intValue();
                    } else if (value instanceof org.joda.time.LocalDate) {
                      return Days.daysBetween(
                              org.joda.time.Instant.EPOCH.toDateTime().toLocalDate(),
                              (org.joda.time.LocalDate) value)
                          .getDays();
                    } else if (value instanceof Integer || value instanceof Long) {
                      return ((Number) value).intValue();
                    }
                    return null;
                  })
              .put(
                  TableFieldSchema.Type.NUMERIC,
                  (schemaInformation, value) -> {
                    if (value instanceof String) {
                      return BigDecimalByteStringEncoder.encodeToNumericByteString(
                          new BigDecimal((String) value));
                    } else if (value instanceof BigDecimal) {
                      return BigDecimalByteStringEncoder.encodeToNumericByteString(
                          ((BigDecimal) value));
                    } else if (value instanceof Double || value instanceof Float) {
                      return BigDecimalByteStringEncoder.encodeToNumericByteString(
                          BigDecimal.valueOf(((Number) value).doubleValue()));
                    } else if (value instanceof Short
                        || value instanceof Integer
                        || value instanceof Long) {
                      return BigDecimalByteStringEncoder.encodeToNumericByteString(
                          BigDecimal.valueOf(((Number) value).longValue()));
                    }
                    return null;
                  })
              .put(
                  TableFieldSchema.Type.BIGNUMERIC,
                  (schemaInformation, value) -> {
                    if (value instanceof String) {
                      return BigDecimalByteStringEncoder.encodeToBigNumericByteString(
                          new BigDecimal((String) value));
                    } else if (value instanceof BigDecimal) {
                      return BigDecimalByteStringEncoder.encodeToBigNumericByteString(
                          ((BigDecimal) value));
                    } else if (value instanceof Double || value instanceof Float) {
                      return BigDecimalByteStringEncoder.encodeToBigNumericByteString(
                          BigDecimal.valueOf(((Number) value).doubleValue()));
                    } else if (value instanceof Short
                        || value instanceof Integer
                        || value instanceof Long) {
                      return BigDecimalByteStringEncoder.encodeToBigNumericByteString(
                          BigDecimal.valueOf(((Number) value).longValue()));
                    }
                    return null;
                  })
              .put(
                  TableFieldSchema.Type.DATETIME,
                  (schemaInformation, value) -> {
                    if (value instanceof String) {
                      try {
                        // '2011-12-03T10:15:30'
                        return CivilTimeEncoder.encodePacked64DatetimeMicros(
                            LocalDateTime.parse((String) value));
                      } catch (DateTimeParseException e2) {
                        // '2011-12-03 10:15:30'
                        return CivilTimeEncoder.encodePacked64DatetimeMicros(
                            LocalDateTime.parse((String) value, DATETIME_SPACE_FORMATTER));
                      }
                    } else if (value instanceof Number) {
                      return ((Number) value).longValue();
                    } else if (value instanceof LocalDateTime) {
                      return CivilTimeEncoder.encodePacked64DatetimeMicros((LocalDateTime) value);
                    } else if (value instanceof org.joda.time.LocalDateTime) {
                      return CivilTimeEncoder.encodePacked64DatetimeMicros(
                          (org.joda.time.LocalDateTime) value);
                    }
                    return null;
                  })
              .put(
                  TableFieldSchema.Type.TIME,
                  (schemaInformation, value) -> {
                    if (value instanceof String) {
                      return CivilTimeEncoder.encodePacked64TimeMicros(
                          LocalTime.parse((String) value));
                    } else if (value instanceof Number) {
                      return ((Number) value).longValue();
                    } else if (value instanceof LocalTime) {
                      return CivilTimeEncoder.encodePacked64TimeMicros((LocalTime) value);
                    } else if (value instanceof org.joda.time.LocalTime) {
                      return CivilTimeEncoder.encodePacked64TimeMicros(
                          (org.joda.time.LocalTime) value);
                    }
                    return null;
                  })
              .put(
                  TableFieldSchema.Type.STRING,
                  (schemaInformation, value) ->
                      Preconditions.checkArgumentNotNull(value).toString())
              .put(
                  TableFieldSchema.Type.JSON,
                  (schemaInformation, value) ->
                      Preconditions.checkArgumentNotNull(value).toString())
              .put(
                  TableFieldSchema.Type.GEOGRAPHY,
                  (schemaInformation, value) ->
                      Preconditions.checkArgumentNotNull(value).toString())
              .build();

  public static TableFieldSchema.Mode modeToProtoMode(
      @Nullable String defaultValueExpression, String mode) {
    TableFieldSchema.Mode resultMode =
        Optional.ofNullable(mode)
            .map(Mode::valueOf)
            .map(MODE_MAP_JSON_PROTO::get)
            .orElse(TableFieldSchema.Mode.NULLABLE);
    if (defaultValueExpression == null) {
      return resultMode;
    } else {
      // If there is a default value expression, treat this field as if it were nullable or
      // repeated.
      return resultMode.equals(TableFieldSchema.Mode.REPEATED)
          ? resultMode
          : TableFieldSchema.Mode.NULLABLE;
    }
  }

  public static String protoModeToJsonMode(TableFieldSchema.Mode protoMode) {
    String jsonMode = MODE_MAP_PROTO_JSON.get(protoMode);
    if (jsonMode == null) {
      throw new RuntimeException("Unknown mode " + protoMode);
    }
    return jsonMode;
  }

  public static String protoTypeToJsonType(TableFieldSchema.Type protoType) {
    String type = TYPE_MAP_PROTO_JSON.get(protoType);
    if (type == null) {
      throw new RuntimeException("Unknown type " + protoType);
    }
    return type;
  }

  public static TableFieldSchema.Type typeToProtoType(String type) {
    TableFieldSchema.Type protoType = TYPE_MAP_JSON_PROTO.get(type);
    if (protoType == null) {
      throw new RuntimeException("Unknown type " + type);
    }
    return protoType;
  }

  public static com.google.api.services.bigquery.model.TableSchema protoSchemaToTableSchema(
      TableSchema protoTableSchema) {
    com.google.api.services.bigquery.model.TableSchema tableSchema =
        new com.google.api.services.bigquery.model.TableSchema();
    List<com.google.api.services.bigquery.model.TableFieldSchema> tableFields =
        Lists.newArrayListWithExpectedSize(protoTableSchema.getFieldsCount());
    for (TableFieldSchema protoTableField : protoTableSchema.getFieldsList()) {
      tableFields.add(protoTableFieldToTableField(protoTableField));
    }
    return tableSchema.setFields(tableFields);
  }

  public static com.google.api.services.bigquery.model.TableFieldSchema protoTableFieldToTableField(
      TableFieldSchema protoTableField) {
    com.google.api.services.bigquery.model.TableFieldSchema tableField =
        new com.google.api.services.bigquery.model.TableFieldSchema();
    tableField = tableField.setName(protoTableField.getName());
    if (!Strings.isNullOrEmpty(tableField.getDescription())) {
      tableField = tableField.setDescription(protoTableField.getDescription());
    }
    if (protoTableField.getMaxLength() != 0) {
      tableField = tableField.setMaxLength(protoTableField.getMaxLength());
    }
    if (protoTableField.getMode() != TableFieldSchema.Mode.MODE_UNSPECIFIED) {
      tableField = tableField.setMode(protoModeToJsonMode(protoTableField.getMode()));
    }
    if (protoTableField.getPrecision() != 0) {
      tableField = tableField.setPrecision(protoTableField.getPrecision());
    }
    if (protoTableField.getScale() != 0) {
      tableField = tableField.setScale(protoTableField.getScale());
    }
    tableField = tableField.setType(protoTypeToJsonType(protoTableField.getType()));
    if (protoTableField.getType().equals(TableFieldSchema.Type.STRUCT)) {
      List<com.google.api.services.bigquery.model.TableFieldSchema> subFields =
          Lists.newArrayListWithExpectedSize(protoTableField.getFieldsCount());
      for (TableFieldSchema subField : protoTableField.getFieldsList()) {
        subFields.add(protoTableFieldToTableField(subField));
      }
      tableField = tableField.setFields(subFields);
    }
    return tableField;
  }

  public static TableSchema schemaToProtoTableSchema(
      com.google.api.services.bigquery.model.TableSchema tableSchema) {
    TableSchema.Builder builder = TableSchema.newBuilder();
    if (tableSchema.getFields() != null) {
      for (com.google.api.services.bigquery.model.TableFieldSchema field :
          tableSchema.getFields()) {
        builder.addFields(tableFieldToProtoTableField(field));
      }
    }
    return builder.build();
  }

  public static TableFieldSchema tableFieldToProtoTableField(
      com.google.api.services.bigquery.model.TableFieldSchema field) {
    TableFieldSchema.Builder builder = TableFieldSchema.newBuilder();
    builder.setName(field.getName().toLowerCase());
    if (field.getDescription() != null) {
      builder.setDescription(field.getDescription());
    }
    if (field.getMaxLength() != null) {
      builder.setMaxLength(field.getMaxLength());
    }
    builder.setMode(modeToProtoMode(field.getDefaultValueExpression(), field.getMode()));
    if (field.getPrecision() != null) {
      builder.setPrecision(field.getPrecision());
    }
    if (field.getScale() != null) {
      builder.setScale(field.getScale());
    }
    builder.setType(typeToProtoType(field.getType()));
    if (builder.getType().equals(TableFieldSchema.Type.STRUCT)) {
      for (com.google.api.services.bigquery.model.TableFieldSchema subField : field.getFields()) {
        builder.addFields(tableFieldToProtoTableField(subField));
      }
    }
    return builder.build();
  }

  public static class SchemaInformation {
    private final TableFieldSchema tableFieldSchema;
    private final List<SchemaInformation> subFields;
    private final Map<String, SchemaInformation> subFieldsByName;
    private final Iterable<SchemaInformation> parentSchemas;

    private SchemaInformation(
        TableFieldSchema tableFieldSchema, Iterable<SchemaInformation> parentSchemas) {
      this.tableFieldSchema = tableFieldSchema;
      this.subFields = Lists.newArrayList();
      this.subFieldsByName = Maps.newHashMap();
      this.parentSchemas = parentSchemas;
      for (TableFieldSchema field : tableFieldSchema.getFieldsList()) {
        SchemaInformation schemaInformation =
            new SchemaInformation(
                field, Iterables.concat(this.parentSchemas, ImmutableList.of(this)));
        subFields.add(schemaInformation);
        subFieldsByName.put(field.getName().toLowerCase(), schemaInformation);
      }
    }

    public String getFullName() {
      String prefix =
          StreamSupport.stream(parentSchemas.spliterator(), false)
              .map(SchemaInformation::getName)
              .collect(Collectors.joining("."));
      return prefix.isEmpty() ? getName() : prefix + "." + getName();
    }

    public String getName() {
      return tableFieldSchema.getName();
    }

    public TableFieldSchema.Type getType() {
      return tableFieldSchema.getType();
    }

    public boolean isNullable() {
      return tableFieldSchema.getMode().equals(TableFieldSchema.Mode.NULLABLE);
    }

    public boolean isRepeated() {
      return tableFieldSchema.getMode().equals(TableFieldSchema.Mode.REPEATED);
    }

    public SchemaInformation getSchemaForField(String name) {
      SchemaInformation schemaInformation = subFieldsByName.get(name.toLowerCase());
      if (schemaInformation == null) {
        throw new RuntimeException("Schema field not found: " + name.toLowerCase());
      }
      return schemaInformation;
    }

    public SchemaInformation getSchemaForField(int i) {
      SchemaInformation schemaInformation = subFields.get(i);
      if (schemaInformation == null) {
        throw new RuntimeException("Schema field not found: " + i);
      }
      return schemaInformation;
    }

    public static SchemaInformation fromTableSchema(TableSchema tableSchema) {
      TableFieldSchema root =
          TableFieldSchema.newBuilder()
              .addAllFields(tableSchema.getFieldsList())
              .setName("root")
              .build();
      return new SchemaInformation(root, Collections.emptyList());
    }

    static SchemaInformation fromTableSchema(
        com.google.api.services.bigquery.model.TableSchema jsonTableSchema) {
      return SchemaInformation.fromTableSchema(schemaToProtoTableSchema(jsonTableSchema));
    }
  }

  static final Map<TableFieldSchema.Type, Type> PRIMITIVE_TYPES_BQ_TO_PROTO =
      ImmutableMap.<TableFieldSchema.Type, Type>builder()
          .put(TableFieldSchema.Type.INT64, Type.TYPE_INT64)
          .put(TableFieldSchema.Type.DOUBLE, Type.TYPE_DOUBLE)
          .put(TableFieldSchema.Type.STRING, Type.TYPE_STRING)
          .put(TableFieldSchema.Type.BOOL, Type.TYPE_BOOL)
          .put(TableFieldSchema.Type.BYTES, Type.TYPE_BYTES)
          .put(TableFieldSchema.Type.NUMERIC, Type.TYPE_BYTES)
          .put(TableFieldSchema.Type.BIGNUMERIC, Type.TYPE_BYTES)
          .put(TableFieldSchema.Type.GEOGRAPHY, Type.TYPE_STRING) // Pass through the JSON encoding.
          .put(TableFieldSchema.Type.DATE, Type.TYPE_INT32)
          .put(TableFieldSchema.Type.TIME, Type.TYPE_INT64)
          .put(TableFieldSchema.Type.DATETIME, Type.TYPE_INT64)
          .put(TableFieldSchema.Type.TIMESTAMP, Type.TYPE_INT64)
          .put(TableFieldSchema.Type.JSON, Type.TYPE_STRING)
          .build();

  static final Map<Descriptors.FieldDescriptor.Type, TableFieldSchema.Type>
      PRIMITIVE_TYPES_PROTO_TO_BQ =
          ImmutableMap.<Descriptors.FieldDescriptor.Type, TableFieldSchema.Type>builder()
              .put(Descriptors.FieldDescriptor.Type.INT32, TableFieldSchema.Type.INT64)
              .put(FieldDescriptor.Type.FIXED32, TableFieldSchema.Type.INT64)
              .put(FieldDescriptor.Type.UINT32, TableFieldSchema.Type.INT64)
              .put(FieldDescriptor.Type.SFIXED32, TableFieldSchema.Type.INT64)
              .put(FieldDescriptor.Type.SINT32, TableFieldSchema.Type.INT64)
              .put(FieldDescriptor.Type.INT64, TableFieldSchema.Type.INT64)
              .put(FieldDescriptor.Type.FIXED64, TableFieldSchema.Type.NUMERIC)
              .put(FieldDescriptor.Type.UINT64, TableFieldSchema.Type.NUMERIC)
              .put(FieldDescriptor.Type.SFIXED64, TableFieldSchema.Type.INT64)
              .put(FieldDescriptor.Type.SINT64, TableFieldSchema.Type.INT64)
              .put(FieldDescriptor.Type.DOUBLE, TableFieldSchema.Type.DOUBLE)
              .put(FieldDescriptor.Type.FLOAT, TableFieldSchema.Type.DOUBLE)
              .put(FieldDescriptor.Type.STRING, TableFieldSchema.Type.STRING)
              .put(FieldDescriptor.Type.BOOL, TableFieldSchema.Type.BOOL)
              .put(FieldDescriptor.Type.BYTES, TableFieldSchema.Type.BYTES)
              .build();

  public static Descriptor getDescriptorFromTableSchema(
      com.google.api.services.bigquery.model.TableSchema jsonSchema,
      boolean respectRequired,
      boolean includeCdcColumns)
      throws DescriptorValidationException {
    return getDescriptorFromTableSchema(
        schemaToProtoTableSchema(jsonSchema), respectRequired, includeCdcColumns);
  }

  /**
   * Given a BigQuery TableSchema, returns a protocol-buffer Descriptor that can be used to write
   * data using the BigQuery Storage API.
   */
  public static Descriptor getDescriptorFromTableSchema(
      TableSchema tableSchema, boolean respectRequired, boolean includeCdcColumns)
      throws DescriptorValidationException {
    return wrapDescriptorProto(
        descriptorSchemaFromTableSchema(tableSchema, respectRequired, includeCdcColumns));
  }

  public static Descriptor wrapDescriptorProto(DescriptorProto descriptorProto)
      throws DescriptorValidationException {
    FileDescriptorProto fileDescriptorProto =
        FileDescriptorProto.newBuilder().addMessageType(descriptorProto).build();
    FileDescriptor fileDescriptor =
        FileDescriptor.buildFrom(fileDescriptorProto, new FileDescriptor[0]);

    return Iterables.getOnlyElement(fileDescriptor.getMessageTypes());
  }

  public static DynamicMessage messageFromMap(
      SchemaInformation schemaInformation,
      Descriptor descriptor,
      AbstractMap<String, Object> map,
      boolean ignoreUnknownValues,
      boolean allowMissingRequiredFields,
      @Nullable TableRow unknownFields,
      @Nullable String changeType,
      @Nullable String changeSequenceNum)
      throws SchemaConversionException {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      String key = entry.getKey().toLowerCase();
      String protoFieldName =
          BigQuerySchemaUtil.isProtoCompatible(key)
              ? key
              : BigQuerySchemaUtil.generatePlaceholderFieldName(key);
      @Nullable FieldDescriptor fieldDescriptor = descriptor.findFieldByName(protoFieldName);
      if (fieldDescriptor == null) {
        if (unknownFields != null) {
          unknownFields.set(key, entry.getValue());
        }
        if (ignoreUnknownValues) {
          continue;
        } else {
          throw new SchemaTooNarrowException(
              "TableRow contained unexpected field with name "
                  + entry.getKey()
                  + " not found in schema for "
                  + schemaInformation.getFullName());
        }
      }

      SchemaInformation fieldSchemaInformation =
          schemaInformation.getSchemaForField(entry.getKey());
      try {
        Supplier<@Nullable TableRow> getNestedUnknown =
            () -> {
              if (unknownFields == null) {
                return null;
              }
              TableRow nestedUnknown = new TableRow();
              if (fieldDescriptor.isRepeated()) {
                ((List<TableRow>)
                        (unknownFields.computeIfAbsent(key, k -> new ArrayList<TableRow>())))
                    .add(nestedUnknown);
                return nestedUnknown;
              }
              return (TableRow) unknownFields.computeIfAbsent(key, k -> nestedUnknown);
            };

        @Nullable
        Object value =
            messageValueFromFieldValue(
                fieldSchemaInformation,
                fieldDescriptor,
                entry.getValue(),
                ignoreUnknownValues,
                allowMissingRequiredFields,
                getNestedUnknown);
        if (value != null) {
          builder.setField(fieldDescriptor, value);
        }
        // For STRUCT fields, we add a placeholder to unknownFields using the getNestedUnknown
        // supplier (in case we encounter unknown nested fields). If the placeholder comes out
        // to be empty, we should clean it up
        if ((fieldSchemaInformation.getType().equals(TableFieldSchema.Type.STRUCT)
                && unknownFields != null)
            && ((unknownFields.get(key) instanceof Map
                    && ((Map<?, ?>) unknownFields.get(key)).isEmpty()) // single struct, empty
                || (unknownFields.get(key)
                        instanceof List // repeated struct, empty list or list with empty structs
                    && (((List<?>) unknownFields.get(key)).isEmpty()
                        || ((List<?>) unknownFields.get(key))
                            .stream()
                                .allMatch(row -> row == null || ((Map<?, ?>) row).isEmpty()))))) {
          unknownFields.remove(key);
        }
      } catch (Exception e) {
        throw new SchemaDoesntMatchException(
            "Problem converting field "
                + fieldSchemaInformation.getFullName()
                + " expected type: "
                + fieldSchemaInformation.getType(),
            e);
      }
    }
    if (changeType != null) {
      builder.setField(
          Preconditions.checkStateNotNull(
              descriptor.findFieldByName(StorageApiCDC.CHANGE_TYPE_COLUMN)),
          changeType);
      builder.setField(
          Preconditions.checkStateNotNull(
              descriptor.findFieldByName(StorageApiCDC.CHANGE_SQN_COLUMN)),
          Preconditions.checkStateNotNull(changeSequenceNum));
    }

    try {
      return builder.build();
    } catch (Exception e) {
      throw new SchemaDoesntMatchException(
          "Couldn't convert schema for " + schemaInformation.getFullName(), e);
    }
  }

  /**
   * Forwards {@param changeSequenceNum} to {@link #messageFromTableRow(SchemaInformation,
   * Descriptor, TableRow, boolean, boolean, TableRow, String, String)} via {@link
   * Long#toHexString}.
   */
  public static DynamicMessage messageFromTableRow(
      SchemaInformation schemaInformation,
      Descriptor descriptor,
      TableRow tableRow,
      boolean ignoreUnknownValues,
      boolean allowMissingRequiredFields,
      final @Nullable TableRow unknownFields,
      @Nullable String changeType,
      long changeSequenceNum)
      throws SchemaConversionException {
    return messageFromTableRow(
        schemaInformation,
        descriptor,
        tableRow,
        ignoreUnknownValues,
        allowMissingRequiredFields,
        unknownFields,
        changeType,
        Long.toHexString(changeSequenceNum));
  }

  /**
   * Given a BigQuery TableRow, returns a protocol-buffer message that can be used to write data
   * using the BigQuery Storage API.
   */
  @SuppressWarnings("nullness")
  public static DynamicMessage messageFromTableRow(
      SchemaInformation schemaInformation,
      Descriptor descriptor,
      TableRow tableRow,
      boolean ignoreUnknownValues,
      boolean allowMissingRequiredFields,
      final @Nullable TableRow unknownFields,
      @Nullable String changeType,
      @Nullable String changeSequenceNum)
      throws SchemaConversionException {
    @Nullable Object fValue = tableRow.get("f");
    if (fValue instanceof List) {
      List<AbstractMap<String, Object>> cells = (List<AbstractMap<String, Object>>) fValue;
      DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
      int cellsToProcess = cells.size();
      if (cells.size() > descriptor.getFields().size()) {
        if (ignoreUnknownValues) {
          cellsToProcess = descriptor.getFields().size();
        } else {
          throw new SchemaTooNarrowException(
              "TableRow contained too many fields and ignoreUnknownValues not set in "
                  + schemaInformation.getName());
        }
      }

      if (unknownFields != null) {
        List<TableCell> unknownValues = Lists.newArrayListWithExpectedSize(cells.size());
        for (int i = 0; i < cells.size(); ++i) {
          unknownValues.add(new TableCell().setV(null));
        }
        unknownFields.setF(unknownValues);
      }

      for (int i = 0; i < cellsToProcess; ++i) {
        AbstractMap<String, Object> cell = cells.get(i);
        FieldDescriptor fieldDescriptor = descriptor.getFields().get(i);
        SchemaInformation fieldSchemaInformation = schemaInformation.getSchemaForField(i);
        try {
          final int finalIndex = i;
          Supplier<@Nullable TableRow> getNestedUnknown =
              () -> {
                if (unknownFields == null) {
                  return null;
                }
                TableRow localUnknownFields = Preconditions.checkStateNotNull(unknownFields);
                @Nullable
                TableRow nested = (TableRow) (localUnknownFields.getF().get(finalIndex).getV());
                if (nested == null) {
                  nested = new TableRow();
                  localUnknownFields.getF().set(finalIndex, new TableCell().setV(nested));
                }
                return nested;
              };

          @Nullable
          Object value =
              messageValueFromFieldValue(
                  fieldSchemaInformation,
                  fieldDescriptor,
                  cell.get("v"),
                  ignoreUnknownValues,
                  allowMissingRequiredFields,
                  getNestedUnknown);
          if (value != null) {
            builder.setField(fieldDescriptor, value);
          }
        } catch (Exception e) {
          throw new SchemaDoesntMatchException(
              "Problem converting field "
                  + fieldSchemaInformation.getFullName()
                  + " expected type: "
                  + fieldSchemaInformation.getType(),
              e);
        }
      }
      if (changeType != null) {
        builder.setField(
            Preconditions.checkStateNotNull(
                descriptor.findFieldByName(StorageApiCDC.CHANGE_TYPE_COLUMN)),
            changeType);
        builder.setField(
            Preconditions.checkStateNotNull(
                descriptor.findFieldByName(StorageApiCDC.CHANGE_SQN_COLUMN)),
            Preconditions.checkStateNotNull(changeSequenceNum));
      }

      // If there are unknown fields, copy them into the output.
      if (unknownFields != null) {
        for (int i = cellsToProcess; i < cells.size(); ++i) {
          unknownFields.getF().set(i, new TableCell().setV(cells.get(i).get("v")));
        }
      }

      try {
        return builder.build();
      } catch (Exception e) {
        throw new SchemaDoesntMatchException(
            "Could convert schema for " + schemaInformation.getFullName(), e);
      }
    } else {
      return messageFromMap(
          schemaInformation,
          descriptor,
          tableRow,
          ignoreUnknownValues,
          allowMissingRequiredFields,
          unknownFields,
          changeType,
          changeSequenceNum);
    }
  }

  static TableSchema tableSchemaFromDescriptor(Descriptor descriptor) {
    List<TableFieldSchema> tableFields =
        descriptor.getFields().stream()
            .map(f -> tableFieldSchemaFromDescriptorField(f))
            .collect(toList());
    return TableSchema.newBuilder().addAllFields(tableFields).build();
  }

  private static String fieldNameFromProtoFieldDescriptor(FieldDescriptor fieldDescriptor) {
    if (fieldDescriptor.getOptions().hasExtension(AnnotationsProto.columnName)) {
      return fieldDescriptor.getOptions().getExtension(AnnotationsProto.columnName);
    } else {
      return fieldDescriptor.getName();
    }
  }

  static TableFieldSchema tableFieldSchemaFromDescriptorField(FieldDescriptor fieldDescriptor) {
    TableFieldSchema.Builder tableFieldSchemaBuilder = TableFieldSchema.newBuilder();
    tableFieldSchemaBuilder =
        tableFieldSchemaBuilder.setName(fieldNameFromProtoFieldDescriptor(fieldDescriptor));

    switch (fieldDescriptor.getType()) {
      case MESSAGE:
        tableFieldSchemaBuilder = tableFieldSchemaBuilder.setType(TableFieldSchema.Type.STRUCT);
        TableSchema nestedTableField = tableSchemaFromDescriptor(fieldDescriptor.getMessageType());
        tableFieldSchemaBuilder =
            tableFieldSchemaBuilder.addAllFields(nestedTableField.getFieldsList());
        break;
      default:
        TableFieldSchema.Type type = PRIMITIVE_TYPES_PROTO_TO_BQ.get(fieldDescriptor.getType());
        if (type == null) {
          throw new UnsupportedOperationException(
              "proto type " + fieldDescriptor.getType() + " is unsupported.");
        }
        tableFieldSchemaBuilder = tableFieldSchemaBuilder.setType(type);
    }

    if (fieldDescriptor.isRepeated()) {
      tableFieldSchemaBuilder = tableFieldSchemaBuilder.setMode(TableFieldSchema.Mode.REPEATED);
    } else if (fieldDescriptor.isRequired()) {
      tableFieldSchemaBuilder = tableFieldSchemaBuilder.setMode(TableFieldSchema.Mode.REQUIRED);
    } else {
      tableFieldSchemaBuilder = tableFieldSchemaBuilder.setMode(TableFieldSchema.Mode.NULLABLE);
    }
    return tableFieldSchemaBuilder.build();
  }

  @VisibleForTesting
  static DescriptorProto descriptorSchemaFromTableSchema(
      com.google.api.services.bigquery.model.TableSchema tableSchema,
      boolean respectRequired,
      boolean includeCdcColumns) {
    return descriptorSchemaFromTableSchema(
        schemaToProtoTableSchema(tableSchema), respectRequired, includeCdcColumns);
  }

  @VisibleForTesting
  static DescriptorProto descriptorSchemaFromTableSchema(
      TableSchema tableSchema, boolean respectRequired, boolean includeCdcColumns) {
    return descriptorSchemaFromTableFieldSchemas(
        tableSchema.getFieldsList(), respectRequired, includeCdcColumns);
  }

  private static DescriptorProto descriptorSchemaFromTableFieldSchemas(
      Iterable<TableFieldSchema> tableFieldSchemas,
      boolean respectRequired,
      boolean includeCdcColumns) {
    DescriptorProto.Builder descriptorBuilder = DescriptorProto.newBuilder();
    // Create a unique name for the descriptor ('-' characters cannot be used).
    descriptorBuilder.setName("D" + UUID.randomUUID().toString().replace("-", "_"));
    int i = 1;
    for (TableFieldSchema fieldSchema : tableFieldSchemas) {
      fieldDescriptorFromTableField(fieldSchema, i++, descriptorBuilder, respectRequired);
    }
    if (includeCdcColumns) {
      FieldDescriptorProto.Builder fieldDescriptorBuilder = FieldDescriptorProto.newBuilder();
      fieldDescriptorBuilder = fieldDescriptorBuilder.setName(StorageApiCDC.CHANGE_TYPE_COLUMN);
      fieldDescriptorBuilder = fieldDescriptorBuilder.setNumber(i++);
      fieldDescriptorBuilder = fieldDescriptorBuilder.setType(Type.TYPE_STRING);
      fieldDescriptorBuilder = fieldDescriptorBuilder.setLabel(Label.LABEL_OPTIONAL);
      descriptorBuilder.addField(fieldDescriptorBuilder.build());

      fieldDescriptorBuilder = FieldDescriptorProto.newBuilder();
      fieldDescriptorBuilder = fieldDescriptorBuilder.setName(StorageApiCDC.CHANGE_SQN_COLUMN);
      fieldDescriptorBuilder = fieldDescriptorBuilder.setNumber(i++);
      fieldDescriptorBuilder = fieldDescriptorBuilder.setType(Type.TYPE_STRING);
      fieldDescriptorBuilder = fieldDescriptorBuilder.setLabel(Label.LABEL_OPTIONAL);
      descriptorBuilder.addField(fieldDescriptorBuilder.build());
    }
    return descriptorBuilder.build();
  }

  private static void fieldDescriptorFromTableField(
      TableFieldSchema fieldSchema,
      int fieldNumber,
      DescriptorProto.Builder descriptorBuilder,
      boolean respectRequired) {
    if (StorageApiCDC.COLUMNS.contains(fieldSchema.getName())) {
      throw new RuntimeException(
          "Reserved field name " + fieldSchema.getName() + " in user schema.");
    }
    FieldDescriptorProto.Builder fieldDescriptorBuilder = FieldDescriptorProto.newBuilder();
    final String fieldName = fieldSchema.getName().toLowerCase();
    fieldDescriptorBuilder = fieldDescriptorBuilder.setName(fieldName);
    fieldDescriptorBuilder = fieldDescriptorBuilder.setNumber(fieldNumber);
    if (!BigQuerySchemaUtil.isProtoCompatible(fieldName)) {
      fieldDescriptorBuilder =
          fieldDescriptorBuilder.setName(
              BigQuerySchemaUtil.generatePlaceholderFieldName(fieldName));

      Message.Builder fieldOptionBuilder = DescriptorProtos.FieldOptions.newBuilder();
      fieldOptionBuilder =
          fieldOptionBuilder.setField(AnnotationsProto.columnName.getDescriptor(), fieldName);
      fieldDescriptorBuilder =
          fieldDescriptorBuilder.setOptions(
              (DescriptorProtos.FieldOptions) fieldOptionBuilder.build());
    }
    switch (fieldSchema.getType()) {
      case STRUCT:
        DescriptorProto nested =
            descriptorSchemaFromTableFieldSchemas(
                fieldSchema.getFieldsList(), respectRequired, false);
        descriptorBuilder.addNestedType(nested);
        fieldDescriptorBuilder =
            fieldDescriptorBuilder.setType(Type.TYPE_MESSAGE).setTypeName(nested.getName());
        break;
      default:
        @Nullable Type type = PRIMITIVE_TYPES_BQ_TO_PROTO.get(fieldSchema.getType());
        if (type == null) {
          throw new UnsupportedOperationException(
              "Converting BigQuery type " + fieldSchema.getType() + " to Beam type is unsupported");
        }
        fieldDescriptorBuilder = fieldDescriptorBuilder.setType(type);
    }

    if (fieldSchema.getMode() == TableFieldSchema.Mode.REPEATED) {
      fieldDescriptorBuilder = fieldDescriptorBuilder.setLabel(Label.LABEL_REPEATED);
    } else if (!respectRequired || fieldSchema.getMode() != TableFieldSchema.Mode.REQUIRED) {
      fieldDescriptorBuilder = fieldDescriptorBuilder.setLabel(Label.LABEL_OPTIONAL);
    } else {
      fieldDescriptorBuilder = fieldDescriptorBuilder.setLabel(Label.LABEL_REQUIRED);
    }
    descriptorBuilder.addField(fieldDescriptorBuilder.build());
  }

  /**
   * mergeNewFields(original, newFields) unlike proto merge or concatenating proto bytes is merging
   * the main differences is skipping primitive fields that are already set and merging structs and
   * lists recursively. Method mutates input.
   *
   * @param original original table row
   * @param newRow
   * @return merged table row
   */
  private static TableRow mergeNewFields(TableRow original, TableRow newRow) {
    if (original == null) {
      return newRow;
    }
    if (newRow == null) {
      return original;
    }

    for (Map.Entry<String, Object> entry : newRow.entrySet()) {
      String key = entry.getKey();
      Object value2 = entry.getValue();
      Object value1 = original.get(key);

      if (value1 == null) {
        original.set(key, value2);
      } else {
        if (value1 instanceof List && value2 instanceof List) {
          List<?> list1 = (List<?>) value1;
          List<?> list2 = (List<?>) value2;
          if (!list1.isEmpty()
              && list1.get(0) instanceof TableRow
              && !list2.isEmpty()
              && list2.get(0) instanceof TableRow) {
            original.set(key, mergeRepeatedStructs((List<TableRow>) list1, (List<TableRow>) list2));
          } else {
            // primitive lists
            original.set(key, value2);
          }
        } else if (value1 instanceof TableRow && value2 instanceof TableRow) {
          original.set(key, mergeNewFields((TableRow) value1, (TableRow) value2));
        }
      }
    }

    return original;
  }

  private static List<TableRow> mergeRepeatedStructs(List<TableRow> list1, List<TableRow> list2) {
    List<TableRow> mergedList = new ArrayList<>();
    int length = Math.min(list1.size(), list2.size());

    for (int i = 0; i < length; i++) {
      TableRow orig = (i < list1.size()) ? list1.get(i) : null;
      TableRow delta = (i < list2.size()) ? list2.get(i) : null;
      // fail if any is shorter
      Preconditions.checkArgumentNotNull(orig);
      Preconditions.checkArgumentNotNull(delta);

      mergedList.add(mergeNewFields(orig, delta));
    }
    return mergedList;
  }

  public static ByteString mergeNewFields(
      ByteString tableRowProto,
      DescriptorProtos.DescriptorProto descriptorProto,
      TableSchema tableSchema,
      SchemaInformation schemaInformation,
      TableRow unknownFields,
      boolean ignoreUnknownValues)
      throws TableRowToStorageApiProto.SchemaConversionException {
    if (unknownFields == null || unknownFields.isEmpty()) {
      // nothing to do here
      return tableRowProto;
    }
    // check if unknownFields contains repeated struct, merge
    boolean hasRepeatedStruct =
        unknownFields.entrySet().stream()
            .anyMatch(
                entry ->
                    entry.getValue() instanceof List
                        && !((List<?>) entry.getValue()).isEmpty()
                        && ((List<?>) entry.getValue()).get(0) instanceof TableRow);
    if (!hasRepeatedStruct) {
      Descriptor descriptorIgnoreRequired = null;
      try {
        descriptorIgnoreRequired =
            TableRowToStorageApiProto.getDescriptorFromTableSchema(tableSchema, false, false);
      } catch (DescriptorValidationException e) {
        throw new RuntimeException(e);
      }
      ByteString unknownFieldsProto =
          messageFromTableRow(
                  schemaInformation,
                  descriptorIgnoreRequired,
                  unknownFields,
                  ignoreUnknownValues,
                  true,
                  null,
                  null,
                  null)
              .toByteString();
      return tableRowProto.concat(unknownFieldsProto);
    }

    DynamicMessage message = null;
    Descriptor descriptor = null;
    try {
      descriptor = wrapDescriptorProto(descriptorProto);
    } catch (DescriptorValidationException e) {
      throw new RuntimeException(e);
    }
    try {
      message = DynamicMessage.parseFrom(descriptor, tableRowProto);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    TableRow original =
        TableRowToStorageApiProto.tableRowFromMessage(
            schemaInformation, message, true, Predicates.alwaysTrue());
    Map<String, Descriptors.FieldDescriptor> fieldDescriptors =
        descriptor.getFields().stream()
            .collect(Collectors.toMap(Descriptors.FieldDescriptor::getName, Functions.identity()));
    // recover cdc data
    String cdcType = null;
    String sequence = null;
    if (fieldDescriptors.get(StorageApiCDC.CHANGE_TYPE_COLUMN) != null
        && fieldDescriptors.get(StorageApiCDC.CHANGE_SQN_COLUMN) != null) {
      cdcType =
          (String)
              message.getField(
                  Preconditions.checkStateNotNull(
                      fieldDescriptors.get(StorageApiCDC.CHANGE_TYPE_COLUMN)));
      sequence =
          (String)
              message.getField(
                  Preconditions.checkStateNotNull(
                      fieldDescriptors.get(StorageApiCDC.CHANGE_SQN_COLUMN)));
    }
    TableRow merged = TableRowToStorageApiProto.mergeNewFields(original, unknownFields);
    DynamicMessage dynamicMessage =
        TableRowToStorageApiProto.messageFromTableRow(
            schemaInformation,
            descriptor,
            merged,
            ignoreUnknownValues,
            false,
            null,
            cdcType,
            sequence);
    return dynamicMessage.toByteString();
  }

  private static @Nullable Object messageValueFromFieldValue(
      SchemaInformation schemaInformation,
      FieldDescriptor fieldDescriptor,
      @Nullable Object bqValue,
      boolean ignoreUnknownValues,
      boolean allowMissingRequiredFields,
      Supplier<@Nullable TableRow> getUnknownNestedFields)
      throws SchemaConversionException {
    if (bqValue == null) {
      if (fieldDescriptor.isOptional() || allowMissingRequiredFields) {
        return null;
      } else if (fieldDescriptor.isRepeated()) {
        return Collections.emptyList();
      } else {
        // TODO: Allow expanding this!
        throw new SchemaDoesntMatchException(
            "Received null value for non-nullable field " + schemaInformation.getFullName());
      }
    }
    if (fieldDescriptor.isRepeated()) {
      List<Object> listValue = (List<Object>) bqValue;
      List<@Nullable Object> protoList = Lists.newArrayListWithCapacity(listValue.size());
      for (@Nullable Object o : listValue) {
        if (o != null) { // repeated field cannot contain null.
          protoList.add(
              singularFieldToProtoValue(
                  schemaInformation,
                  fieldDescriptor,
                  o,
                  ignoreUnknownValues,
                  allowMissingRequiredFields,
                  getUnknownNestedFields));
        }
      }
      return protoList;
    }
    return singularFieldToProtoValue(
        schemaInformation,
        fieldDescriptor,
        Preconditions.checkStateNotNull(bqValue),
        ignoreUnknownValues,
        allowMissingRequiredFields,
        getUnknownNestedFields);
  }

  @VisibleForTesting
  static @Nullable Object singularFieldToProtoValue(
      SchemaInformation schemaInformation,
      FieldDescriptor fieldDescriptor,
      Object value,
      boolean ignoreUnknownValues,
      boolean allowMissingRequiredFields,
      Supplier<@Nullable TableRow> getUnknownNestedFields)
      throws SchemaConversionException {
    @Nullable Object converted = null;
    if (schemaInformation.getType() == TableFieldSchema.Type.STRUCT) {
      if (value instanceof TableRow) {
        TableRow tableRow = (TableRow) value;
        converted =
            messageFromTableRow(
                schemaInformation,
                fieldDescriptor.getMessageType(),
                tableRow,
                ignoreUnknownValues,
                allowMissingRequiredFields,
                getUnknownNestedFields.get(),
                null,
                null);
      } else if (value instanceof AbstractMap) {
        // This will handle nested rows.
        AbstractMap<String, Object> map = ((AbstractMap<String, Object>) value);
        converted =
            messageFromMap(
                schemaInformation,
                fieldDescriptor.getMessageType(),
                map,
                ignoreUnknownValues,
                allowMissingRequiredFields,
                getUnknownNestedFields.get(),
                null,
                null);
      }
    } else {
      @Nullable
      ThrowingBiFunction<String, Object, @Nullable Object> converter =
          TYPE_MAP_PROTO_CONVERTERS.get(schemaInformation.getType());
      if (converter == null) {
        throw new RuntimeException("Unknown type " + schemaInformation.getType());
      }
      converted = converter.apply(schemaInformation.getFullName(), value);
    }
    if (converted == null) {
      throw new SchemaDoesntMatchException(
          "Unexpected value: "
              + value
              + ", type: "
              + (value == null ? "null" : value.getClass())
              + ". Table field name: "
              + schemaInformation.getFullName()
              + ", type: "
              + schemaInformation.getType());
    }
    return converted;
  }

  private static long toEpochMicros(Instant timestamp) {
    // i.e 1970-01-01T00:01:01.000040Z: 61 * 1000_000L + 40000/1000 = 61000040
    return timestamp.getEpochSecond() * 1000_000L + timestamp.getNano() / 1000;
  }

  @VisibleForTesting
  public static TableRow tableRowFromMessage(
      SchemaInformation schemaInformation,
      Message message,
      boolean includeCdcColumns,
      Predicate<String> includeField) {
    return tableRowFromMessage(schemaInformation, message, includeCdcColumns, includeField, "");
  }

  public static TableRow tableRowFromMessage(
      SchemaInformation schemaInformation,
      Message message,
      boolean includeCdcColumns,
      Predicate<String> includeField,
      String namePrefix) {
    // We first try to create a map-style TableRow for backwards compatibility with existing usage.
    // However this will
    // fail if there is a column name "f". If it fails, we then instead create a list-style
    // TableRow.
    Optional<TableRow> tableRow =
        tableRowFromMessageNoF(
            schemaInformation, message, includeCdcColumns, includeField, namePrefix);
    return tableRow.orElseGet(
        () ->
            tableRowFromMessageUseSetF(
                schemaInformation, message, includeCdcColumns, includeField, ""));
  }

  private static Optional<TableRow> tableRowFromMessageNoF(
      SchemaInformation schemaInformation,
      Message message,
      boolean includeCdcColumns,
      Predicate<String> includeField,
      String namePrefix) {
    TableRow tableRow = new TableRow();
    for (Map.Entry<FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
      StringBuilder fullName = new StringBuilder();
      FieldDescriptor fieldDescriptor = field.getKey();
      String fieldName = fieldNameFromProtoFieldDescriptor(fieldDescriptor);
      if ("f".equals(fieldName)) {
        // TableRow.put won't work as expected if the fields in named "f." Fail the call, and force
        // a retry using
        // the setF codepath.
        return Optional.empty();
      }
      fullName = fullName.append(namePrefix).append(fieldName);
      Object fieldValue = field.getValue();
      if ((includeCdcColumns || !StorageApiCDC.COLUMNS.contains(fullName.toString()))
          && includeField.test(fieldName)) {
        SchemaInformation fieldSchemaInformation = schemaInformation.getSchemaForField(fieldName);
        Object convertedFieldValue =
            jsonValueFromMessageValue(
                fieldSchemaInformation,
                fieldDescriptor,
                fieldValue,
                true,
                includeField,
                fullName.append(".").toString(),
                false);
        if (convertedFieldValue instanceof Optional) {
          Optional<?> optional = (Optional<?>) convertedFieldValue;
          if (!optional.isPresent()) {
            // Some nested message had a field named "f." Fail.
            return Optional.empty();
          } else {
            convertedFieldValue = optional.get();
          }
        }
        tableRow.put(fieldName, convertedFieldValue);
      }
    }
    return Optional.of(tableRow);
  }

  public static TableRow tableRowFromMessageUseSetF(
      SchemaInformation schemaInformation,
      Message message,
      boolean includeCdcColumns,
      Predicate<String> includeField,
      String namePrefix) {
    List<TableCell> tableCells =
        Lists.newArrayListWithCapacity(message.getDescriptorForType().getFields().size());

    for (FieldDescriptor fieldDescriptor : message.getDescriptorForType().getFields()) {
      TableCell tableCell = new TableCell();
      boolean isPresent =
          (fieldDescriptor.isRepeated() && message.getRepeatedFieldCount(fieldDescriptor) > 0)
              || (!fieldDescriptor.isRepeated() && message.hasField(fieldDescriptor));
      if (isPresent) {
        StringBuilder fullName = new StringBuilder();
        String fieldName = fieldNameFromProtoFieldDescriptor(fieldDescriptor);
        fullName = fullName.append(namePrefix).append(fieldName);
        if ((includeCdcColumns || !StorageApiCDC.COLUMNS.contains(fullName.toString()))
            && includeField.test(fieldName)) {
          SchemaInformation fieldSchemaInformation = schemaInformation.getSchemaForField(fieldName);
          Object fieldValue = message.getField(fieldDescriptor);
          Object converted =
              jsonValueFromMessageValue(
                  fieldSchemaInformation,
                  fieldDescriptor,
                  fieldValue,
                  true,
                  includeField,
                  fullName.append(".").toString(),
                  true);
          tableCell.setV(converted);
        }
      }
      tableCells.add(tableCell);
    }

    TableRow tableRow = new TableRow();
    tableRow.setF(tableCells);

    return tableRow;
  }

  // Our process for generating descriptors modifies the names of nested descriptors for wrapper
  // types, so we record them here.
  private static final Set<String> FLOAT_VALUE_DESCRIPTOR_NAMES =
      ImmutableSet.of("google_protobuf_FloatValue", "FloatValue");
  private static final Set<String> DOUBLE_VALUE_DESCRIPTOR_NAMES =
      ImmutableSet.of("google_protobuf_DoubleValue", "DoubleValue");
  private static final Set<String> BOOL_VALUE_DESCRIPTOR_NAMES =
      ImmutableSet.of("google_protobuf_BoolValue", "BoolValue");
  private static final Set<String> INT32_VALUE_DESCRIPTOR_NAMES =
      ImmutableSet.of("google_protobuf_Int32Value", "Int32Value");
  private static final Set<String> INT64_VALUE_DESCRIPTOR_NAMES =
      ImmutableSet.of("google_protobuf_Int64Value", "Int64Value");
  private static final Set<String> UINT32_VALUE_DESCRIPTOR_NAMES =
      ImmutableSet.of("google_protobuf_UInt32Value", "UInt32Value");
  private static final Set<String> UINT64_VALUE_DESCRIPTOR_NAMES =
      ImmutableSet.of("google_protobuf_UInt64Value", "UInt64Value");
  private static final Set<String> BYTES_VALUE_DESCRIPTOR_NAMES =
      ImmutableSet.of("google_protobuf_BytesValue", "BytesValue");
  private static final Set<String> TIMESTAMP_VALUE_DESCRIPTOR_NAMES =
      ImmutableSet.of("google_protobuf_Timestamp", "Timestamp");

  // Translate a proto message value into a json value. If useSetF==false, this will fail with
  // Optional.empty() if
  // any fields named "f" are found (due to restrictions on the TableRow class). In that case, the
  // top level will retry
  // with useSetF==true. We fallback this way in order to maintain backwards compatibility with
  // existing users.
  public static Object jsonValueFromMessageValue(
      SchemaInformation schemaInformation,
      FieldDescriptor fieldDescriptor,
      Object fieldValue,
      boolean expandRepeated,
      Predicate<String> includeField,
      String prefix,
      boolean useSetF) {
    if (expandRepeated && fieldDescriptor.isRepeated()) {
      List<Object> valueList = (List<Object>) fieldValue;
      List<Object> expanded = Lists.newArrayListWithCapacity(valueList.size());
      for (Object value : valueList) {
        Object translatedValue =
            jsonValueFromMessageValue(
                schemaInformation, fieldDescriptor, value, false, includeField, prefix, useSetF);
        if (!useSetF && translatedValue instanceof Optional) {
          Optional<?> optional = (Optional<?>) translatedValue;
          if (!optional.isPresent()) {
            // A nested element contained an "f" column. Fail the call.
            return Optional.empty();
          }
          translatedValue = optional.get();
        }
        expanded.add(translatedValue);
      }
      return expanded;
    }

    // BigQueryIO supports direct proto writes - i.e. we allow the user to pass in their own proto
    // and skip our
    // conversion layer, as long as the proto conforms to the types supported by the BigQuery
    // Storage Write API.
    // For many schema types, the Storage Write API supports different proto field types (often with
    // different
    // encodings), so the mapping of schema type -> proto type is one to many. To read the data out
    // of the proto,
    // we need to examine both the schema type and the proto field type.
    switch (schemaInformation.getType()) {
      case DOUBLE:
        switch (fieldDescriptor.getType()) {
          case FLOAT:
          case DOUBLE:
          case STRING:
            return DECIMAL_FORMAT.format(Double.parseDouble(fieldValue.toString()));
          case MESSAGE:
            // Handle the various number wrapper types.
            Message doubleMessage = (Message) fieldValue;
            if (FLOAT_VALUE_DESCRIPTOR_NAMES.contains(fieldDescriptor.getMessageType().getName())) {
              float floatValue =
                  (float)
                      doubleMessage.getField(
                          doubleMessage.getDescriptorForType().findFieldByName("value"));

              return DECIMAL_FORMAT.format(floatValue);
            } else if (DOUBLE_VALUE_DESCRIPTOR_NAMES.contains(
                fieldDescriptor.getMessageType().getName())) {
              double doubleValue =
                  (double)
                      doubleMessage.getField(
                          doubleMessage.getDescriptorForType().findFieldByName("value"));
              return DECIMAL_FORMAT.format(doubleValue);
            } else {
              throw new RuntimeException(
                  "Not implemented yet " + fieldDescriptor.getMessageType().getName());
            }
          default:
            return fieldValue.toString();
        }
      case BOOL:
        // Wrapper type.
        if (fieldDescriptor.getType().equals(FieldDescriptor.Type.MESSAGE)) {
          Message boolMessage = (Message) fieldValue;
          if (BOOL_VALUE_DESCRIPTOR_NAMES.contains(fieldDescriptor.getMessageType().getName())) {
            return boolMessage
                .getField(boolMessage.getDescriptorForType().findFieldByName("value"))
                .toString();
          } else {
            throw new RuntimeException(
                "Not implemented yet " + fieldDescriptor.getMessageType().getName());
          }
        }
        return fieldValue.toString();
      case JSON:
      case GEOGRAPHY:
        // The above types have native representations in JSON for all their
        // possible values.
      case STRING:
        return fieldValue.toString();
      case INT64:
        switch (fieldDescriptor.getType()) {
          case MESSAGE:
            // Wrapper types.
            Message message = (Message) fieldValue;
            if (INT32_VALUE_DESCRIPTOR_NAMES.contains(fieldDescriptor.getMessageType().getName())) {
              return message
                  .getField(message.getDescriptorForType().findFieldByName("value"))
                  .toString();
            } else if (INT64_VALUE_DESCRIPTOR_NAMES.contains(
                fieldDescriptor.getMessageType().getName())) {
              return message
                  .getField(message.getDescriptorForType().findFieldByName("value"))
                  .toString();
            } else if (UINT32_VALUE_DESCRIPTOR_NAMES.contains(
                fieldDescriptor.getMessageType().getName())) {
              return message
                  .getField(message.getDescriptorForType().findFieldByName("value"))
                  .toString();
            } else if (UINT64_VALUE_DESCRIPTOR_NAMES.contains(
                fieldDescriptor.getMessageType().getName())) {
              return message
                  .getField(message.getDescriptorForType().findFieldByName("value"))
                  .toString();
            } else {
              throw new RuntimeException(
                  "Not implemented yet " + fieldDescriptor.getMessageType().getFullName());
            }
          default:
            return fieldValue.toString();
        }
      case BYTES:
        switch (fieldDescriptor.getType()) {
          case BYTES:
            return BaseEncoding.base64().encode(((ByteString) fieldValue).toByteArray());
          case STRING:
            return BaseEncoding.base64()
                .encode(((String) fieldValue).getBytes(StandardCharsets.UTF_8));
          case MESSAGE:
            Message message = (Message) fieldValue;
            if (BYTES_VALUE_DESCRIPTOR_NAMES.contains(fieldDescriptor.getMessageType().getName())) {
              ByteString byteString =
                  (ByteString)
                      message.getField(message.getDescriptorForType().findFieldByName("value"));
              return BaseEncoding.base64().encode(byteString.toByteArray());
            }
            throw new RuntimeException(
                "Not implemented " + fieldDescriptor.getMessageType().getFullName());
          default:
            return fieldValue.toString();
        }
      case TIMESTAMP:
        if (isProtoFieldTypeInteger(fieldDescriptor.getType())) {
          long epochMicros = Long.valueOf(fieldValue.toString());
          long epochSeconds = epochMicros / 1_000_000L;
          long nanoAdjustment = (epochMicros % 1_000_000L) * 1_000L;
          Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
          return LocalDateTime.ofInstant(instant, ZoneOffset.UTC).format(TIMESTAMP_FORMATTER);
        } else if (fieldDescriptor.getType().equals(FieldDescriptor.Type.MESSAGE)) {
          Message message = (Message) fieldValue;
          if (TIMESTAMP_VALUE_DESCRIPTOR_NAMES.contains(
              fieldDescriptor.getMessageType().getName())) {
            Descriptor descriptor = message.getDescriptorForType();
            long seconds = (long) message.getField(descriptor.findFieldByName("seconds"));
            int nanos = (int) message.getField(descriptor.findFieldByName("nanos"));
            Instant instant = Instant.ofEpochSecond(seconds, nanos);
            return LocalDateTime.ofInstant(instant, ZoneOffset.UTC).format(TIMESTAMP_FORMATTER);
          } else {
            throw new RuntimeException(
                "Not implemented yet " + fieldDescriptor.getMessageType().getFullName());
          }
        } else {
          return fieldValue.toString();
        }

      case DATE:
        if (isProtoFieldTypeInteger(fieldDescriptor.getType())) {
          int intDate = Integer.parseInt(fieldValue.toString());
          return LocalDate.ofEpochDay(intDate).toString();
        } else {
          return fieldValue.toString();
        }
      case NUMERIC:
        switch (fieldDescriptor.getType()) {
          case BYTES:
            ByteString numericByteString = (ByteString) fieldValue;
            return BigDecimalByteStringEncoder.decodeNumericByteString(numericByteString)
                .stripTrailingZeros()
                .toString();
          default:
            return fieldValue.toString();
        }
      case BIGNUMERIC:
        switch (fieldDescriptor.getType()) {
          case BYTES:
            ByteString numericByteString = (ByteString) fieldValue;
            return BigDecimalByteStringEncoder.decodeBigNumericByteString(numericByteString)
                .stripTrailingZeros()
                .toString();
          default:
            return fieldValue.toString();
        }

      case DATETIME:
        if (isProtoFieldTypeInteger(fieldDescriptor.getType())) {
          long packedDateTime = Long.valueOf(fieldValue.toString());
          return CivilTimeEncoder.decodePacked64DatetimeMicrosAsJavaTime(packedDateTime)
              .format(BigQueryUtils.BIGQUERY_DATETIME_FORMATTER);
        } else {
          return fieldValue.toString();
        }

      case TIME:
        if (isProtoFieldTypeInteger(fieldDescriptor.getType())) {
          long packedTime = Long.valueOf(fieldValue.toString());
          return CivilTimeEncoder.decodePacked64TimeMicrosAsJavaTime(packedTime).toString();
        } else {
          return fieldValue.toString();
        }
      case STRUCT:
        return useSetF
            ? tableRowFromMessageUseSetF(
                schemaInformation, (Message) fieldValue, false, includeField, prefix)
            : tableRowFromMessageNoF(
                schemaInformation, (Message) fieldValue, false, includeField, prefix);
      default:
        return fieldValue.toString();
    }
  }

  private static boolean isProtoFieldTypeInteger(FieldDescriptor.Type type) {
    switch (type) {
      case INT32:
      case INT64:
      case UINT32:
      case UINT64:
      case SFIXED32:
      case SFIXED64:
      case SINT64:
        return true;
      default:
        return false;
    }
  }
}
