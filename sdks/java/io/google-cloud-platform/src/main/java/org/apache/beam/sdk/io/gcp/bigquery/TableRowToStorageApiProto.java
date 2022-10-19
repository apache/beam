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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Days;

/**
 * Utility methods for converting JSON {@link TableRow} objects to dynamic protocol message, for use
 * with the Storage write API.
 */
public class TableRowToStorageApiProto {
  // Custom formatter that accepts "2022-05-09 18:04:59.123456"
  // The old dremel parser accepts this format, and so does insertall. We need to accept it
  // for backwards compatibility, and it is based on UTC time.
  private static final DateTimeFormatter DATETIME_SPACE_FORMATTER =
      new DateTimeFormatterBuilder()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .appendLiteral(' ')
          .append(DateTimeFormatter.ISO_LOCAL_TIME)
          .toFormatter()
          .withZone(ZoneOffset.UTC);

  public static class SchemaConversionException extends Exception {
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

  static class SchemaInformation {
    private final TableFieldSchema tableFieldSchema;
    private final List<SchemaInformation> subFields;
    private final Map<String, SchemaInformation> subFieldsByName;
    private final Iterable<SchemaInformation> parentSchemas;

    private SchemaInformation(TableFieldSchema tableFieldSchema) {
      this(tableFieldSchema, Collections.emptyList());
    }

    private SchemaInformation(
        TableFieldSchema tableFieldSchema, Iterable<SchemaInformation> parentSchemas) {
      this.tableFieldSchema = tableFieldSchema;
      this.subFields = Lists.newArrayList();
      this.subFieldsByName = Maps.newHashMap();
      this.parentSchemas = parentSchemas;
      if (tableFieldSchema.getFields() != null) {
        for (TableFieldSchema field : tableFieldSchema.getFields()) {
          SchemaInformation schemaInformation =
              new SchemaInformation(
                  field, Iterables.concat(this.parentSchemas, ImmutableList.of(this)));
          subFields.add(schemaInformation);
          subFieldsByName.put(field.getName(), schemaInformation);
        }
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

    public String getType() {
      return tableFieldSchema.getType();
    }

    public SchemaInformation getSchemaForField(String name) {
      SchemaInformation schemaInformation = subFieldsByName.get(name);
      if (schemaInformation == null) {
        throw new RuntimeException("Schema field not found: " + name);
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

    static SchemaInformation fromTableSchema(TableSchema tableSchema) {
      TableFieldSchema rootSchema =
          new TableFieldSchema()
              .setName("__root__")
              .setType("RECORD")
              .setFields(tableSchema.getFields());
      return new SchemaInformation(rootSchema);
    }
  }

  static final Map<String, Type> PRIMITIVE_TYPES =
      ImmutableMap.<String, Type>builder()
          .put("INT64", Type.TYPE_INT64)
          .put("INTEGER", Type.TYPE_INT64)
          .put("FLOAT64", Type.TYPE_DOUBLE)
          .put("FLOAT", Type.TYPE_DOUBLE)
          .put("STRING", Type.TYPE_STRING)
          .put("BOOL", Type.TYPE_BOOL)
          .put("BOOLEAN", Type.TYPE_BOOL)
          .put("BYTES", Type.TYPE_BYTES)
          .put("NUMERIC", Type.TYPE_BYTES)
          .put("BIGNUMERIC", Type.TYPE_BYTES)
          .put("GEOGRAPHY", Type.TYPE_STRING) // Pass through the JSON encoding.
          .put("DATE", Type.TYPE_INT32)
          .put("TIME", Type.TYPE_INT64)
          .put("DATETIME", Type.TYPE_INT64)
          .put("TIMESTAMP", Type.TYPE_INT64)
          .put("JSON", Type.TYPE_STRING)
          .build();

  /**
   * Given a BigQuery TableSchema, returns a protocol-buffer Descriptor that can be used to write
   * data using the BigQuery Storage API.
   */
  public static Descriptor getDescriptorFromTableSchema(TableSchema jsonSchema)
      throws DescriptorValidationException {
    DescriptorProto descriptorProto = descriptorSchemaFromTableSchema(jsonSchema);
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
      boolean ignoreUnknownValues)
      throws SchemaConversionException {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      @Nullable
      FieldDescriptor fieldDescriptor = descriptor.findFieldByName(entry.getKey().toLowerCase());
      if (fieldDescriptor == null) {
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
        @Nullable
        Object value =
            messageValueFromFieldValue(
                fieldSchemaInformation, fieldDescriptor, entry.getValue(), ignoreUnknownValues);
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
    try {
      return builder.build();
    } catch (Exception e) {
      throw new SchemaDoesntMatchException(
          "Couldn't convert schema for " + schemaInformation.getFullName(), e);
    }
  }

  /**
   * Given a BigQuery TableRow, returns a protocol-buffer message that can be used to write data
   * using the BigQuery Storage API.
   */
  public static DynamicMessage messageFromTableRow(
      SchemaInformation schemaInformation,
      Descriptor descriptor,
      TableRow tableRow,
      boolean ignoreUnkownValues)
      throws SchemaConversionException {
    @Nullable Object fValue = tableRow.get("f");
    if (fValue instanceof List) {
      List<AbstractMap<String, Object>> cells = (List<AbstractMap<String, Object>>) fValue;
      DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
      int cellsToProcess = cells.size();
      if (cells.size() > descriptor.getFields().size()) {
        if (ignoreUnkownValues) {
          cellsToProcess = descriptor.getFields().size();
        } else {
          throw new SchemaTooNarrowException(
              "TableRow contained too many fields and ignoreUnknownValues not set.");
        }
      }
      for (int i = 0; i < cellsToProcess; ++i) {
        AbstractMap<String, Object> cell = cells.get(i);
        FieldDescriptor fieldDescriptor = descriptor.getFields().get(i);
        SchemaInformation fieldSchemaInformation = schemaInformation.getSchemaForField(i);
        try {
          @Nullable
          Object value =
              messageValueFromFieldValue(
                  fieldSchemaInformation, fieldDescriptor, cell.get("v"), ignoreUnkownValues);
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

      try {
        return builder.build();
      } catch (Exception e) {
        throw new SchemaDoesntMatchException(
            "Could convert schema for " + schemaInformation.getFullName(), e);
      }
    } else {
      return messageFromMap(schemaInformation, descriptor, tableRow, ignoreUnkownValues);
    }
  }

  @VisibleForTesting
  static DescriptorProto descriptorSchemaFromTableSchema(TableSchema tableSchema) {
    return descriptorSchemaFromTableFieldSchemas(tableSchema.getFields());
  }

  private static DescriptorProto descriptorSchemaFromTableFieldSchemas(
      Iterable<TableFieldSchema> tableFieldSchemas) {
    DescriptorProto.Builder descriptorBuilder = DescriptorProto.newBuilder();
    // Create a unique name for the descriptor ('-' characters cannot be used).
    descriptorBuilder.setName("D" + UUID.randomUUID().toString().replace("-", "_"));
    int i = 1;
    for (TableFieldSchema fieldSchema : tableFieldSchemas) {
      fieldDescriptorFromTableField(fieldSchema, i++, descriptorBuilder);
    }
    return descriptorBuilder.build();
  }

  private static void fieldDescriptorFromTableField(
      TableFieldSchema fieldSchema, int fieldNumber, DescriptorProto.Builder descriptorBuilder) {
    FieldDescriptorProto.Builder fieldDescriptorBuilder = FieldDescriptorProto.newBuilder();
    fieldDescriptorBuilder = fieldDescriptorBuilder.setName(fieldSchema.getName().toLowerCase());
    fieldDescriptorBuilder = fieldDescriptorBuilder.setNumber(fieldNumber);
    switch (fieldSchema.getType()) {
      case "STRUCT":
      case "RECORD":
        DescriptorProto nested = descriptorSchemaFromTableFieldSchemas(fieldSchema.getFields());
        descriptorBuilder.addNestedType(nested);
        fieldDescriptorBuilder =
            fieldDescriptorBuilder.setType(Type.TYPE_MESSAGE).setTypeName(nested.getName());
        break;
      default:
        @Nullable Type type = PRIMITIVE_TYPES.get(fieldSchema.getType());
        if (type == null) {
          throw new UnsupportedOperationException(
              "Converting BigQuery type " + fieldSchema.getType() + " to Beam type is unsupported");
        }
        fieldDescriptorBuilder = fieldDescriptorBuilder.setType(type);
    }

    Optional<Mode> fieldMode = Optional.ofNullable(fieldSchema.getMode()).map(Mode::valueOf);
    if (fieldMode.filter(m -> m == Mode.REPEATED).isPresent()) {
      fieldDescriptorBuilder = fieldDescriptorBuilder.setLabel(Label.LABEL_REPEATED);
    } else if (!fieldMode.isPresent() || fieldMode.filter(m -> m == Mode.NULLABLE).isPresent()) {
      fieldDescriptorBuilder = fieldDescriptorBuilder.setLabel(Label.LABEL_OPTIONAL);
    } else {
      fieldDescriptorBuilder = fieldDescriptorBuilder.setLabel(Label.LABEL_REQUIRED);
    }
    descriptorBuilder.addField(fieldDescriptorBuilder.build());
  }

  private static @Nullable Object messageValueFromFieldValue(
      SchemaInformation schemaInformation,
      FieldDescriptor fieldDescriptor,
      @Nullable Object bqValue,
      boolean ignoreUnknownValues)
      throws SchemaConversionException {
    if (bqValue == null) {
      if (fieldDescriptor.isOptional()) {
        return null;
      } else if (fieldDescriptor.isRepeated()) {
        return Collections.emptyList();
      } else {
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
                  schemaInformation, fieldDescriptor, o, ignoreUnknownValues));
        }
      }
      return protoList;
    }
    return singularFieldToProtoValue(
        schemaInformation, fieldDescriptor, bqValue, ignoreUnknownValues);
  }

  @VisibleForTesting
  static @Nullable Object singularFieldToProtoValue(
      SchemaInformation schemaInformation,
      FieldDescriptor fieldDescriptor,
      @Nullable Object value,
      boolean ignoreUnknownValues)
      throws SchemaConversionException {
    switch (schemaInformation.getType()) {
      case "INT64":
      case "INTEGER":
        if (value instanceof String) {
          return Long.valueOf((String) value);
        } else if (value instanceof Integer || value instanceof Long) {
          return ((Number) value).longValue();
        }
        break;
      case "FLOAT64":
      case "FLOAT":
        if (value instanceof String) {
          return Double.valueOf((String) value);
        } else if (value instanceof Number) {
          return ((Number) value).doubleValue();
        }
        break;
      case "BOOLEAN":
      case "BOOL":
        if (value instanceof String) {
          return Boolean.valueOf((String) value);
        } else if (value instanceof Boolean) {
          return value;
        }
        break;
      case "BYTES":
        if (value instanceof String) {
          return ByteString.copyFrom(BaseEncoding.base64().decode((String) value));
        } else if (value instanceof byte[]) {
          return ByteString.copyFrom((byte[]) value);
        } else if (value instanceof ByteString) {
          return value;
        }
        break;
      case "TIMESTAMP":
        if (value instanceof String) {
          try {
            // '2011-12-03T10:15:30+01:00' '2011-12-03T10:15:30'
            return ChronoUnit.MICROS.between(
                Instant.EPOCH, Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse((String) value)));
          } catch (DateTimeParseException e) {
            try {
              // "12345667"
              return ChronoUnit.MICROS.between(
                  Instant.EPOCH, Instant.ofEpochMilli(Long.parseLong((String) value)));
            } catch (NumberFormatException e2) {
              // "yyyy-MM-dd HH:mm:ss.SSSSSS"
              return ChronoUnit.MICROS.between(
                  Instant.EPOCH, Instant.from(DATETIME_SPACE_FORMATTER.parse((String) value)));
            }
          }
        } else if (value instanceof Instant) {
          return ChronoUnit.MICROS.between(Instant.EPOCH, (Instant) value);
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
        break;
      case "DATE":
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
        break;
      case "NUMERIC":
        if (value instanceof String) {
          return BigDecimalByteStringEncoder.encodeToNumericByteString(
              new BigDecimal((String) value));
        } else if (value instanceof BigDecimal) {
          return BigDecimalByteStringEncoder.encodeToNumericByteString(((BigDecimal) value));
        } else if (value instanceof Double || value instanceof Float) {
          return BigDecimalByteStringEncoder.encodeToNumericByteString(
              BigDecimal.valueOf(((Number) value).doubleValue()));
        }
        break;
      case "BIGNUMERIC":
        if (value instanceof String) {
          return BigDecimalByteStringEncoder.encodeToBigNumericByteString(
              new BigDecimal((String) value));
        } else if (value instanceof BigDecimal) {
          return BigDecimalByteStringEncoder.encodeToBigNumericByteString(((BigDecimal) value));
        } else if (value instanceof Double || value instanceof Float) {
          return BigDecimalByteStringEncoder.encodeToBigNumericByteString(
              BigDecimal.valueOf(((Number) value).doubleValue()));
        }
        break;
      case "DATETIME":
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
          return CivilTimeEncoder.encodePacked64DatetimeMicros((org.joda.time.LocalDateTime) value);
        }
        break;
      case "TIME":
        if (value instanceof String) {
          return CivilTimeEncoder.encodePacked64TimeMicros(LocalTime.parse((String) value));
        } else if (value instanceof Number) {
          return ((Number) value).longValue();
        } else if (value instanceof LocalTime) {
          return CivilTimeEncoder.encodePacked64TimeMicros((LocalTime) value);
        } else if (value instanceof org.joda.time.LocalTime) {
          return CivilTimeEncoder.encodePacked64TimeMicros((org.joda.time.LocalTime) value);
        }
        break;
      case "STRING":
      case "JSON":
      case "GEOGRAPHY":
        return Preconditions.checkArgumentNotNull(value).toString();
      case "STRUCT":
      case "RECORD":
        if (value instanceof TableRow) {
          TableRow tableRow = (TableRow) value;
          return messageFromTableRow(
              schemaInformation, fieldDescriptor.getMessageType(), tableRow, ignoreUnknownValues);
        } else if (value instanceof AbstractMap) {
          // This will handle nested rows.
          AbstractMap<String, Object> map = ((AbstractMap<String, Object>) value);
          return messageFromMap(
              schemaInformation, fieldDescriptor.getMessageType(), map, ignoreUnknownValues);
        }
        break;
    }

    throw new SchemaDoesntMatchException(
        "Unexpected value :"
            + value
            + ", type: "
            + (value == null ? "null" : value.getClass())
            + ". Table field name: "
            + schemaInformation.getFullName()
            + ", type: "
            + schemaInformation.getType());
  }

  @VisibleForTesting
  public static TableRow tableRowFromMessage(Message message) {
    // TODO: Would be more correct to generate TableRows using setF.
    TableRow tableRow = new TableRow();
    for (Map.Entry<FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
      FieldDescriptor fieldDescriptor = field.getKey();
      Object fieldValue = field.getValue();
      tableRow.putIfAbsent(
          fieldDescriptor.getName(), jsonValueFromMessageValue(fieldDescriptor, fieldValue, true));
    }
    return tableRow;
  }

  public static Object jsonValueFromMessageValue(
      FieldDescriptor fieldDescriptor, Object fieldValue, boolean expandRepeated) {
    if (expandRepeated && fieldDescriptor.isRepeated()) {
      List<Object> valueList = (List<Object>) fieldValue;
      return valueList.stream()
          .map(v -> jsonValueFromMessageValue(fieldDescriptor, v, false))
          .collect(toList());
    }

    switch (fieldDescriptor.getType()) {
      case GROUP:
      case MESSAGE:
        return tableRowFromMessage((Message) fieldValue);
      case BYTES:
        return BaseEncoding.base64().encode(((ByteString) fieldValue).toByteArray());
      case ENUM:
        throw new RuntimeException("Enumerations not supported");
      case INT32:
      case FLOAT:
      case BOOL:
      case DOUBLE:
        // The above types have native representations in JSON for all their
        // possible values.
        return fieldValue;
      case STRING:
      case INT64:
      default:
        // The above types must be cast to string to be safely encoded in
        // JSON (due to JSON's float-based representation of all numbers).
        return fieldValue.toString();
    }
  }
}
