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

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static java.util.stream.Collectors.toList;

/**
 * Utility methods for converting JSON {@link TableRow} objects to dynamic protocol message, for use
 * with the Storage write API.
 */
public class TableRowToStorageApiProto {

  // see protocol buffer type and bigquery type conversion
  // https://cloud.google.com/bigquery/docs/write-api#data_type_conversions
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
          .put("NUMERIC", Type.TYPE_STRING)
          .put("BIGNUMERIC", Type.TYPE_STRING)
          .put("GEOGRAPHY", Type.TYPE_STRING)
          .put("DATE", Type.TYPE_INT32)
          .put("TIME", Type.TYPE_STRING)
          .put("DATETIME", Type.TYPE_STRING)
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

  private static TableFieldSchema getByName(List<TableFieldSchema> tableFieldSchemaList, String name) {
    for (TableFieldSchema tableFieldSchema : tableFieldSchemaList) {
      if (tableFieldSchema.getName().equals(name))
        return tableFieldSchema;
    }
    throw new RuntimeException("cannot find table schema for " + name);
  }

  public static DynamicMessage messageFromMap(
      List<TableFieldSchema> tableFieldSchemaList,
      Descriptor descriptor, AbstractMap<String, Object> map) {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    for (Map.Entry<String, Object> entry : map.entrySet()) {

      @Nullable
      FieldDescriptor fieldDescriptor = descriptor.findFieldByName(entry.getKey().toLowerCase());
      if (fieldDescriptor == null) {
        throw new RuntimeException(
            "TableRow contained unexpected field with name " + entry.getKey());
      }
      TableFieldSchema tableFieldSchema = getByName(tableFieldSchemaList, entry.getKey());
      @Nullable Object value = messageValueFromFieldValue(tableFieldSchema, fieldDescriptor, entry.getValue());
      if (value != null) {
        builder.setField(fieldDescriptor, value);
      }
    }
    return builder.build();
  }

  /**
   * Given a BigQuery TableRow, returns a protocol-buffer message that can be used to write data
   * using the BigQuery Storage API.
   */
  public static DynamicMessage messageFromTableRow(List<TableFieldSchema> tableFieldSchemaList, Descriptor descriptor, TableRow tableRow) {
    @Nullable List<TableCell> cells = tableRow.getF();
    if (cells != null) {
      DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
      if (cells.size() > descriptor.getFields().size()) {
        throw new RuntimeException("TableRow contained too many fields");
      }
      for (int i = 0; i < cells.size(); ++i) {
        TableCell cell = cells.get(i);
        FieldDescriptor fieldDescriptor = descriptor.getFields().get(i);
        TableFieldSchema tableFieldSchema = tableFieldSchemaList.get(i);
        @Nullable Object value = messageValueFromFieldValue(tableFieldSchema, fieldDescriptor, cell.getV());
        if (value != null) {
          builder.setField(fieldDescriptor, value);
        }
      }

      return builder.build();
    } else {
      return messageFromMap(tableFieldSchemaList, descriptor, tableRow);
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

  @Nullable
  private static Object messageValueFromFieldValue(
      TableFieldSchema tableFieldSchema,
      FieldDescriptor fieldDescriptor, Object bqValue) {
    if (bqValue == null) {
      if (fieldDescriptor.isOptional()) {
        return null;
      } else if (fieldDescriptor.isRepeated()) {
        return Collections.emptyList();
      }
      {
        throw new IllegalArgumentException(
            "Received null value for non-nullable field " + fieldDescriptor.getName());
      }
    }
    return toProtoValue(tableFieldSchema, fieldDescriptor, bqValue, fieldDescriptor.isRepeated());
  }

  @Nullable
  @SuppressWarnings({"nullness"})
  @VisibleForTesting
  static Object toProtoValue(
      TableFieldSchema tableFieldSchema,
      FieldDescriptor fieldDescriptor, Object jsonBQValue, boolean isRepeated) {
    if (isRepeated) {
      return ((List<Object>) jsonBQValue)
          .stream().map(v -> toProtoValue(tableFieldSchema, fieldDescriptor, v, false)).collect(toList());
    }

    if (fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE) {
      if (jsonBQValue instanceof TableRow) {
        TableRow tableRow = (TableRow) jsonBQValue;
        return messageFromTableRow(tableFieldSchema.getFields(), fieldDescriptor.getMessageType(), tableRow);
      } else if (jsonBQValue instanceof AbstractMap) {
        // This will handle nested rows.
        AbstractMap<String, Object> map = ((AbstractMap<String, Object>) jsonBQValue);
        return messageFromMap(tableFieldSchema.getFields(), fieldDescriptor.getMessageType(), map);
      } else {
        throw new RuntimeException("Unexpected value " + jsonBQValue + " Expected a JSON map.");
      }
    }
    return scalarToProtoValue(tableFieldSchema, jsonBQValue);
  }

  @VisibleForTesting
  @Nullable
  static Object scalarToProtoValue(TableFieldSchema tableFieldSchema, Object jsonBQValue) {
    if (jsonBQValue == null) // nullable value
      return null;

    switch (tableFieldSchema.getType()) {
      case "INT64":
      case "INTEGER":
        if (jsonBQValue instanceof String) {
          return Long.valueOf((String) jsonBQValue);
        } else if (jsonBQValue instanceof Integer) {
          return (long) jsonBQValue;
        } else if (jsonBQValue instanceof Long) {
          return jsonBQValue;
        }
        break;
      case "FLOAT64":
      case "FLOAT":
        if (jsonBQValue instanceof String) {
          return Double.valueOf((String) jsonBQValue);
        } else if (jsonBQValue instanceof Double) {
          return jsonBQValue;
        } else if (jsonBQValue instanceof Float) {
          return (double) jsonBQValue;
        }
        break;
      case "BOOLEAN":
      case "BOOL":
        if (jsonBQValue instanceof String) {
          return Boolean.valueOf((String) jsonBQValue);
        } else if (jsonBQValue instanceof Boolean) {
          return jsonBQValue;
        }
        break;
      case "BYTES":
        if (jsonBQValue instanceof String) {
          return ByteString.copyFrom(BaseEncoding.base64().decode((String) jsonBQValue));
        } else if (jsonBQValue instanceof byte[]) {
          return ByteString.copyFrom((byte[]) jsonBQValue);
        } else if (jsonBQValue instanceof ByteString) {
          return jsonBQValue;
        }
        break;
      case "TIMESTAMP":
        if (jsonBQValue instanceof String) {
          return ChronoUnit.MICROS.between(Instant.EPOCH, Instant.parse((String) jsonBQValue));
        } else if (jsonBQValue instanceof Instant) {
          return ChronoUnit.MICROS.between(Instant.EPOCH, (Instant) jsonBQValue);
        } else if (jsonBQValue instanceof Timestamp) {
          return ChronoUnit.MICROS.between(Instant.EPOCH, ((Timestamp) jsonBQValue).toInstant());
        } else if (jsonBQValue instanceof Long) {
          return jsonBQValue;
        } else if (jsonBQValue instanceof Integer) {
          return (long) jsonBQValue;
        }
        break;
      case "DATE":
        if (jsonBQValue instanceof String) {
          return (int) LocalDate.parse((String) jsonBQValue).toEpochDay();
        } else if (jsonBQValue instanceof LocalDate) {
          return (int) ((LocalDate) jsonBQValue).toEpochDay();
        }
        break;
      case "STRING":
      case "NUMERIC":
      case "BIGNUMERIC":
      case "TIME":
      case "DATETIME":
      case "JSON":
      case "GEOGRAPHY":
        return jsonBQValue.toString();
    }

    throw new RuntimeException("Unexpected value :" + jsonBQValue + ", type: " + jsonBQValue.getClass() +
        ". Table field name: " + tableFieldSchema.getName() + ", type: " + tableFieldSchema.getType());
  }

  @VisibleForTesting
  public static TableRow tableRowFromMessage(Message message) {
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
      default:
        return fieldValue.toString();
    }
  }
}
