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
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;

/**
 * Utility methods for converting JSON {@link TableRow} objects to dynamic protocol message, for use
 * with the Storage write API.
 */
public class TableRowToStorageApiProto {
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
          .put("NUMERIC", Type.TYPE_STRING) // Pass through the JSON encoding.
          .put("BIGNUMERIC", Type.TYPE_STRING) // Pass through the JSON encoding.
          .put("GEOGRAPHY", Type.TYPE_STRING) // Pass through the JSON encoding.
          .put("DATE", Type.TYPE_STRING) // Pass through the JSON encoding.
          .put("TIME", Type.TYPE_STRING) // Pass through the JSON encoding.
          .put("DATETIME", Type.TYPE_STRING) // Pass through the JSON encoding.
          .put("TIMESTAMP", Type.TYPE_STRING) // Pass through the JSON encoding.
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

  /**
   * Given a BigQuery TableRow, returns a protocol-buffer message that can be used to write data
   * using the BigQuery Storage API.
   */
  public static DynamicMessage messageFromTableRow(Descriptor descriptor, TableRow tableRow) {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    for (Map.Entry<String, Object> entry : tableRow.entrySet()) {
      @Nullable
      FieldDescriptor fieldDescriptor = descriptor.findFieldByName(entry.getKey().toLowerCase());
      if (fieldDescriptor == null) {
        throw new RuntimeException(
            "TableRow contained unexpected field with name " + entry.getKey());
      }
      @Nullable Object value = messageValueFromFieldValue(fieldDescriptor, entry.getValue());
      if (value != null) {
        builder.setField(fieldDescriptor, value);
      }
    }
    return builder.build();
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
      FieldDescriptor fieldDescriptor, Object bqValue) {
    if (bqValue == null) {
      if (fieldDescriptor.isOptional()) {
        return null;
      } else {
        throw new IllegalArgumentException(
            "Received null value for non-nullable field " + fieldDescriptor.getName());
      }
    }
    return toProtoValue(fieldDescriptor, bqValue, fieldDescriptor.isRepeated());
  }

  private static final Map<FieldDescriptor.Type, Function<String, Object>>
      JSON_PROTO_STRING_PARSERS =
          ImmutableMap.<FieldDescriptor.Type, Function<String, Object>>builder()
              .put(FieldDescriptor.Type.INT32, Integer::valueOf)
              .put(FieldDescriptor.Type.INT64, Long::valueOf)
              .put(FieldDescriptor.Type.FLOAT, Float::valueOf)
              .put(FieldDescriptor.Type.DOUBLE, Double::valueOf)
              .put(FieldDescriptor.Type.BOOL, Boolean::valueOf)
              .put(FieldDescriptor.Type.STRING, str -> str)
              .put(
                  FieldDescriptor.Type.BYTES,
                  b64 -> ByteString.copyFrom(BaseEncoding.base64().decode(b64)))
              .build();

  @Nullable
  @SuppressWarnings({"nullness"})
  @VisibleForTesting
  static Object toProtoValue(
      FieldDescriptor fieldDescriptor, Object jsonBQValue, boolean isRepeated) {
    if (isRepeated) {
      return ((List<Object>) jsonBQValue)
          .stream()
              .map(
                  v -> {
                    if (fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE) {
                      return ((Map<String, Object>) v).get("v");
                    } else {
                      return v;
                    }
                  })
              .map(v -> toProtoValue(fieldDescriptor, v, false))
              .collect(toList());
    }

    if (fieldDescriptor.getType() == FieldDescriptor.Type.MESSAGE) {
      if (jsonBQValue instanceof AbstractMap) {
        // This will handle nested rows.
        TableRow tr = new TableRow();
        tr.putAll((AbstractMap<String, Object>) jsonBQValue);
        return messageFromTableRow(fieldDescriptor.getMessageType(), tr);
      } else {
        throw new RuntimeException("Unexpected value " + jsonBQValue + " Expected a JSON map.");
      }
    }
    @Nullable Object scalarValue = scalarToProtoValue(fieldDescriptor, jsonBQValue);
    if (scalarValue == null) {
      return toProtoValue(fieldDescriptor, jsonBQValue.toString(), isRepeated);
    } else {
      return scalarValue;
    }
  }

  @VisibleForTesting
  @Nullable
  static Object scalarToProtoValue(FieldDescriptor fieldDescriptor, Object jsonBQValue) {
    if (jsonBQValue instanceof String) {
      Function<String, Object> mapper = JSON_PROTO_STRING_PARSERS.get(fieldDescriptor.getType());
      if (mapper == null) {
        throw new UnsupportedOperationException(
            "Converting BigQuery type '"
                + jsonBQValue.getClass()
                + "' to '"
                + fieldDescriptor
                + "' is not supported");
      }
      return mapper.apply((String) jsonBQValue);
    }

    switch (fieldDescriptor.getType()) {
      case BOOL:
        if (jsonBQValue instanceof Boolean) {
          return jsonBQValue;
        }
        break;
      case BYTES:
        break;
      case INT64:
        if (jsonBQValue instanceof Integer) {
          return Long.valueOf((Integer) jsonBQValue);
        } else if (jsonBQValue instanceof Long) {
          return jsonBQValue;
        }
        break;
      case INT32:
        if (jsonBQValue instanceof Integer) {
          return jsonBQValue;
        }
        break;
      case STRING:
        break;
      case DOUBLE:
        if (jsonBQValue instanceof Double) {
          return jsonBQValue;
        } else if (jsonBQValue instanceof Float) {
          return Double.valueOf((Float) jsonBQValue);
        }
        break;
      default:
        throw new RuntimeException("Unsupported proto type " + fieldDescriptor.getType());
    }
    return null;
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
