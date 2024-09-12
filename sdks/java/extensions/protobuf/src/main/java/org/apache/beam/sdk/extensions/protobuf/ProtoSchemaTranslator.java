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
package org.apache.beam.sdk.extensions.protobuf;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.Fixed32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.Fixed64;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SFixed32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SFixed64;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SInt32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SInt64;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.UInt32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.UInt64;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This class provides utilities for inferring a Beam schema from a protocol buffer.
 *
 * <p>The following proto primitive types map to the following Beam types:
 *
 * <ul>
 *   <li>INT32 maps to FieldType.INT32
 *   <li>INT64 maps to FieldType.INT64
 *   <li>FLOAT maps to FieldType.FLOAT
 *   <li>DOUBLE maps to FieldType.DOUBLE
 *   <li>BOOL maps to FieldType.BOOLEAN
 *   <li>STRING maps to FieldType.STRING
 *   <li>BYTES maps to FieldType.BYTES
 * </ul>
 *
 * <p>The following proto numeric types do not have have native Beam primitive types. LogicalType
 * objects were created to represent these types. Normal numeric types are used as the base type of
 * each of these logical types, so SQL queries should work as normal.
 *
 * <ul>
 *   <li>UINT32 maps to FieldType.logicalType(new ProtoSchemaLogicalTypes.UInt32()))
 *   <li>SINT32 maps to FieldType.logicalType(new ProtoSchemaLogicalTypes.SInt32()))
 *   <li>FIXED32 maps to FieldType.logicalType(new ProtoSchemaLogicalTypes.Fixed32()))
 *   <li>SFIXED32 maps to FieldType.logicalType(new ProtoSchemaLogicalTypes.SFixed32()))
 *   <li>UINT64 maps to FieldType.logicalType(new ProtoSchemaLogicalTypes.UInt64()))
 *   <li>SINT64 maps to FieldType.logicalType(new ProtoSchemaLogicalTypes.SInt64()))
 *   <li>FIXED64 maps to FieldType.logicalType(new ProtoSchemaLogicalTypes.Fixed64()))
 *   <li>SFIXED64 maps to FieldType.logicalType(new ProtoSchemaLogicalTypes.SFixed64()))
 * </ul>
 *
 * <p>Protobuf maps are mapped to Beam FieldType.MAP types. Protobuf repeated fields are mapped to
 * Beam FieldType.ARRAY types.
 *
 * <p>Beam schemas include the EnumerationType logical type to represent enumerations, and protobuf
 * enumerations are translated to this logical type. The base representation type for this logical
 * type is an INT32.
 *
 * <p>Beam schemas include the OneOfType logical type to represent unions, and protobuf oneOfs are
 * translated to this logical type. The base representation type for this logical type is a subrow
 * containing an optional field for each oneof option.
 *
 * <p>google.com.protobuf.Timestamp messages cannot be translated to FieldType.DATETIME types, as
 * the proto type represents nanonseconds and Beam's native type does not currently support that. a
 * new TimestampNanos logical type has been introduced to allow representing nanosecond timestamp,
 * as well as a DurationNanos logical type to represent google.com.protobuf.Duration types.
 *
 * <p>As primitive types are mapped to a <b>not</b> nullable scalar type their nullable counter
 * parts "wrapper classes" are translated to nullable types, as follows.
 *
 * <ul>
 *   <li>google.protobuf.Int32Value maps to a nullable FieldType.INT32
 *   <li>google.protobuf.Int64Value maps to a nullable FieldType.INT64
 *   <li>google.protobuf.UInt32Value maps to a nullable FieldType.logicalType(new UInt32())
 *   <li>google.protobuf.UInt64Value maps to a nullable Field.logicalType(new UInt64())
 *   <li>google.protobuf.FloatValue maps to a nullable FieldType.FLOAT
 *   <li>google.protobuf.DoubleValue maps to a nullable FieldType.DOUBLE
 *   <li>google.protobuf.BoolValue maps to a nullable FieldType.BOOLEAN
 *   <li>google.protobuf.StringValue maps to a nullable FieldType.STRING
 *   <li>google.protobuf.BytesValue maps to a nullable FieldType.BYTES
 * </ul>
 *
 * <p>All message in Protobuf are translated to a nullable Row, except for the well known types
 * listed above. The rest of the nullable rules are as follows.
 *
 * <ul>
 *   <li>Proto3 primitive types are <b>not</b> nullable
 *   <li>Proto2 required types are <b>not</b> nullable
 *   <li>Proto2 optional are <b>not</b> nullable as having an optional value doesn't mean it has not
 *       value. The spec states it has the optional value.
 *   <li>Arrays are <b>not</b> nullable, as proto arrays always have an empty array when no value is
 *       set.
 *   <li>Maps are <b>not</b> nullable, as proto maps always have an empty map when no value is set
 *   <li>Elements in an array are <b>not</b> nullable, as nulls are not allowed in an array
 *   <li>Names and Values are <b>not</b> nullable, as nulls are not allowed. Rows are nullable, as
 *       messages are nullable.
 *   <li>Messages, as well as Well Known Types are nullable, unless using proto2 and the required
 *       label is specified.
 * </ul>
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
class ProtoSchemaTranslator {
  public static final String SCHEMA_OPTION_META_NUMBER = "beam:option:proto:meta:number";

  public static final String SCHEMA_OPTION_META_TYPE_NAME = "beam:option:proto:meta:type_name";

  /** Option prefix for options on messages. */
  public static final String SCHEMA_OPTION_MESSAGE_PREFIX = "beam:option:proto:message:";

  /** Option prefix for options on fields. */
  public static final String SCHEMA_OPTION_FIELD_PREFIX = "beam:option:proto:field:";

  /**
   * A HashMap containing the sentinel values (null values) of schemas in the process of being
   * inferenced, to prevent circular references.
   */
  private static Map<Descriptors.Descriptor, @Nullable Schema> alreadyVisitedSchemas =
      new HashMap<Descriptors.Descriptor, @Nullable Schema>();

  /** Attach a proto field number to a type. */
  static Field withFieldNumber(Field field, int number) {
    return field.withOptions(
        Schema.Options.builder().setOption(SCHEMA_OPTION_META_NUMBER, FieldType.INT32, number));
  }

  /** Return the proto field number for a type. */
  static int getFieldNumber(Field field) {
    return field.getOptions().getValue(SCHEMA_OPTION_META_NUMBER);
  }

  /** Return a Beam schema representing a proto class. */
  static Schema getSchema(Class<? extends Message> clazz) {
    return getSchema(ProtobufUtil.getDescriptorForClass(clazz));
  }

  static synchronized Schema getSchema(Descriptors.Descriptor descriptor) {
    if (alreadyVisitedSchemas.containsKey(descriptor)) {
      @Nullable Schema existingSchema = alreadyVisitedSchemas.get(descriptor);
      if (existingSchema == null) {
        String name = descriptor.getFullName();
        if ("google.protobuf.Struct".equals(name)) {
          throw new UnsupportedOperationException("Infer schema of Struct type is not supported.");
        }
        throw new IllegalArgumentException(
            "Cannot infer schema with a circular reference. Proto Field: " + name);
      }
      return existingSchema;
    }
    alreadyVisitedSchemas.put(descriptor, null);
    /* OneOfComponentFields refers to the field number in the protobuf where the component subfields
     * are. This is needed to prevent double inclusion of the component fields.*/
    Set<Integer> oneOfComponentFields = Sets.newHashSet();
    /* OneOfFieldLocation stores the field number of the first field in the OneOf. Using this, we can use the location
    of the first field in the OneOf as the location of the entire OneOf.*/
    Map<Integer, Field> oneOfFieldLocation = Maps.newHashMap();
    List<Field> fields = Lists.newArrayListWithCapacity(descriptor.getFields().size());
    for (OneofDescriptor oneofDescriptor : descriptor.getOneofs()) {
      List<Field> subFields = Lists.newArrayListWithCapacity(oneofDescriptor.getFieldCount());
      Map<String, Integer> enumIds = Maps.newHashMap();
      for (FieldDescriptor fieldDescriptor : oneofDescriptor.getFields()) {
        oneOfComponentFields.add(fieldDescriptor.getNumber());
        // Store proto field number in a field option.
        FieldType fieldType = beamFieldTypeFromProtoField(fieldDescriptor);
        subFields.add(
            withFieldNumber(
                Field.nullable(fieldDescriptor.getName(), fieldType), fieldDescriptor.getNumber()));
        checkArgument(
            enumIds.putIfAbsent(fieldDescriptor.getName(), fieldDescriptor.getNumber()) == null);
      }
      FieldType oneOfType = FieldType.logicalType(OneOfType.create(subFields, enumIds));
      oneOfFieldLocation.put(
          oneofDescriptor.getFields().get(0).getNumber(),
          Field.of(oneofDescriptor.getName(), oneOfType));
    }

    for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
      int fieldDescriptorNumber = fieldDescriptor.getNumber();
      if (!(oneOfComponentFields.contains(fieldDescriptorNumber)
          && fieldDescriptor.getRealContainingOneof() != null)) {
        // Store proto field number in metadata.
        FieldType fieldType = beamFieldTypeFromProtoField(fieldDescriptor);
        fields.add(
            withFieldNumber(Field.of(fieldDescriptor.getName(), fieldType), fieldDescriptorNumber)
                .withOptions(getFieldOptions(fieldDescriptor)));
        /* Note that descriptor.getFields() returns an iterator in the order of the fields in the .proto file, not
         * in field number order. Therefore we can safely insert the OneOfField at the field of its first component.*/
      } else {
        Field oneOfField = oneOfFieldLocation.get(fieldDescriptorNumber);
        if (oneOfField != null) {
          fields.add(oneOfField);
        }
      }
    }

    Schema generatedSchema =
        Schema.builder()
            .addFields(fields)
            .setOptions(
                getSchemaOptions(descriptor)
                    .setOption(
                        SCHEMA_OPTION_META_TYPE_NAME, FieldType.STRING, descriptor.getFullName()))
            .build();
    alreadyVisitedSchemas.put(descriptor, generatedSchema);

    return generatedSchema;
  }

  private static FieldType beamFieldTypeFromProtoField(
      Descriptors.FieldDescriptor protoFieldDescriptor) {
    FieldType fieldType = null;
    if (protoFieldDescriptor.isMapField()) {
      FieldDescriptor keyFieldDescriptor =
          protoFieldDescriptor.getMessageType().findFieldByName("key");
      FieldDescriptor valueFieldDescriptor =
          protoFieldDescriptor.getMessageType().findFieldByName("value");
      fieldType =
          FieldType.map(
              beamFieldTypeFromProtoField(keyFieldDescriptor).withNullable(false),
              beamFieldTypeFromProtoField(valueFieldDescriptor).withNullable(false));
    } else if (protoFieldDescriptor.isRepeated()) {
      fieldType =
          FieldType.array(
              beamFieldTypeFromSingularProtoField(protoFieldDescriptor).withNullable(false));
    } else {
      fieldType = beamFieldTypeFromSingularProtoField(protoFieldDescriptor);
    }
    return fieldType;
  }

  private static FieldType beamFieldTypeFromSingularProtoField(
      Descriptors.FieldDescriptor protoFieldDescriptor) {
    Descriptors.FieldDescriptor.Type fieldDescriptor = protoFieldDescriptor.getType();
    FieldType fieldType;
    switch (fieldDescriptor) {
      case INT32:
        fieldType = FieldType.INT32;
        break;
      case INT64:
        fieldType = FieldType.INT64;
        break;
      case FLOAT:
        fieldType = FieldType.FLOAT;
        break;
      case DOUBLE:
        fieldType = FieldType.DOUBLE;
        break;
      case BOOL:
        fieldType = FieldType.BOOLEAN;
        break;
      case STRING:
        fieldType = FieldType.STRING;
        break;
      case BYTES:
        fieldType = FieldType.BYTES;
        break;
      case UINT32:
        fieldType = FieldType.logicalType(new UInt32());
        break;
      case SINT32:
        fieldType = FieldType.logicalType(new SInt32());
        break;
      case FIXED32:
        fieldType = FieldType.logicalType(new Fixed32());
        break;
      case SFIXED32:
        fieldType = FieldType.logicalType(new SFixed32());
        break;
      case UINT64:
        fieldType = FieldType.logicalType(new UInt64());
        break;
      case SINT64:
        fieldType = FieldType.logicalType(new SInt64());
        break;
      case FIXED64:
        fieldType = FieldType.logicalType(new Fixed64());
        break;
      case SFIXED64:
        fieldType = FieldType.logicalType(new SFixed64());
        break;

      case ENUM:
        Map<String, Integer> enumValues = Maps.newHashMap();
        for (EnumValueDescriptor enumValue : protoFieldDescriptor.getEnumType().getValues()) {
          if (enumValues.putIfAbsent(enumValue.getName(), enumValue.getNumber()) != null) {
            throw new RuntimeException("Aliased enumerations not currently supported.");
          }
        }
        fieldType = FieldType.logicalType(EnumerationType.create(enumValues));
        break;
      case MESSAGE:
      case GROUP:
        String fullName = protoFieldDescriptor.getMessageType().getFullName();
        switch (fullName) {
          case "google.protobuf.Timestamp":
            fieldType = FieldType.logicalType(new NanosInstant());
            break;
          case "google.protobuf.Int32Value":
          case "google.protobuf.UInt32Value":
          case "google.protobuf.Int64Value":
          case "google.protobuf.UInt64Value":
          case "google.protobuf.FloatValue":
          case "google.protobuf.DoubleValue":
          case "google.protobuf.StringValue":
          case "google.protobuf.BoolValue":
          case "google.protobuf.BytesValue":
            fieldType =
                beamFieldTypeFromSingularProtoField(
                    protoFieldDescriptor.getMessageType().findFieldByNumber(1));
            break;
          case "google.protobuf.Duration":
            fieldType = FieldType.logicalType(new NanosDuration());
            break;
          case "google.protobuf.Any":
            throw new UnsupportedOperationException("Any not yet supported");
          default:
            fieldType = FieldType.row(getSchema(protoFieldDescriptor.getMessageType()));
        }
        // all messages are nullable in Proto
        if (protoFieldDescriptor.isOptional()) {
          fieldType = fieldType.withNullable(true);
        }
        break;
      default:
        throw new RuntimeException("Field type not matched.");
    }
    return fieldType;
  }

  private static Schema.Options.Builder getFieldOptions(FieldDescriptor fieldDescriptor) {
    return getOptions(SCHEMA_OPTION_FIELD_PREFIX, fieldDescriptor.getOptions().getAllFields());
  }

  private static Schema.Options.Builder getSchemaOptions(Descriptors.Descriptor descriptor) {
    return getOptions(SCHEMA_OPTION_MESSAGE_PREFIX, descriptor.getOptions().getAllFields());
  }

  private static Schema.Options.Builder getOptions(
      String prefix, Map<FieldDescriptor, Object> allFields) {
    Schema.Options.Builder optionsBuilder = Schema.Options.builder();
    for (Map.Entry<FieldDescriptor, Object> entry : allFields.entrySet()) {
      FieldDescriptor fieldDescriptor = entry.getKey();
      FieldType fieldType = beamFieldTypeFromProtoField(fieldDescriptor);

      switch (fieldType.getTypeName()) {
        case BYTE:
        case BYTES:
        case INT16:
        case INT32:
        case INT64:
        case DECIMAL:
        case FLOAT:
        case DOUBLE:
        case STRING:
        case BOOLEAN:
        case LOGICAL_TYPE:
        case ROW:
        case ARRAY:
        case ITERABLE:
          Field field = Field.of("OPTION", fieldType);
          ProtoDynamicMessageSchema schema = ProtoDynamicMessageSchema.forSchema(Schema.of(field));
          @SuppressWarnings("rawtypes")
          ProtoDynamicMessageSchema.Convert convert = schema.createConverter(field);
          Object value = checkArgumentNotNull(convert.convertFromProtoValue(entry.getValue()));
          optionsBuilder.setOption(prefix + fieldDescriptor.getFullName(), fieldType, value);
          break;
        case MAP:
        case DATETIME:
        default:
          throw new IllegalStateException("These datatypes are not possible in extentions.");
      }
    }
    return optionsBuilder;
  }
}
