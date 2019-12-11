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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.DurationNanos;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.Fixed32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.Fixed64;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SFixed32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SFixed64;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SInt32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SInt64;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.TimestampNanos;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.UInt32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.UInt64;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

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
 * <p>Protobuf wrapper classes are translated to nullable types, as follows.
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
 */
@Experimental(Experimental.Kind.SCHEMAS)
public class ProtoSchemaTranslator {
  /** This METADATA tag is used to store the field number of a proto tag. */
  public static final String PROTO_NUMBER_METADATA_TAG = "PROTO_NUMBER";

  /** Attach a proto field number to a type. */
  public static FieldType withFieldNumber(FieldType fieldType, int index) {
    return fieldType.withMetadata(PROTO_NUMBER_METADATA_TAG, Long.toString(index));
  }

  /** Return the proto field number for a type. */
  public static int getFieldNumber(FieldType fieldType) {
    return Integer.parseInt(fieldType.getMetadataString(PROTO_NUMBER_METADATA_TAG));
  }

  /** Return a Beam scheam representing a proto class. */
  public static Schema getSchema(Class<? extends Message> clazz) {
    return getSchema(ProtobufUtil.getDescriptorForClass(clazz));
  }

  private static Schema getSchema(Descriptors.Descriptor descriptor) {
    Set<Integer> oneOfFields = Sets.newHashSet();
    List<Field> fields = Lists.newArrayListWithCapacity(descriptor.getFields().size());
    for (OneofDescriptor oneofDescriptor : descriptor.getOneofs()) {
      List<Field> subFields = Lists.newArrayListWithCapacity(oneofDescriptor.getFieldCount());
      Map<String, Integer> enumIds = Maps.newHashMap();
      for (FieldDescriptor fieldDescriptor : oneofDescriptor.getFields()) {
        oneOfFields.add(fieldDescriptor.getNumber());
        // Store proto field number in metadata.
        FieldType fieldType =
            withFieldNumber(
                beamFieldTypeFromProtoField(fieldDescriptor), fieldDescriptor.getNumber());
        subFields.add(Field.nullable(fieldDescriptor.getName(), fieldType));
        checkArgument(
            enumIds.putIfAbsent(fieldDescriptor.getName(), fieldDescriptor.getNumber()) == null);
      }
      FieldType oneOfType = FieldType.logicalType(OneOfType.create(subFields, enumIds));
      fields.add(Field.of(oneofDescriptor.getName(), oneOfType));
    }

    for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
      if (!oneOfFields.contains(fieldDescriptor.getNumber())) {
        // Store proto field number in metadata.
        FieldType fieldType =
            withFieldNumber(
                beamFieldTypeFromProtoField(fieldDescriptor), fieldDescriptor.getNumber());
        fields.add(Field.of(fieldDescriptor.getName(), fieldType));
      }
    }
    return Schema.builder().addFields(fields).build();
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
              beamFieldTypeFromProtoField(keyFieldDescriptor),
              beamFieldTypeFromProtoField(valueFieldDescriptor));
    } else if (protoFieldDescriptor.isRepeated()) {
      fieldType = FieldType.array(beamFieldTypeFromSingularProtoField(protoFieldDescriptor));
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
            fieldType = FieldType.logicalType(new TimestampNanos());
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
                        protoFieldDescriptor.getMessageType().findFieldByNumber(1))
                    .withNullable(true);
            break;
          case "google.protobuf.Duration":
            fieldType = FieldType.logicalType(new DurationNanos());
            break;
          case "google.protobuf.Any":
            throw new RuntimeException("Any not yet supported");
          default:
            fieldType = FieldType.row(getSchema(protoFieldDescriptor.getMessageType()));
        }
        break;
      default:
        throw new RuntimeException("Field type not matched.");
    }
    if (protoFieldDescriptor.isOptional()) {
      fieldType = fieldType.withNullable(true);
    }
    return fieldType;
  }
}
