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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/** Utilities for converting between Beam and Iceberg types. */
public class IcebergUtils {
  // This is made public for users convenience, as many may have more experience working with
  // Iceberg types.

  private IcebergUtils() {}

  private static final Map<Schema.TypeName, Type> BEAM_TYPES_TO_ICEBERG_TYPES =
      ImmutableMap.<Schema.TypeName, Type>builder()
          .put(Schema.TypeName.BOOLEAN, Types.BooleanType.get())
          .put(Schema.TypeName.INT32, Types.IntegerType.get())
          .put(Schema.TypeName.INT64, Types.LongType.get())
          .put(Schema.TypeName.FLOAT, Types.FloatType.get())
          .put(Schema.TypeName.DOUBLE, Types.DoubleType.get())
          .put(Schema.TypeName.STRING, Types.StringType.get())
          .put(Schema.TypeName.BYTES, Types.BinaryType.get())
          .build();

  private static Schema.FieldType icebergTypeToBeamFieldType(final Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return Schema.FieldType.BOOLEAN;
      case INTEGER:
        return Schema.FieldType.INT32;
      case LONG:
        return Schema.FieldType.INT64;
      case FLOAT:
        return Schema.FieldType.FLOAT;
      case DOUBLE:
        return Schema.FieldType.DOUBLE;
      case DATE:
      case TIME:
      case TIMESTAMP: // TODO: Logical types?
        return Schema.FieldType.DATETIME;
      case STRING:
        return Schema.FieldType.STRING;
      case UUID:
      case BINARY:
        return Schema.FieldType.BYTES;
      case FIXED:
      case DECIMAL:
        return Schema.FieldType.DECIMAL;
      case STRUCT:
        return Schema.FieldType.row(icebergStructTypeToBeamSchema(type.asStructType()));
      case LIST:
        return Schema.FieldType.iterable(
            icebergTypeToBeamFieldType(type.asListType().elementType()));
      case MAP:
        return Schema.FieldType.map(
            icebergTypeToBeamFieldType(type.asMapType().keyType()),
            icebergTypeToBeamFieldType(type.asMapType().valueType()));
    }
    throw new RuntimeException("Unrecognized IcebergIO Type");
  }

  private static Schema.Field icebergFieldToBeamField(final Types.NestedField field) {
    return Schema.Field.of(field.name(), icebergTypeToBeamFieldType(field.type()))
        .withNullable(field.isOptional());
  }

  /** Converts an Iceberg {@link org.apache.iceberg.Schema} to a Beam {@link Schema}. */
  public static Schema icebergSchemaToBeamSchema(final org.apache.iceberg.Schema schema) {
    Schema.Builder builder = Schema.builder();
    for (Types.NestedField f : schema.columns()) {
      builder.addField(icebergFieldToBeamField(f));
    }
    return builder.build();
  }

  private static Schema icebergStructTypeToBeamSchema(final Types.StructType struct) {
    Schema.Builder builder = Schema.builder();
    for (Types.NestedField f : struct.fields()) {
      builder.addField(icebergFieldToBeamField(f));
    }
    return builder.build();
  }

  /**
   * Represents a {@link Type} and the most recent field ID used to build it.
   *
   * <p>Iceberg Schema fields are required to have unique IDs. This includes unique IDs for a {@link
   * org.apache.iceberg.types.Type.NestedType}'s components (e.g. {@link Types.ListType}'s
   * collection type, {@link Types.MapType}'s key type and value type, and {@link
   * Types.StructType}'s nested fields). The {@code maxId} in this object represents the most recent
   * ID used after building this type. This helps signal that the next {@link
   * org.apache.iceberg.types.Type.NestedType} we construct should have an ID greater than this one.
   */
  @VisibleForTesting
  static class TypeAndMaxId {
    int maxId;
    Type type;

    TypeAndMaxId(int id, Type object) {
      this.maxId = id;
      this.type = object;
    }
  }

  /**
   * Takes a Beam {@link Schema.FieldType} and an index intended as a starting point for Iceberg
   * {@link org.apache.iceberg.types.Type.NestedType}s. Returns an Iceberg {@link Type} and the
   * maximum index after building that type.
   *
   * <p>Returns this information in an {@link TypeAndMaxId} object.
   */
  @VisibleForTesting
  static TypeAndMaxId beamFieldTypeToIcebergFieldType(
      Schema.FieldType beamType, int nestedFieldId) {
    if (BEAM_TYPES_TO_ICEBERG_TYPES.containsKey(beamType.getTypeName())) {
      // we don't use nested field ID for primitive types. decrement it so the caller can use it for
      // other types.
      return new TypeAndMaxId(
          --nestedFieldId, BEAM_TYPES_TO_ICEBERG_TYPES.get(beamType.getTypeName()));
    } else if (beamType.getTypeName().isCollectionType()) { // ARRAY or ITERABLE
      Schema.FieldType beamCollectionType =
          Preconditions.checkArgumentNotNull(beamType.getCollectionElementType());

      // nestedFieldId is reserved for the list's collection type.
      // we increment here because further nested fields should use unique ID's
      TypeAndMaxId listInfo =
          beamFieldTypeToIcebergFieldType(beamCollectionType, nestedFieldId + 1);
      Type icebergCollectionType = listInfo.type;

      boolean elementTypeIsNullable =
          Preconditions.checkArgumentNotNull(beamType.getCollectionElementType()).getNullable();

      Type listType =
          elementTypeIsNullable
              ? Types.ListType.ofOptional(nestedFieldId, icebergCollectionType)
              : Types.ListType.ofRequired(nestedFieldId, icebergCollectionType);

      return new TypeAndMaxId(listInfo.maxId, listType);
    } else if (beamType.getTypeName().isMapType()) { // MAP
      // key and value IDs need to be unique
      int keyId = nestedFieldId;
      int valueId = keyId + 1;

      // nested field IDs should be unique
      nestedFieldId = valueId + 1;
      Schema.FieldType beamKeyType = Preconditions.checkArgumentNotNull(beamType.getMapKeyType());
      TypeAndMaxId keyInfo = beamFieldTypeToIcebergFieldType(beamKeyType, nestedFieldId);
      Type icebergKeyType = keyInfo.type;

      nestedFieldId = keyInfo.maxId + 1;
      Schema.FieldType beamValueType =
          Preconditions.checkArgumentNotNull(beamType.getMapValueType());
      TypeAndMaxId valueInfo = beamFieldTypeToIcebergFieldType(beamValueType, nestedFieldId);
      Type icebergValueType = valueInfo.type;

      Type mapType =
          beamValueType.getNullable()
              ? Types.MapType.ofOptional(keyId, valueId, icebergKeyType, icebergValueType)
              : Types.MapType.ofRequired(keyId, valueId, icebergKeyType, icebergValueType);

      return new TypeAndMaxId(valueInfo.maxId, mapType);
    } else if (beamType.getTypeName().isCompositeType()) { // ROW
      // Nested field IDs need to be unique from the field that contains this StructType
      Schema nestedSchema = Preconditions.checkArgumentNotNull(beamType.getRowSchema());
      List<Types.NestedField> nestedFields = new ArrayList<>(nestedSchema.getFieldCount());

      int icebergFieldId = nestedFieldId;
      nestedFieldId = icebergFieldId + nestedSchema.getFieldCount();
      for (Schema.Field beamField : nestedSchema.getFields()) {
        TypeAndMaxId typeAndMaxId =
            beamFieldTypeToIcebergFieldType(beamField.getType(), nestedFieldId);
        Types.NestedField icebergField =
            Types.NestedField.of(
                icebergFieldId++,
                beamField.getType().getNullable(),
                beamField.getName(),
                typeAndMaxId.type);

        nestedFields.add(icebergField);
        nestedFieldId = typeAndMaxId.maxId + 1;
      }

      Type structType = Types.StructType.of(nestedFields);

      return new TypeAndMaxId(nestedFieldId - 1, structType);
    }

    return new TypeAndMaxId(nestedFieldId, Types.StringType.get());
  }

  /**
   * Converts a Beam {@link Schema} to an Iceberg {@link org.apache.iceberg.Schema}.
   *
   * <p>The following unsupported Beam types will be defaulted to {@link Types.StringType}:
   * <li>{@link Schema.TypeName.DECIMAL}
   * <li>{@link Schema.TypeName.DATETIME}
   * <li>{@link Schema.TypeName.LOGICAL_TYPE}
   */
  public static org.apache.iceberg.Schema beamSchemaToIcebergSchema(final Schema schema) {
    List<Types.NestedField> fields = new ArrayList<>(schema.getFieldCount());
    int nestedFieldId = schema.getFieldCount() + 1;
    int icebergFieldId = 1;
    for (Schema.Field beamField : schema.getFields()) {
      TypeAndMaxId typeAndMaxId =
          beamFieldTypeToIcebergFieldType(beamField.getType(), nestedFieldId);
      Types.NestedField icebergField =
          Types.NestedField.of(
              icebergFieldId++,
              beamField.getType().getNullable(),
              beamField.getName(),
              typeAndMaxId.type);

      fields.add(icebergField);
      nestedFieldId = typeAndMaxId.maxId + 1;
    }
    return new org.apache.iceberg.Schema(fields.toArray(new Types.NestedField[fields.size()]));
  }

  /** Converts a Beam {@link Row} to an Iceberg {@link Record}. */
  public static Record beamRowToIcebergRecord(org.apache.iceberg.Schema schema, Row row) {
    return copyRowIntoRecord(GenericRecord.create(schema), row);
  }

  private static Record copyRowIntoRecord(Record baseRecord, Row value) {
    Record rec = baseRecord.copy();
    for (Types.NestedField f : rec.struct().fields()) {
      copyFieldIntoRecord(rec, f, value);
    }
    return rec;
  }

  private static void copyFieldIntoRecord(Record rec, Types.NestedField field, Row value) {
    String name = field.name();
    switch (field.type().typeId()) {
      case BOOLEAN:
        Optional.ofNullable(value.getBoolean(name)).ifPresent(v -> rec.setField(name, v));
        break;
      case INTEGER:
        Optional.ofNullable(value.getInt32(name)).ifPresent(v -> rec.setField(name, v));
        break;
      case LONG:
        Optional.ofNullable(value.getInt64(name)).ifPresent(v -> rec.setField(name, v));
        break;
      case FLOAT:
        Optional.ofNullable(value.getFloat(name)).ifPresent(v -> rec.setField(name, v));
        break;
      case DOUBLE:
        Optional.ofNullable(value.getDouble(name)).ifPresent(v -> rec.setField(name, v));
        break;
      case DATE:
        throw new UnsupportedOperationException("Date fields not yet supported");
      case TIME:
        throw new UnsupportedOperationException("Time fields not yet supported");
      case TIMESTAMP:
        Optional.ofNullable(value.getDateTime(name))
            .ifPresent(v -> rec.setField(name, v.getMillis()));
        break;
      case STRING:
        Optional.ofNullable(value.getString(name)).ifPresent(v -> rec.setField(name, v));
        break;
      case UUID:
        Optional.ofNullable(value.getBytes(name))
            .ifPresent(v -> rec.setField(name, UUID.nameUUIDFromBytes(v)));
        break;
      case FIXED:
        throw new UnsupportedOperationException("Fixed-precision fields are not yet supported.");
      case BINARY:
        Optional.ofNullable(value.getBytes(name))
            .ifPresent(v -> rec.setField(name, ByteBuffer.wrap(v)));
        break;
      case DECIMAL:
        Optional.ofNullable(value.getDecimal(name)).ifPresent(v -> rec.setField(name, v));
        break;
      case STRUCT:
        Optional.ofNullable(value.getRow(name))
            .ifPresent(
                row ->
                    rec.setField(
                        name,
                        copyRowIntoRecord(GenericRecord.create(field.type().asStructType()), row)));
        break;
      case LIST:
        Optional.ofNullable(value.getArray(name)).ifPresent(list -> rec.setField(name, list));
        break;
      case MAP:
        Optional.ofNullable(value.getMap(name)).ifPresent(v -> rec.setField(name, v));
        break;
    }
  }

  /** Converts an Iceberg {@link Record} to a Beam {@link Row}. */
  public static Row icebergRecordToBeamRow(Schema schema, Record record) {
    Row.Builder rowBuilder = Row.withSchema(schema);
    for (Schema.Field field : schema.getFields()) {
      boolean isNullable = field.getType().getNullable();
      @Nullable Object icebergValue = record.getField(field.getName());
      if (icebergValue == null) {
        if (isNullable) {
          rowBuilder.addValue(null);
          continue;
        }
        throw new RuntimeException(
            String.format("Received null value for required field '%s'.", field.getName()));
      }
      switch (field.getType().getTypeName()) {
        case BYTE:
        case INT16:
        case INT32:
        case INT64:
        case DECIMAL: // Iceberg and Beam both use BigDecimal
        case FLOAT: // Iceberg and Beam both use float
        case DOUBLE: // Iceberg and Beam both use double
        case STRING: // Iceberg and Beam both use String
        case BOOLEAN: // Iceberg and Beam both use String
        case ARRAY:
        case ITERABLE:
        case MAP:
          rowBuilder.addValue(icebergValue);
          break;
        case DATETIME:
          // Iceberg uses a long for millis; Beam uses joda time DateTime
          long millis = (long) icebergValue;
          rowBuilder.addValue(new DateTime(millis, DateTimeZone.UTC));
          break;
        case BYTES:
          // Iceberg uses ByteBuffer; Beam uses byte[]
          rowBuilder.addValue(((ByteBuffer) icebergValue).array());
          break;
        case ROW:
          Record nestedRecord = (Record) icebergValue;
          Schema nestedSchema =
              checkArgumentNotNull(
                  field.getType().getRowSchema(),
                  "Corrupted schema: Row type did not have associated nested schema.");
          rowBuilder.addValue(icebergRecordToBeamRow(nestedSchema, nestedRecord));
          break;
        case LOGICAL_TYPE:
          throw new UnsupportedOperationException(
              "Cannot convert iceberg field to Beam logical type");
        default:
          throw new UnsupportedOperationException(
              "Unsupported Beam type: " + field.getType().getTypeName());
      }
    }
    return rowBuilder.build();
  }
}
