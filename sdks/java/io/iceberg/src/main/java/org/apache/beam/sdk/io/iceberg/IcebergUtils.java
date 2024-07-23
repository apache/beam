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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class IcebergUtils {

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
   * Represents an Object (in practice, either {@link Type} or {@link Types.NestedField}) along with
   * the most recent (max) ID that has been used to build this object.
   *
   * <p>Iceberg Schema fields are required to have unique IDs. This includes unique IDs for a {@link
   * Types.ListType}'s collection type, a {@link Types.MapType}'s key type and value type, and
   * nested {@link Types.StructType}s. When constructing any of these types, we use multiple unique
   * ID's for the type's components. The {@code maxId} in this object represents the most recent ID
   * used after building this type. This helps signal that the next field we construct should have
   * an ID greater than this one.
   */
  private static class ObjectAndMaxId<T> {
    int maxId;
    T object;

    ObjectAndMaxId(int id, T object) {
      this.maxId = id;
      this.object = object;
    }
  }

  private static ObjectAndMaxId<Type> beamFieldTypeToIcebergFieldType(
      int fieldId, Schema.FieldType beamType) {
    if (BEAM_TYPES_TO_ICEBERG_TYPES.containsKey(beamType.getTypeName())) {
      return new ObjectAndMaxId<>(fieldId, BEAM_TYPES_TO_ICEBERG_TYPES.get(beamType.getTypeName()));
    } else if (beamType.getTypeName().isCollectionType()) { // ARRAY or ITERABLE
      // List ID needs to be unique from the NestedField that contains this ListType
      int listId = fieldId + 1;
      Schema.FieldType beamCollectionType =
          Preconditions.checkArgumentNotNull(beamType.getCollectionElementType());
      Type icebergCollectionType =
          beamFieldTypeToIcebergFieldType(listId, beamCollectionType).object;

      boolean elementTypeIsNullable =
          Preconditions.checkArgumentNotNull(beamType.getCollectionElementType()).getNullable();

      Type listType =
          elementTypeIsNullable
              ? Types.ListType.ofOptional(listId, icebergCollectionType)
              : Types.ListType.ofRequired(listId, icebergCollectionType);

      return new ObjectAndMaxId<>(listId, listType);
    } else if (beamType.getTypeName().isMapType()) { // MAP
      // key and value IDs need to be unique from the NestedField that contains this MapType
      int keyId = fieldId + 1;
      int valueId = fieldId + 2;

      Schema.FieldType beamKeyType = Preconditions.checkArgumentNotNull(beamType.getMapKeyType());
      Schema.FieldType beamValueType =
          Preconditions.checkArgumentNotNull(beamType.getMapValueType());

      Type icebergKeyType = beamFieldTypeToIcebergFieldType(keyId, beamKeyType).object;
      Type icebergValueType = beamFieldTypeToIcebergFieldType(valueId, beamValueType).object;

      Type mapType =
          beamValueType.getNullable()
              ? Types.MapType.ofOptional(keyId, valueId, icebergKeyType, icebergValueType)
              : Types.MapType.ofRequired(keyId, valueId, icebergKeyType, icebergValueType);

      return new ObjectAndMaxId<>(valueId, mapType);
    } else if (beamType.getTypeName().isCompositeType()) { // ROW
      // Nested field IDs need to be unique from the field that contains this StructType
      int nestedFieldId = fieldId;

      Schema nestedSchema = Preconditions.checkArgumentNotNull(beamType.getRowSchema());
      List<Types.NestedField> nestedFields = new ArrayList<>(nestedSchema.getFieldCount());
      for (Schema.Field field : nestedSchema.getFields()) {
        Types.NestedField nestedField = beamFieldToIcebergField(++nestedFieldId, field).object;
        nestedFields.add(nestedField);
      }

      Type structType = Types.StructType.of(nestedFields);

      return new ObjectAndMaxId<>(nestedFieldId, structType);
    }

    return new ObjectAndMaxId<>(fieldId, Types.StringType.get());
  }

  private static ObjectAndMaxId<Types.NestedField> beamFieldToIcebergField(
      int fieldId, final Schema.Field field) {
    ObjectAndMaxId<Type> typeAndMaxId = beamFieldTypeToIcebergFieldType(fieldId, field.getType());
    Type icebergType = typeAndMaxId.object;
    int id = typeAndMaxId.maxId;

    Types.NestedField icebergField =
        Types.NestedField.of(fieldId, field.getType().getNullable(), field.getName(), icebergType);

    return new ObjectAndMaxId<>(id, icebergField);
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
    Types.NestedField[] fields = new Types.NestedField[schema.getFieldCount()];
    int nextId = 1;
    for (int i = 0; i < schema.getFieldCount(); i++) {
      Schema.Field beamField = schema.getField(i);
      ObjectAndMaxId<Types.NestedField> fieldAndMaxId = beamFieldToIcebergField(nextId, beamField);
      Types.NestedField field = fieldAndMaxId.object;
      fields[i] = field;

      nextId = fieldAndMaxId.maxId + 1;
    }
    return new org.apache.iceberg.Schema(fields);
  }

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

  public static Row icebergRecordToBeamRow(Schema schema, Record record) {
    Row.Builder rowBuilder = Row.withSchema(schema);
    for (Schema.Field field : schema.getFields()) {
      switch (field.getType().getTypeName()) {
        case BYTE:
          // I guess allow anything we can cast here
          byte byteValue = (byte) record.getField(field.getName());
          rowBuilder.addValue(byteValue);
          break;
        case INT16:
          // I guess allow anything we can cast here
          short shortValue = (short) record.getField(field.getName());
          rowBuilder.addValue(shortValue);
          break;
        case INT32:
          // I guess allow anything we can cast here
          int intValue = (int) record.getField(field.getName());
          rowBuilder.addValue(intValue);
          break;
        case INT64:
          // I guess allow anything we can cast here
          long longValue = (long) record.getField(field.getName());
          rowBuilder.addValue(longValue);
          break;
        case DECIMAL: // Iceberg and Beam both use BigDecimal
        case FLOAT: // Iceberg and Beam both use float
        case DOUBLE: // Iceberg and Beam both use double
        case STRING: // Iceberg and Beam both use String
        case BOOLEAN: // Iceberg and Beam both use String
        case ARRAY:
        case ITERABLE:
        case MAP:
          rowBuilder.addValue(record.getField(field.getName()));
          break;
        case DATETIME:
          // Iceberg uses a long for millis; Beam uses joda time DateTime
          long millis = (long) record.getField(field.getName());
          rowBuilder.addValue(new DateTime(millis, DateTimeZone.UTC));
          break;
        case BYTES:
          // Iceberg uses ByteBuffer; Beam uses byte[]
          rowBuilder.addValue(((ByteBuffer) record.getField(field.getName())).array());
          break;
        case ROW:
          Record nestedRecord = (Record) record.getField(field.getName());
          Schema nestedSchema =
              checkArgumentNotNull(
                  field.getType().getRowSchema(),
                  "Corrupted schema: Row type did not have associated nested schema.");
          Row nestedRow = icebergRecordToBeamRow(nestedSchema, nestedRecord);
          rowBuilder.addValue(nestedRow);
          break;
        case LOGICAL_TYPE:
          throw new UnsupportedOperationException(
              "Cannot convert iceberg field to Beam logical type");
      }
    }
    return rowBuilder.build();
  }
}
