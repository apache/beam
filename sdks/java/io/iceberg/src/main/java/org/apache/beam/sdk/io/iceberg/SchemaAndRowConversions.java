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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

class SchemaAndRowConversions {

  private SchemaAndRowConversions() {}

  static final Map<Schema.FieldType, Type> BEAM_TYPES_TO_ICEBERG_TYPES =
      ImmutableMap.<Schema.FieldType, Type>builder()
          .put(Schema.FieldType.BOOLEAN, Types.BooleanType.get())
          .put(Schema.FieldType.INT32, Types.IntegerType.get())
          .put(Schema.FieldType.INT64, Types.LongType.get())
          .put(Schema.FieldType.FLOAT, Types.FloatType.get())
          .put(Schema.FieldType.DOUBLE, Types.DoubleType.get())
          .put(Schema.FieldType.STRING, Types.StringType.get())
          .put(Schema.FieldType.BYTES, Types.BinaryType.get())
          .build();

  public static Schema.FieldType icebergTypeToBeamFieldType(final Type type) {
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

  public static Schema.Field icebergFieldToBeamField(final Types.NestedField field) {
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

  public static Schema icebergStructTypeToBeamSchema(final Types.StructType struct) {
    Schema.Builder builder = Schema.builder();
    for (Types.NestedField f : struct.fields()) {
      builder.addField(icebergFieldToBeamField(f));
    }
    return builder.build();
  }

  public static Types.NestedField beamFieldToIcebergField(int fieldId, final Schema.Field field) {
    @Nullable Type icebergType = BEAM_TYPES_TO_ICEBERG_TYPES.get(field.getType());

    if (icebergType != null) {
      return Types.NestedField.of(
          fieldId, field.getType().getNullable(), field.getName(), icebergType);
    } else {
      return Types.NestedField.of(
          fieldId, field.getType().getNullable(), field.getName(), Types.StringType.get());
    }
  }

  public static org.apache.iceberg.Schema beamSchemaToIcebergSchema(final Schema schema) {
    Types.NestedField[] fields = new Types.NestedField[schema.getFieldCount()];
    int fieldId = 0;
    for (Schema.Field f : schema.getFields()) {
      fields[fieldId++] = beamFieldToIcebergField(fieldId, f);
    }
    return new org.apache.iceberg.Schema(fields);
  }

  public static Record rowToRecord(org.apache.iceberg.Schema schema, Row row) {
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
        throw new UnsupportedOperationException("List fields are not yet supported.");
      case MAP:
        throw new UnsupportedOperationException("Map fields are not yet supported.");
    }
  }

  public static Row recordToRow(Schema schema, Record record) {
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
        case DECIMAL:
          // Iceberg and Beam both use BigDecimal
          rowBuilder.addValue(record.getField(field.getName()));
          break;
        case FLOAT:
          // Iceberg and Beam both use float
          rowBuilder.addValue(record.getField(field.getName()));
          break;
        case DOUBLE:
          // Iceberg and Beam both use double
          rowBuilder.addValue(record.getField(field.getName()));
          break;
        case STRING:
          // Iceberg and Beam both use String
          rowBuilder.addValue(record.getField(field.getName()));
          break;
        case DATETIME:
          // Iceberg uses a long for millis; Beam uses joda time DateTime
          long millis = (long) record.getField(field.getName());
          rowBuilder.addValue(new DateTime(millis, DateTimeZone.UTC));
          break;
        case BOOLEAN:
          // Iceberg and Beam both use String
          rowBuilder.addValue(record.getField(field.getName()));
          break;
        case BYTES:
          // Iceberg uses ByteBuffer; Beam uses byte[]
          rowBuilder.addValue(((ByteBuffer) record.getField(field.getName())).array());
          break;
        case ARRAY:
          throw new UnsupportedOperationException("Array fields are not yet supported.");
        case ITERABLE:
          throw new UnsupportedOperationException("Iterable fields are not yet supported.");
        case MAP:
          throw new UnsupportedOperationException("Map fields are not yet supported.");
        case ROW:
          Record nestedRecord = (Record) record.getField(field.getName());
          Schema nestedSchema =
              checkArgumentNotNull(
                  field.getType().getRowSchema(),
                  "Corrupted schema: Row type did not have associated nested schema.");
          Row nestedRow = recordToRow(nestedSchema, nestedRecord);
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
