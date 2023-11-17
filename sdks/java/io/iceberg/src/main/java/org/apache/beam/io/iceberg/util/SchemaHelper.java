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
package org.apache.beam.io.iceberg.util;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

@SuppressWarnings({"dereference.of.nullable"})
public class SchemaHelper {

  private SchemaHelper() {}

  public static String ICEBERG_TYPE_OPTION_NAME = "icebergTypeID";

  public static Schema.FieldType fieldTypeForType(final Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return FieldType.BOOLEAN;
      case INTEGER:
        return FieldType.INT32;
      case LONG:
        return FieldType.INT64;
      case FLOAT:
        return FieldType.FLOAT;
      case DOUBLE:
        return FieldType.DOUBLE;
      case DATE:
      case TIME:
      case TIMESTAMP: // TODO: Logical types?
        return FieldType.DATETIME;
      case STRING:
        return FieldType.STRING;
      case UUID:
      case BINARY:
        return FieldType.BYTES;
      case FIXED:
      case DECIMAL:
        return FieldType.DECIMAL;
      case STRUCT:
        return FieldType.row(convert(type.asStructType()));
      case LIST:
        return FieldType.iterable(fieldTypeForType(type.asListType().elementType()));
      case MAP:
        return FieldType.map(
            fieldTypeForType(type.asMapType().keyType()),
            fieldTypeForType(type.asMapType().valueType()));
    }
    throw new RuntimeException("Unrecognized Iceberg Type");
  }

  public static Schema.Field convert(final Types.NestedField field) {
    return Schema.Field.of(field.name(), fieldTypeForType(field.type()))
        .withOptions(
            Schema.Options.builder()
                .setOption(
                    ICEBERG_TYPE_OPTION_NAME, Schema.FieldType.STRING, field.type().typeId().name())
                .build())
        .withNullable(field.isOptional());
  }

  public static Schema convert(final org.apache.iceberg.Schema schema) {
    Schema.Builder builder = Schema.builder();
    for (Types.NestedField f : schema.columns()) {
      builder.addField(convert(f));
    }
    return builder.build();
  }

  public static Schema convert(final Types.StructType struct) {
    Schema.Builder builder = Schema.builder();
    for (Types.NestedField f : struct.fields()) {
      builder.addField(convert(f));
    }
    return builder.build();
  }

  public static Types.NestedField convert(int fieldId, final Schema.Field field) {
    String typeId = field.getOptions().getValue(ICEBERG_TYPE_OPTION_NAME, String.class);
    if (typeId != null) {
      return Types.NestedField.of(
          fieldId,
          field.getType().getNullable(),
          field.getName(),
          Types.fromPrimitiveString(typeId));
    } else {
      return Types.NestedField.of(
          fieldId, field.getType().getNullable(), field.getName(), Types.StringType.get());
    }
  }

  public static org.apache.iceberg.Schema convert(final Schema schema) {
    Types.NestedField[] fields = new Types.NestedField[schema.getFieldCount()];
    int fieldId = 0;
    for (Schema.Field f : schema.getFields()) {
      fields[fieldId++] = convert(fieldId, f);
    }
    return new org.apache.iceberg.Schema(fields);
  }
}
