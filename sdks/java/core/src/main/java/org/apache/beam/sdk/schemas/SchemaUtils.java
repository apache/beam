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
package org.apache.beam.sdk.schemas;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.values.Row;

/** A set of utility functions for schemas. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SchemaUtils {
  private static final String INDENT = "  ";

  /**
   * Given two schema that have matching types, return a nullable-widened schema.
   *
   * <p>The schemas must have matching types, except for field names which can differ. The returned
   * schema will contain the field names in the first schema. All field types will be nullable if
   * the corresponding field type is nullable in either of the input schemas.
   */
  public static Schema mergeWideningNullable(Schema schema1, Schema schema2) {
    if (schema1.getFieldCount() != schema2.getFieldCount()) {
      throw new IllegalArgumentException(
          "Cannot merge schemas with different numbers of fields. "
              + "schema1: "
              + schema1
              + " schema2: "
              + schema2);
    }
    Schema.Builder builder = Schema.builder();
    for (int i = 0; i < schema1.getFieldCount(); ++i) {
      String name = schema1.getField(i).getName();
      builder.addField(
          name, widenNullableTypes(schema1.getField(i).getType(), schema2.getField(i).getType()));
    }
    return builder.build();
  }

  static FieldType widenNullableTypes(FieldType fieldType1, FieldType fieldType2) {
    if (fieldType1.getTypeName() != fieldType2.getTypeName()) {
      throw new IllegalArgumentException(
          "Cannot merge two types: "
              + fieldType1.getTypeName()
              + " and "
              + fieldType2.getTypeName());
    }

    FieldType result;
    switch (fieldType1.getTypeName()) {
      case ROW:
        result =
            FieldType.row(
                mergeWideningNullable(fieldType1.getRowSchema(), fieldType2.getRowSchema()));
        break;
      case ARRAY:
        FieldType arrayElementType =
            widenNullableTypes(
                fieldType1.getCollectionElementType(), fieldType2.getCollectionElementType());
        result = FieldType.array(arrayElementType);
        break;
      case ITERABLE:
        FieldType iterableElementType =
            widenNullableTypes(
                fieldType1.getCollectionElementType(), fieldType2.getCollectionElementType());
        result = FieldType.iterable(iterableElementType);
        break;
      case MAP:
        FieldType keyType =
            widenNullableTypes(fieldType1.getMapKeyType(), fieldType2.getMapKeyType());
        FieldType valueType =
            widenNullableTypes(fieldType1.getMapValueType(), fieldType2.getMapValueType());
        result = FieldType.map(keyType, valueType);
        break;
      case LOGICAL_TYPE:
        if (!fieldType1
            .getLogicalType()
            .getIdentifier()
            .equals(fieldType2.getLogicalType().getIdentifier())) {
          throw new IllegalArgumentException(
              "Logical types don't match and cannot be merged: "
                  + fieldType1.getLogicalType().getIdentifier()
                  + ".v.s"
                  + fieldType2.getLogicalType().getIdentifier());
        }
        // fall through
      default:
        result = fieldType1;
    }
    return result.withNullable(fieldType1.getNullable() || fieldType2.getNullable());
  }

  /**
   * Returns the base type given a logical type and the input type.
   *
   * <p>This function can be used to handle logical types without knowing InputT or BaseT.
   */
  public static <InputT, BaseT> BaseT toLogicalBaseType(
      LogicalType<InputT, BaseT> logicalType, InputT inputType) {
    return logicalType.toBaseType(inputType);
  }

  /**
   * Returns the input type given a logical type and the base type.
   *
   * <p>This function can be used to handle logical types without knowing InputT or BaseT.
   */
  public static <BaseT, InputT> InputT toLogicalInputType(
      LogicalType<InputT, BaseT> logicalType, BaseT baseType) {
    return logicalType.toInputType(baseType);
  }

  public static String toPrettyString(Row row) {
    return toPrettyRowString(row, "");
  }

  public static String toPrettyString(Schema schema) {
    return toPrettySchemaString(schema, "");
  }

  static String toFieldTypeNameString(FieldType fieldType) {
    return fieldType.getTypeName()
        + (Boolean.TRUE.equals(fieldType.getNullable()) ? "" : " NOT NULL");
  }

  static String toPrettyFieldTypeString(Schema.FieldType fieldType, String prefix) {
    String nextPrefix = prefix + INDENT;
    switch (fieldType.getTypeName()) {
      case BYTE:
      case INT16:
      case INT32:
      case INT64:
      case DECIMAL:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case DATETIME:
      case BOOLEAN:
      case BYTES:
        return "<" + toFieldTypeNameString(fieldType) + ">";
      case ARRAY:
      case ITERABLE:
        {
          StringBuilder sb = new StringBuilder();
          sb.append("<").append(toFieldTypeNameString(fieldType)).append("> {\n");
          sb.append(nextPrefix)
              .append("<element>: ")
              .append(
                  toPrettyFieldTypeString(
                      Objects.requireNonNull(fieldType.getCollectionElementType()), nextPrefix))
              .append("\n");
          sb.append(prefix).append("}");
          return sb.toString();
        }
      case MAP:
        {
          StringBuilder sb = new StringBuilder();
          sb.append("<").append(toFieldTypeNameString(fieldType)).append("> {\n");
          sb.append(nextPrefix)
              .append("<key>: ")
              .append(
                  toPrettyFieldTypeString(
                      Objects.requireNonNull(fieldType.getMapKeyType()), nextPrefix))
              .append(",\n");
          sb.append(nextPrefix)
              .append("<value>: ")
              .append(
                  toPrettyFieldTypeString(
                      Objects.requireNonNull(fieldType.getMapValueType()), nextPrefix))
              .append("\n");
          sb.append(prefix).append("}");
          return sb.toString();
        }
      case ROW:
        {
          return "<"
              + toFieldTypeNameString(fieldType)
              + "> "
              + toPrettySchemaString(Objects.requireNonNull(fieldType.getRowSchema()), prefix);
        }
      case LOGICAL_TYPE:
        {
          Schema.FieldType baseType =
              Objects.requireNonNull(fieldType.getLogicalType()).getBaseType();
          StringBuilder sb = new StringBuilder();
          sb.append("<")
              .append(toFieldTypeNameString(fieldType))
              .append("(")
              .append(fieldType.getLogicalType().getIdentifier())
              .append(")> {\n");
          sb.append(nextPrefix)
              .append("<base>: ")
              .append(toPrettyFieldTypeString(baseType, nextPrefix))
              .append("\n");
          sb.append(prefix).append("}");
          return sb.toString();
        }
      default:
        throw new UnsupportedOperationException(fieldType.getTypeName() + " is not supported");
    }
  }

  static String toPrettyOptionsString(Schema.Options options, String prefix) {
    String nextPrefix = prefix + INDENT;
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    for (String optionName : options.getOptionNames()) {
      sb.append(nextPrefix)
          .append(optionName)
          .append(" = ")
          .append(
              toPrettyFieldValueString(
                  options.getType(optionName), options.getValue(optionName), nextPrefix))
          .append("\n");
    }
    sb.append(prefix).append("}");
    return sb.toString();
  }

  static String toPrettyFieldValueString(Schema.FieldType fieldType, Object value, String prefix) {
    String nextPrefix = prefix + INDENT;
    switch (fieldType.getTypeName()) {
      case BYTE:
      case INT16:
      case INT32:
      case INT64:
      case DECIMAL:
      case FLOAT:
      case DOUBLE:
      case DATETIME:
      case BOOLEAN:
        return Objects.toString(value);
      case STRING:
        {
          String string = (String) value;
          return "\"" + string.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        }
      case BYTES:
        {
          byte[] bytes = (byte[]) value;
          return Arrays.toString(bytes);
        }
      case ARRAY:
      case ITERABLE:
        {
          if (!(value instanceof List)) {
            throw new IllegalArgumentException(
                String.format(
                    "value type is '%s' for field type '%s'",
                    value.getClass(), fieldType.getTypeName()));
          }
          FieldType elementType = Objects.requireNonNull(fieldType.getCollectionElementType());

          @SuppressWarnings("unchecked")
          List<Object> list = (List<Object>) value;
          if (list.isEmpty()) {
            return "[]";
          }
          StringBuilder sb = new StringBuilder();
          sb.append("[\n");
          int size = list.size();
          int index = 0;
          for (Object element : list) {
            sb.append(nextPrefix)
                .append(toPrettyFieldValueString(elementType, element, nextPrefix));
            if (index++ < size - 1) {
              sb.append(",\n");
            } else {
              sb.append("\n");
            }
          }
          sb.append(prefix).append("]");
          return sb.toString();
        }
      case MAP:
        {
          if (!(value instanceof Map)) {
            throw new IllegalArgumentException(
                String.format(
                    "value type is '%s' for field type '%s'",
                    value.getClass(), fieldType.getTypeName()));
          }

          FieldType keyType = Objects.requireNonNull(fieldType.getMapKeyType());
          FieldType valueType = Objects.requireNonNull(fieldType.getMapValueType());

          @SuppressWarnings("unchecked")
          Map<Object, Object> map = (Map<Object, Object>) value;
          if (map.isEmpty()) {
            return "{}";
          }

          StringBuilder sb = new StringBuilder();
          sb.append("{\n");
          int size = map.size();
          int index = 0;
          for (Map.Entry<Object, Object> entry : map.entrySet()) {
            sb.append(nextPrefix)
                .append(toPrettyFieldValueString(keyType, entry.getKey(), nextPrefix))
                .append(": ")
                .append(toPrettyFieldValueString(valueType, entry.getValue(), nextPrefix));
            if (index++ < size - 1) {
              sb.append(",\n");
            } else {
              sb.append("\n");
            }
          }
          sb.append(prefix).append("}");
          return sb.toString();
        }
      case ROW:
        {
          return toPrettyRowString((Row) value, prefix);
        }
      case LOGICAL_TYPE:
        {
          @SuppressWarnings("unchecked")
          Schema.LogicalType<Object, Object> logicalType =
              (Schema.LogicalType<Object, Object>)
                  Objects.requireNonNull(fieldType.getLogicalType());
          Schema.FieldType baseType = logicalType.getBaseType();
          Object baseValue = logicalType.toBaseType(value);
          return toPrettyFieldValueString(baseType, baseValue, prefix);
        }
      default:
        throw new UnsupportedOperationException(fieldType.getTypeName() + " is not supported");
    }
  }

  static String toPrettySchemaString(Schema schema, String prefix) {
    String nextPrefix = prefix + INDENT;
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    for (Schema.Field field : schema.getFields()) {
      sb.append(nextPrefix)
          .append(field.getName())
          .append(": ")
          .append(toPrettyFieldTypeString(field.getType(), nextPrefix));
      if (field.getOptions().hasOptions()) {
        sb.append(", fieldOptions = ")
            .append(toPrettyOptionsString(field.getOptions(), nextPrefix));
      }
      sb.append("\n");
    }
    sb.append(prefix).append("}");
    if (schema.getOptions().hasOptions()) {
      sb.append(", schemaOptions = ").append(toPrettyOptionsString(schema.getOptions(), prefix));
    }
    if (schema.getUUID() != null) {
      sb.append(", schemaUUID = ").append(schema.getUUID());
    }
    return sb.toString();
  }

  static String toPrettyRowString(Row row, String prefix) {
    long nonNullFieldCount = row.getValues().stream().filter(Objects::nonNull).count();
    if (nonNullFieldCount == 0) {
      return "{}";
    }

    String nextPrefix = prefix + INDENT;
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    long nonNullFieldIndex = 0;
    for (Schema.Field field : row.getSchema().getFields()) {
      String fieldName = field.getName();
      Object fieldValue = row.getValue(fieldName);
      if (fieldValue == null) {
        continue;
      }
      sb.append(nextPrefix)
          .append(fieldName)
          .append(": ")
          .append(toPrettyFieldValueString(field.getType(), fieldValue, nextPrefix));
      if (nonNullFieldIndex++ < nonNullFieldCount - 1) {
        sb.append(",\n");
      } else {
        sb.append("\n");
      }
    }
    sb.append(prefix).append("}");
    return sb.toString();
  }
}
