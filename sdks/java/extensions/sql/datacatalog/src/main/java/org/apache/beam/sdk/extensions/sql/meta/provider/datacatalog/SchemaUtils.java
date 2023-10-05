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
package org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog;

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import com.google.cloud.datacatalog.v1beta1.ColumnSchema;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class SchemaUtils {

  private static final Map<String, FieldType> FIELD_TYPES =
      ImmutableMap.<String, FieldType>builder()
          .put("BOOL", FieldType.BOOLEAN)
          .put("BYTES", FieldType.BYTES)
          .put("DATE", FieldType.logicalType(SqlTypes.DATE))
          .put("DATETIME", FieldType.logicalType(SqlTypes.DATETIME))
          .put("DOUBLE", FieldType.DOUBLE)
          .put("FLOAT", FieldType.DOUBLE)
          .put("FLOAT64", FieldType.DOUBLE)
          .put("INT32", FieldType.INT32)
          .put("INT64", FieldType.INT64)
          .put("STRING", FieldType.STRING)
          .put("TIME", FieldType.logicalType(SqlTypes.TIME))
          .put("TIMESTAMP", FieldType.DATETIME)
          .put("NUMERIC", FieldType.DECIMAL)
          .put("MAP<STRING,STRING>", FieldType.map(FieldType.STRING, FieldType.STRING))
          .build();

  /** Convert DataCatalog schema to Beam schema. */
  static Schema fromDataCatalog(com.google.cloud.datacatalog.v1beta1.Schema dcSchema) {
    return fromColumnsList(dcSchema.getColumnsList());
  }

  private static Schema fromColumnsList(List<ColumnSchema> columnsMap) {
    return columnsMap.stream().map(SchemaUtils::toBeamField).collect(toSchema());
  }

  private static Field toBeamField(ColumnSchema column) {
    String name = column.getColumn();

    // basic field type
    FieldType fieldType = getBeamFieldType(column);
    Field field = Field.of(name, fieldType);

    // set the nullable flag, or convert to a list if repeated
    if (Strings.isNullOrEmpty(column.getMode()) || "NULLABLE".equals(column.getMode())) {
      field = field.withNullable(true);
    } else if ("REQUIRED".equals(column.getMode())) {
      field = field.withNullable(false);
    } else if ("REPEATED".equals(column.getMode())) {
      field = Field.of(name, FieldType.array(fieldType));
    } else {
      throw new UnsupportedOperationException(
          "Field mode '" + column.getMode() + "' is not supported (field '" + name + "')");
    }

    return field;
  }

  private static FieldType getBeamFieldType(ColumnSchema column) {
    String dcFieldType = column.getType();

    if (FIELD_TYPES.containsKey(dcFieldType)) {
      return FIELD_TYPES.get(dcFieldType);
    }

    if ("STRUCT".equals(dcFieldType)) {
      Schema structSchema = fromColumnsList(column.getSubcolumnsList());
      return FieldType.row(structSchema);
    }

    throw new UnsupportedOperationException(
        "Field type '" + dcFieldType + "' is not supported (field '" + column.getColumn() + "')");
  }

  /** Convert Beam schema to DataCatalog schema. */
  static com.google.cloud.datacatalog.v1beta1.Schema toDataCatalog(Schema schema) {
    com.google.cloud.datacatalog.v1beta1.Schema.Builder schemaBuilder =
        com.google.cloud.datacatalog.v1beta1.Schema.newBuilder();
    for (Schema.Field field : schema.getFields()) {
      schemaBuilder.addColumns(fromBeamField(field));
    }
    return schemaBuilder.build();
  }

  private static ColumnSchema fromBeamField(Schema.Field field) {
    Schema.FieldType fieldType = field.getType();
    if (fieldType.getTypeName().equals(Schema.TypeName.ARRAY)) {
      if (fieldType.getNullable()) {
        throw new UnsupportedOperationException(
            "Nullable array type is not supported in DataCatalog schemas: " + fieldType);
      } else if (fieldType.getCollectionElementType().getNullable()) {
        throw new UnsupportedOperationException(
            "Nullable array element type is not supported in DataCatalog schemas: " + fieldType);
      } else if (fieldType.getCollectionElementType().getTypeName().equals(Schema.TypeName.ARRAY)) {
        throw new UnsupportedOperationException(
            "Array of arrays not supported in DataCatalog schemas: " + fieldType);
      }
      ColumnSchema column =
          fromBeamField(Field.of(field.getName(), fieldType.getCollectionElementType()));
      if (!column.getMode().equals("REQUIRED")) {
        // We should have bailed out earlier for any cases that would result in mode being set.
        throw new AssertionError(
            "ColumnSchema for collection element type has non-empty mode: " + fieldType);
      }
      return column.toBuilder().setMode("REPEATED").build();
    } else { // struct or primitive type
      ColumnSchema.Builder colBuilder =
          ColumnSchema.newBuilder().setType(getDataCatalogType(fieldType));

      if (fieldType.getNullable()) {
        colBuilder.setMode("NULLABLE");
      } else {
        colBuilder.setMode("REQUIRED");
      }

      // if this is a struct, add the child columns
      if (fieldType.getTypeName().equals(Schema.TypeName.ROW)) {
        for (Schema.Field subField : fieldType.getRowSchema().getFields()) {
          colBuilder.addSubcolumns(fromBeamField(subField));
        }
      }

      return colBuilder.setColumn(field.getName()).build();
    }
  }

  private static String getDataCatalogType(FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case INT32:
      case INT64:
      case BYTES:
      case DOUBLE:
      case STRING:
        return fieldType.getTypeName().name();
      case BOOLEAN:
        return "BOOL";
      case DATETIME:
        return "TIMESTAMP";
      case DECIMAL:
        return "NUMERIC";
      case LOGICAL_TYPE:
        Schema.LogicalType<?, ?> logical = fieldType.getLogicalType();
        if (SqlTypes.TIME.getIdentifier().equals(logical.getIdentifier())) {
          return "TIME";
        } else if (SqlTypes.DATE.getIdentifier().equals(logical.getIdentifier())) {
          return "DATE";
        } else if (SqlTypes.DATETIME.getIdentifier().equals(logical.getIdentifier())) {
          return "DATETIME";
        } else {
          throw new UnsupportedOperationException("Unsupported logical type: " + logical);
        }
      case ROW:
        return "STRUCT";
      case MAP:
        return String.format(
            "MAP<%s,%s>",
            getDataCatalogType(fieldType.getMapKeyType()),
            getDataCatalogType(fieldType.getMapValueType()));
      default:
        throw new UnsupportedOperationException("Unsupported type: " + fieldType);
    }
  }
}
