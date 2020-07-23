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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.Strings;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;

@Experimental(Kind.SCHEMAS)
class SchemaUtils {

  private static final Map<String, FieldType> FIELD_TYPES =
      ImmutableMap.<String, FieldType>builder()
          .put("BOOL", FieldType.BOOLEAN)
          .put("BYTES", FieldType.BYTES)
          .put("DATE", FieldType.logicalType(SqlTypes.DATE))
          .put("DATETIME", FieldType.DATETIME)
          .put("DOUBLE", FieldType.DOUBLE)
          .put("FLOAT", FieldType.DOUBLE)
          .put("FLOAT64", FieldType.DOUBLE)
          .put("INT32", FieldType.INT32)
          .put("INT64", FieldType.INT64)
          .put("STRING", FieldType.STRING)
          .put("TIME", FieldType.logicalType(SqlTypes.TIME))
          .put("TIMESTAMP", FieldType.DATETIME)
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
}
