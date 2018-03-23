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

package org.apache.beam.sdk.extensions.sql.impl.utils;

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Utility methods for Calcite related operations.
 */
public class CalciteUtils {
  private static final long UNLIMITED_ARRAY_SIZE = -1L;
  private static final BiMap<Schema.FieldType, SqlTypeName> BEAM_TO_CALCITE_TYPE_MAPPING =
      ImmutableBiMap.<Schema.FieldType, SqlTypeName>builder()
          .put(FieldType.BYTE, SqlTypeName.TINYINT)
          .put(FieldType.INT16, SqlTypeName.SMALLINT)
          .put(FieldType.INT32, SqlTypeName.INTEGER)
          .put(FieldType.INT64, SqlTypeName.BIGINT)

          .put(FieldType.FLOAT, SqlTypeName.FLOAT)
          .put(FieldType.DOUBLE, SqlTypeName.DOUBLE)

          .put(FieldType.DECIMAL, SqlTypeName.DECIMAL)

          .put(FieldType.CHAR, SqlTypeName.CHAR)
          .put(FieldType.STRING, SqlTypeName.VARCHAR)

          .put(FieldType.DATE, SqlTypeName.DATE)
          .put(FieldType.TIME, SqlTypeName.TIME)
          .put(FieldType.DATETIME, SqlTypeName.TIMESTAMP)

          .put(FieldType.BOOLEAN, SqlTypeName.BOOLEAN)

          .put(FieldType.ARRAY, SqlTypeName.ARRAY)
          .put(FieldType.ROW, SqlTypeName.ROW)
          .build();

  private static final BiMap<SqlTypeName, Schema.FieldType> CALCITE_TO_BEAM_TYPE_MAPPING =
      BEAM_TO_CALCITE_TYPE_MAPPING.inverse();

  /**
   * Generate {@code BeamSqlRowType} from {@code RelDataType} which is used to create table.
   */
  public static Schema toBeamSchema(RelDataType tableInfo) {
    return
        tableInfo
            .getFieldList()
            .stream()
            .map(CalciteUtils::toBeamSchemaField)
            .collect(toSchema());
  }

  public static Schema.FieldType toFieldType(SqlTypeName sqlTypeName) {
    return CALCITE_TO_BEAM_TYPE_MAPPING.get(sqlTypeName);
  }

  public static SqlTypeName toSqlTypeName(Schema.FieldType fieldType) {
    return BEAM_TO_CALCITE_TYPE_MAPPING.get(fieldType);
  }

  public static Schema.FieldTypeDescriptor toFieldTypeDescriptor(RelDataType calciteType) {
    FieldType mainType = toFieldType((calciteType.getSqlTypeName()));
    Schema.FieldTypeDescriptor typeDescriptor = Schema.FieldTypeDescriptor.of(mainType);
    if (calciteType.getComponentType() != null) {
      typeDescriptor = typeDescriptor.withComponentType(toFieldTypeDescriptor(
          calciteType.getComponentType()));
    }
    if (calciteType.isStruct()) {
      typeDescriptor = typeDescriptor.withRowSchema(toBeamSchema(calciteType));
    }
    return typeDescriptor;
  }

  public static Schema.Field toBeamSchemaField(RelDataTypeField calciteField) {
    Schema.FieldTypeDescriptor fieldTypeDescriptor = toFieldTypeDescriptor(calciteField.getType());
    boolean isNullable = calciteField.getType().isNullable();
    return Schema.Field.of(calciteField.getName(), fieldTypeDescriptor)
        .withNullable(isNullable);
  }

  /**
   * Create an instance of {@code RelDataType} so it can be used to create a table.
   */
  public static RelDataType toCalciteRowType(Schema schema, RelDataTypeFactory dataTypeFactory) {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(dataTypeFactory);

    IntStream
        .range(0, schema.getFieldCount())
        .forEach(idx ->
                     builder.add(
                         schema.getField(idx).getName(),
                         toRelDataType(dataTypeFactory, schema, idx)));
    return builder.build();
  }

  private static RelDataType toRelDataType(
      RelDataTypeFactory dataTypeFactory, Schema.FieldTypeDescriptor fieldTypeDescriptor) {
    SqlTypeName typeName = toSqlTypeName(fieldTypeDescriptor.getType());
    if (SqlTypeName.ARRAY.equals(typeName)) {
      RelDataType componentType = toRelDataType(
          dataTypeFactory, fieldTypeDescriptor.getComponentType());
      return dataTypeFactory.createArrayType(componentType, UNLIMITED_ARRAY_SIZE);
    } else if (SqlTypeName.ROW.equals(typeName)) {
      return toCalciteRowType(fieldTypeDescriptor.getRowSchema(), dataTypeFactory);
    } else {
      return dataTypeFactory.createSqlType(typeName);
    }
  }

  private static RelDataType toRelDataType(
      RelDataTypeFactory dataTypeFactory,
      Schema schema,
      int fieldIndex) {
    Schema.Field field = schema.getField(fieldIndex);
    return toRelDataType(dataTypeFactory, field.getTypeDescriptor());
  }
}
