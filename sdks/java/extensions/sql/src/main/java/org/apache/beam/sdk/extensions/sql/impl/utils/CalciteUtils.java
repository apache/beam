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

import static org.apache.beam.sdk.values.RowType.toRowType;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoders;
import org.apache.beam.sdk.values.RowType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Utility methods for Calcite related operations.
 */
public class CalciteUtils {
  private static final BiMap<SqlTypeCoder, SqlTypeName> BEAM_TO_CALCITE_TYPE_MAPPING =
      ImmutableBiMap.<SqlTypeCoder, SqlTypeName>builder()
          .put(SqlTypeCoders.TINYINT, SqlTypeName.TINYINT)
          .put(SqlTypeCoders.SMALLINT, SqlTypeName.SMALLINT)
          .put(SqlTypeCoders.INTEGER, SqlTypeName.INTEGER)
          .put(SqlTypeCoders.BIGINT, SqlTypeName.BIGINT)

          .put(SqlTypeCoders.FLOAT, SqlTypeName.FLOAT)
          .put(SqlTypeCoders.DOUBLE, SqlTypeName.DOUBLE)

          .put(SqlTypeCoders.DECIMAL, SqlTypeName.DECIMAL)

          .put(SqlTypeCoders.CHAR, SqlTypeName.CHAR)
          .put(SqlTypeCoders.VARCHAR, SqlTypeName.VARCHAR)

          .put(SqlTypeCoders.DATE, SqlTypeName.DATE)
          .put(SqlTypeCoders.TIME, SqlTypeName.TIME)
          .put(SqlTypeCoders.TIMESTAMP, SqlTypeName.TIMESTAMP)

          .put(SqlTypeCoders.BOOLEAN, SqlTypeName.BOOLEAN)
          .build();

  private static final BiMap<SqlTypeName, SqlTypeCoder> CALCITE_TO_BEAM_TYPE_MAPPING =
      BEAM_TO_CALCITE_TYPE_MAPPING.inverse();

  /**
   * Get the corresponding Calcite's {@link SqlTypeName}
   * for supported Beam SQL type coder, see {@link SqlTypeCoder}.
   */
  public static SqlTypeName toCalciteType(SqlTypeCoder coder) {
    return SqlTypeCoder.isArray(coder)
        ? SqlTypeName.ARRAY
        : BEAM_TO_CALCITE_TYPE_MAPPING.get(coder);
  }

  /**
   * Get the Beam SQL type coder ({@link SqlTypeCoder}) from Calcite's {@link RelDataTypeField}.
   */
  public static SqlTypeCoder toCoder(RelDataTypeField relFieldType) {
    if (relFieldType.getValue() instanceof BasicSqlType) {
      return toCoder(relFieldType.getType().getSqlTypeName());
    } else if (relFieldType.getValue() instanceof ArraySqlType) {
      SqlTypeName elementType = relFieldType.getValue().getComponentType().getSqlTypeName();
      SqlTypeCoder elementCoder = CALCITE_TO_BEAM_TYPE_MAPPING.get(elementType);
      return SqlTypeCoders.arrayOf(elementCoder);
    } else {
      throw new UnsupportedOperationException("Field type '" + relFieldType + "' is not supported");
    }
  }

  /**
   * Get the Beam SQL type coder ({@link SqlTypeCoder}) from Calcite's {@link SqlTypeName}.
   */
  public static SqlTypeCoder toCoder(SqlTypeName relFieldType) {
    return CALCITE_TO_BEAM_TYPE_MAPPING.get(relFieldType);
  }

  /**
   * Get the {@code SqlTypeName} for the specified column of a table.
   */
  public static SqlTypeName getFieldCalciteType(RowType schema, int index) {
    return toCalciteType((SqlTypeCoder) schema.getFieldCoder(index));
  }

  /**
   * Generate {@code BeamSqlRowType} from {@code RelDataType} which is used to create table.
   */
  public static RowType toBeamRowType(RelDataType tableInfo) {
    return
        tableInfo
            .getFieldList()
            .stream()
            .map(CalciteUtils::toBeamRowField)
            .collect(toRowType());
  }

  private static RowType.Field toBeamRowField(RelDataTypeField calciteField) {
    return
        RowType.newField(
            calciteField.getName(),
            toCoder(calciteField));
  }

  /**
   * Create an instance of {@code RelDataType} so it can be used to create a table.
   */
  public static RelProtoDataType toCalciteRowType(final RowType rowType) {
    return fieldInfo -> {
      RelDataTypeFactory.FieldInfoBuilder builder = fieldInfo.builder();
      for (int idx = 0; idx < rowType.getFieldNames().size(); ++idx) {
        builder.add(
            rowType.getFieldName(idx),
            toCalciteType((SqlTypeCoder) rowType.getFieldCoder(idx)));
      }
      return builder.build();
    };
  }
}
