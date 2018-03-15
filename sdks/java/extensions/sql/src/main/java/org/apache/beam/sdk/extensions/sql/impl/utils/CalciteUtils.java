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
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlArrayCoder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder.SqlRowCoder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoders;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Utility methods for Calcite related operations.
 */
public class CalciteUtils {
  private static final long UNLIMITED_ARRAY_SIZE = -1L;
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
    if (SqlTypeCoders.isArray(coder)) {
        return SqlTypeName.ARRAY;
    }

    if (SqlTypeCoders.isRow(coder)) {
      return SqlTypeName.ROW;
    }

    return BEAM_TO_CALCITE_TYPE_MAPPING.get(coder);
  }

  /**
   * Get the Beam SQL type coder ({@link SqlTypeCoder}) from Calcite's {@link RelDataTypeField}.
   */
  public static SqlTypeCoder toCoder(RelDataTypeField relFieldType) {
    SqlTypeName fieldTypeName = relFieldType.getType().getSqlTypeName();

    if (SqlTypeName.ARRAY.equals(fieldTypeName)) {
      RelDataType elementType = relFieldType.getValue().getComponentType();
      SqlTypeCoder elementCoder = CALCITE_TO_BEAM_TYPE_MAPPING.get(elementType.getSqlTypeName());
      return SqlTypeCoders.arrayOf(elementCoder);
    }

    if (SqlTypeName.ROW.equals(fieldTypeName)) {
      RelDataType nestedCalciteRowType = relFieldType.getValue().getComponentType();
      return SqlTypeCoders.rowOf(toBeamRowType(nestedCalciteRowType));
    }

    return toCoder(fieldTypeName);
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
  public static SqlTypeName getFieldCalciteType(Schema schema, int index) {
    return toCalciteType((SqlTypeCoder) schema.getFieldCoder(index));
  }

  /**
   * Generate {@code BeamSqlRowType} from {@code RelDataType} which is used to create table.
   */
  public static Schema toBeamRowType(RelDataType tableInfo) {
    return
        tableInfo
            .getFieldList()
            .stream()
            .map(CalciteUtils::toBeamRowField)
            .collect(toSchema());
  }

  private static Schema.Field toBeamRowField(RelDataTypeField calciteField) {
    return
        Schema.newField(
            calciteField.getName(),
            toCoder(calciteField));
  }

  /**
   * Create an instance of {@code RelDataType} so it can be used to create a table.
   */
  public static RelDataType toCalciteRowType(Schema rowType, RelDataTypeFactory dataTypeFactory) {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(dataTypeFactory);

    IntStream
        .range(0, rowType.getFieldCount())
        .forEach(idx ->
                     builder.add(
                         rowType.getFieldName(idx),
                         toRelDataType(dataTypeFactory, rowType, idx)));
    return builder.build();
  }

  private static RelDataType toRelDataType(
      RelDataTypeFactory dataTypeFactory,
      Schema schema,
      int fieldIndex) {

    SqlTypeCoder fieldCoder = (SqlTypeCoder) schema.getFieldCoder(fieldIndex);
    SqlTypeName typeName = toCalciteType(fieldCoder);

    if (SqlTypeName.ARRAY.equals(typeName)) {
      return createArrayRelType(dataTypeFactory, (SqlArrayCoder) fieldCoder);
    }

    if (SqlTypeName.ROW.equals(typeName)) {
      return toCalciteRowType(((SqlRowCoder) fieldCoder).getSchema(), dataTypeFactory);
    }

    return dataTypeFactory.createSqlType(typeName);
  }

  private static RelDataType createArrayRelType(
      RelDataTypeFactory dataTypeFactory,
      SqlArrayCoder arrayFieldCoder) {

    SqlTypeName elementTypeName = toCalciteType(arrayFieldCoder.getElementCoder());

    RelDataType elementType;

    if (SqlTypeName.ROW.equals(elementTypeName)) {
      Schema schema = ((SqlRowCoder) arrayFieldCoder.getElementCoder()).getSchema();
      elementType = toCalciteRowType(schema, dataTypeFactory);
    } else {
      elementType = dataTypeFactory.createSqlType(elementTypeName);
    }

    return dataTypeFactory.createArrayType(elementType, UNLIMITED_ARRAY_SIZE);
  }
}
