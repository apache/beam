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

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoders;
import org.apache.beam.sdk.values.BeamRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
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
    return BEAM_TO_CALCITE_TYPE_MAPPING.get(coder);
  }

  /**
   * Get the Beam SQL type coder ({@link SqlTypeCoder}) from Calcite's {@link SqlTypeName}.
   */
  public static SqlTypeCoder toCoder(SqlTypeName typeName) {
    return CALCITE_TO_BEAM_TYPE_MAPPING.get(typeName);
  }

  /**
   * Get the {@code SqlTypeName} for the specified column of a table.
   */
  public static SqlTypeName getFieldCalciteType(BeamRecordType schema, int index) {
    return toCalciteType((SqlTypeCoder) schema.getFieldCoder(index));
  }

  /**
   * Generate {@code BeamSqlRowType} from {@code RelDataType} which is used to create table.
   */
  public static BeamRecordType toBeamRowType(RelDataType tableInfo) {
    List<String> fieldNames = new ArrayList<>();
    List<Coder> fieldCoders = new ArrayList<>();
    for (RelDataTypeField f : tableInfo.getFieldList()) {
      fieldNames.add(f.getName());
      fieldCoders.add(toCoder(f.getType().getSqlTypeName()));
    }
    return new BeamRecordType(fieldNames, fieldCoders);
  }

  /**
   * Create an instance of {@code RelDataType} so it can be used to create a table.
   */
  public static RelProtoDataType toCalciteRowType(final BeamRecordType recordType) {
    return fieldInfo -> {
      RelDataTypeFactory.FieldInfoBuilder builder = fieldInfo.builder();
      for (int idx = 0; idx < recordType.getFieldNames().size(); ++idx) {
        builder.add(
            recordType.getFieldNameByIndex(idx),
            toCalciteType((SqlTypeCoder) recordType.getFieldCoder(idx)));
      }
      return builder.build();
    };
  }
}
