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

package org.apache.beam.dsls.sql.utils;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Utility methods for Calcite related operations.
 */
public class CalciteUtils {
  private static final Map<Integer, SqlTypeName> JAVA_TO_CALCITE_MAPPING = new HashMap<>();
  private static final Map<SqlTypeName, Integer> CALCITE_TO_JAVA_MAPPING = new HashMap<>();
  static {
    JAVA_TO_CALCITE_MAPPING.put(Types.TINYINT, SqlTypeName.TINYINT);
    JAVA_TO_CALCITE_MAPPING.put(Types.SMALLINT, SqlTypeName.SMALLINT);
    JAVA_TO_CALCITE_MAPPING.put(Types.INTEGER, SqlTypeName.INTEGER);
    JAVA_TO_CALCITE_MAPPING.put(Types.BIGINT, SqlTypeName.BIGINT);

    JAVA_TO_CALCITE_MAPPING.put(Types.FLOAT, SqlTypeName.FLOAT);
    JAVA_TO_CALCITE_MAPPING.put(Types.DOUBLE, SqlTypeName.DOUBLE);

    JAVA_TO_CALCITE_MAPPING.put(Types.DECIMAL, SqlTypeName.DECIMAL);

    JAVA_TO_CALCITE_MAPPING.put(Types.CHAR, SqlTypeName.CHAR);
    JAVA_TO_CALCITE_MAPPING.put(Types.VARCHAR, SqlTypeName.VARCHAR);

    JAVA_TO_CALCITE_MAPPING.put(Types.DATE, SqlTypeName.DATE);
    JAVA_TO_CALCITE_MAPPING.put(Types.TIME, SqlTypeName.TIME);
    JAVA_TO_CALCITE_MAPPING.put(Types.TIMESTAMP, SqlTypeName.TIMESTAMP);

    for (Map.Entry<Integer, SqlTypeName> pair : JAVA_TO_CALCITE_MAPPING.entrySet()) {
      CALCITE_TO_JAVA_MAPPING.put(pair.getValue(), pair.getKey());
    }
  }

  /**
   * Get the corresponding {@code SqlTypeName} for an integer sql type.
   */
  public static SqlTypeName toCalciteType(int type) {
    return JAVA_TO_CALCITE_MAPPING.get(type);
  }

  /**
   * Get the integer sql type from Calcite {@code SqlTypeName}.
   */
  public static Integer toJavaType(SqlTypeName typeName) {
    return CALCITE_TO_JAVA_MAPPING.get(typeName);
  }

  /**
   * Get the {@code SqlTypeName} for the specified column of a table.
   */
  public static SqlTypeName getFieldType(BeamSqlRecordType schema, int index) {
    return toCalciteType(schema.getFieldsType().get(index));
  }

  /**
   * Generate {@code BeamSqlRecordType} from {@code RelDataType} which is used to create table.
   */
  public static BeamSqlRecordType toBeamRecordType(RelDataType tableInfo) {
    List<String> fieldNames = new ArrayList<>();
    List<Integer> fieldTypes = new ArrayList<>();
    for (RelDataTypeField f : tableInfo.getFieldList()) {
      fieldNames.add(f.getName());
      fieldTypes.add(toJavaType(f.getType().getSqlTypeName()));
    }
    return BeamSqlRecordType.create(fieldNames, fieldTypes);
  }

  /**
   * Create an instance of {@code RelDataType} so it can be used to create a table.
   */
  public static RelProtoDataType toCalciteRecordType(final BeamSqlRecordType that) {
    return new RelProtoDataType() {
      @Override
      public RelDataType apply(RelDataTypeFactory a) {
        RelDataTypeFactory.FieldInfoBuilder builder = a.builder();
        for (int idx = 0; idx < that.getFieldsName().size(); ++idx) {
          builder.add(that.getFieldsName().get(idx), toCalciteType(that.getFieldsType().get(idx)));
        }
        return builder.build();
      }
    };
  }
}
