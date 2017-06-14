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
import java.util.HashMap;
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
  private static final Map<Integer, SqlTypeName> SQL_TYPE_MAPPING = new HashMap<>();
  private static final Map<SqlTypeName, Integer> INVERSED_SQL_TYPE_MAPPING = new HashMap<>();
  static {
    SQL_TYPE_MAPPING.put(Types.TINYINT, SqlTypeName.TINYINT);
    SQL_TYPE_MAPPING.put(Types.SMALLINT, SqlTypeName.SMALLINT);
    SQL_TYPE_MAPPING.put(Types.INTEGER, SqlTypeName.INTEGER);
    SQL_TYPE_MAPPING.put(Types.BIGINT, SqlTypeName.BIGINT);

    SQL_TYPE_MAPPING.put(Types.FLOAT, SqlTypeName.FLOAT);
    SQL_TYPE_MAPPING.put(Types.DOUBLE, SqlTypeName.DOUBLE);

    SQL_TYPE_MAPPING.put(Types.DECIMAL, SqlTypeName.DECIMAL);

    SQL_TYPE_MAPPING.put(Types.CHAR, SqlTypeName.CHAR);
    SQL_TYPE_MAPPING.put(Types.VARCHAR, SqlTypeName.VARCHAR);

    SQL_TYPE_MAPPING.put(Types.TIME, SqlTypeName.TIME);
    SQL_TYPE_MAPPING.put(Types.TIMESTAMP, SqlTypeName.TIMESTAMP);

    for (Map.Entry<Integer, SqlTypeName> pair : SQL_TYPE_MAPPING.entrySet()) {
      INVERSED_SQL_TYPE_MAPPING.put(pair.getValue(), pair.getKey());
    }
  }

  /**
   * Get the corresponding {@code SqlTypeName} for an integer sql type.
   */
  public static SqlTypeName getSqlTypeName(int type) {
    return SQL_TYPE_MAPPING.get(type);
  }

  /**
   * Get the integer sql type from Calcite {@code SqlTypeName}.
   */
  public static Integer getJavaSqlType(SqlTypeName typeName) {
    return INVERSED_SQL_TYPE_MAPPING.get(typeName);
  }

  /**
   * Get the {@code SqlTypeName} for the specified column of a table.
   * @return
   */
  public static SqlTypeName getFieldType(BeamSqlRecordType schema, int index) {
    return getSqlTypeName(schema.getFieldsType().get(index));
  }

  /**
   * Generate {@code BeamSqlRecordType} from {@code RelDataType} which is used to create table.
   */
  public static BeamSqlRecordType buildRecordType(RelDataType tableInfo) {
    BeamSqlRecordType record = new BeamSqlRecordType();
    for (RelDataTypeField f : tableInfo.getFieldList()) {
      record.getFieldsName().add(f.getName());
      record.getFieldsType().add(getJavaSqlType(f.getType().getSqlTypeName()));
    }
    return record;
  }

  /**
   * Create an instance of {@code RelDataType} so it can be used to create a table.
   */
  public static RelProtoDataType toRelDataType(final BeamSqlRecordType that) {
    return new RelProtoDataType() {
      @Override
      public RelDataType apply(RelDataTypeFactory a) {
        RelDataTypeFactory.FieldInfoBuilder builder = a.builder();
        for (int idx = 0; idx < that.getFieldsName().size(); ++idx) {
          builder.add(that.getFieldsName().get(idx), getSqlTypeName(that.getFieldsType().get(idx)));
        }
        return builder.build();
      }
    };
  }
}
