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
package org.apache.beam.sdk.extensions.sql.schema;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.values.BeamRecordTypeProvider;

/**
 * Type provider for {@link BeamSqlRow} with SQL types.
 *
 * <p>Limited SQL types are supported now, visit
 * <a href="https://beam.apache.org/blog/2017/07/21/sql-dsl.html#data-type">data types</a>
 * for more details.
 *
 */
public class BeamSqlRowType extends BeamRecordTypeProvider {
  private static final Map<Integer, Class> SQL_TYPE_TO_JAVA_CLASS = new HashMap<>();
  static {
    SQL_TYPE_TO_JAVA_CLASS.put(Types.TINYINT, Byte.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.SMALLINT, Short.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.INTEGER, Integer.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.BIGINT, Long.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.FLOAT, Float.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.DOUBLE, Double.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.DECIMAL, BigDecimal.class);

    SQL_TYPE_TO_JAVA_CLASS.put(Types.BOOLEAN, Boolean.class);

    SQL_TYPE_TO_JAVA_CLASS.put(Types.CHAR, String.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.VARCHAR, String.class);

    SQL_TYPE_TO_JAVA_CLASS.put(Types.TIME, GregorianCalendar.class);

    SQL_TYPE_TO_JAVA_CLASS.put(Types.DATE, Date.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.TIMESTAMP, Date.class);
  }

  public List<Integer> fieldsType;

  protected BeamSqlRowType(List<String> fieldsName) {
    super(fieldsName);
  }

  public BeamSqlRowType(List<String> fieldsName, List<Integer> fieldsType) {
    super(fieldsName);
    this.fieldsType = fieldsType;
  }

  public static BeamSqlRowType create(List<String> fieldNames,
      List<Integer> fieldTypes) {
    return new BeamSqlRowType(fieldNames, fieldTypes);
  }

  @Override
  public void validateValueType(int index, Object fieldValue) throws IllegalArgumentException {
    int fieldType = fieldsType.get(index);
    Class javaClazz = SQL_TYPE_TO_JAVA_CLASS.get(fieldType);
    if (javaClazz == null) {
      throw new IllegalArgumentException("Data type: " + fieldType + " not supported yet!");
    }

    if (!fieldValue.getClass().equals(javaClazz)) {
      throw new IllegalArgumentException(
          String.format("[%s](%s) doesn't match type [%s]",
              fieldValue, fieldValue.getClass(), fieldType)
      );
    }
  }

  public List<Integer> getFieldsType() {
    return fieldsType;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj instanceof BeamSqlRowType) {
      BeamSqlRowType ins = (BeamSqlRowType) obj;
      return fieldsType.equals(ins.getFieldsType()) && getFieldsName().equals(ins.getFieldsName());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return 31 * getFieldsName().hashCode() + getFieldsType().hashCode();
  }
}
