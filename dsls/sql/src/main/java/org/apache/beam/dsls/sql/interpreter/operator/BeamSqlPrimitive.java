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
package org.apache.beam.dsls.sql.interpreter.operator;

import java.math.BigDecimal;
import java.util.List;
import org.apache.beam.dsls.sql.exception.BeamInvalidOperatorException;
import org.apache.beam.dsls.sql.exception.BeamSqlUnsupportedException;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

/**
 * {@link BeamSqlPrimitive} is a special, self-reference {@link BeamSqlExpression}.
 * It holds the value, and return it directly during {@link #evaluate(BeamSQLRow)}.
 *
 */
public class BeamSqlPrimitive<T> extends BeamSqlExpression{
  private SqlTypeName outputType;
  private T value;

  private BeamSqlPrimitive() {
  }

  private BeamSqlPrimitive(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    super(operands, outputType);
  }

  /**
   * A builder function to create from Type and value directly.
   */
  public static <T> BeamSqlPrimitive<T> of(SqlTypeName outputType, T value){
    BeamSqlPrimitive<T> exp = new BeamSqlPrimitive<T>();
    exp.outputType = outputType;
    exp.value = value;
    if (!exp.accept()) {
      throw new BeamInvalidOperatorException(
          String.format("value [%s] doesn't match type [%s].", value, outputType));
    }
    return exp;
  }

  public SqlTypeName getOutputType() {
    return outputType;
  }

  public T getValue() {
    return value;
  }

  @Override
  public boolean accept() {
    if (value == null) {
      return true;
    }

    switch (outputType) {
    case BIGINT:
      return value instanceof Long;
    case DECIMAL:
      return value instanceof BigDecimal;
    case DOUBLE:
      return value instanceof Double;
    case FLOAT:
      return value instanceof Float;
    case INTEGER:
      return value instanceof Integer;
    case SMALLINT:
      return value instanceof Short;
    case TINYINT:
      return value instanceof Byte;
    case BOOLEAN:
      return value instanceof Boolean;
    case CHAR:
    case VARCHAR:
      return value instanceof String || value instanceof NlsString;
    default:
      throw new BeamSqlUnsupportedException(outputType.name());
    }
  }

  @Override
  public BeamSqlPrimitive<T> evaluate(BeamSQLRow inputRecord) {
    return this;
  }

}
