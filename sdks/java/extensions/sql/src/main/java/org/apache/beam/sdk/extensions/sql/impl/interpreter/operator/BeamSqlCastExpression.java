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

package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator;

import java.util.List;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

/**
 * Base class to support 'CAST' operations for all {@link SqlTypeName}.
 */
public class  BeamSqlCastExpression extends BeamSqlExpression {

  private static final int index = 0;
  /**
   * Date and Timestamp formats used to parse
   * {@link SqlTypeName#DATE}, {@link SqlTypeName#TIMESTAMP}.
   */
  private static final DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
      .append(null/*printer*/, new DateTimeParser[] {
          // date formats
          DateTimeFormat.forPattern("yy-MM-dd").getParser(),
          DateTimeFormat.forPattern("yy/MM/dd").getParser(),
          DateTimeFormat.forPattern("yy.MM.dd").getParser(),
          DateTimeFormat.forPattern("yyMMdd").getParser(),
          DateTimeFormat.forPattern("yyyyMMdd").getParser(),
          DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
          DateTimeFormat.forPattern("yyyy/MM/dd").getParser(),
          DateTimeFormat.forPattern("yyyy.MM.dd").getParser(),
          // datetime formats
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser(),
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssz").getParser(),
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss z").getParser(),
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").getParser(),
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSSz").getParser(),
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS z").getParser() }).toFormatter()
      .withPivotYear(2020);

  public BeamSqlCastExpression(List<BeamSqlExpression> operands, SqlTypeName castType) {
    super(operands, castType);
  }

  @Override
  public boolean accept() {
    return numberOfOperands() == 1;
  }

  @Override
  public BeamSqlPrimitive evaluate(Row inputRow, BoundedWindow window) {
    SqlTypeName castOutputType = getOutputType();
    switch (castOutputType) {
      case INTEGER:
        return BeamSqlPrimitive
            .of(SqlTypeName.INTEGER,
                    SqlFunctions.toInt((Object) opValueEvaluated(index, inputRow, window)));
      case DOUBLE:
        return BeamSqlPrimitive.of(SqlTypeName.DOUBLE,
            SqlFunctions.toDouble((Object) opValueEvaluated(index, inputRow, window)));
      case SMALLINT:
        return BeamSqlPrimitive.of(SqlTypeName.SMALLINT,
            SqlFunctions.toShort((Object) opValueEvaluated(index, inputRow, window)));
      case TINYINT:
        return BeamSqlPrimitive.of(SqlTypeName.TINYINT,
            SqlFunctions.toByte(opValueEvaluated(index, inputRow, window)));
      case BIGINT:
        return BeamSqlPrimitive
            .of(SqlTypeName.BIGINT,
                    SqlFunctions.toLong((Object) opValueEvaluated(index, inputRow, window)));
      case DECIMAL:
        return BeamSqlPrimitive.of(SqlTypeName.DECIMAL,
            SqlFunctions.toBigDecimal((Object) opValueEvaluated(index, inputRow, window)));
      case FLOAT:
        return BeamSqlPrimitive.of(SqlTypeName.FLOAT,
                SqlFunctions.toFloat((Object) opValueEvaluated(index, inputRow, window)));
      case CHAR:
      case VARCHAR:
        return BeamSqlPrimitive
            .of(SqlTypeName.VARCHAR, opValueEvaluated(index, inputRow, window).toString());
      case DATE:
        return BeamSqlPrimitive.of(SqlTypeName.DATE,
            toDate(opValueEvaluated(index, inputRow, window)));
      case TIMESTAMP:
        return BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP,
            toTimeStamp(opValueEvaluated(index, inputRow, window)));
    }
    throw new UnsupportedOperationException(
        String.format("Cast to type %s not supported", castOutputType));
  }

  private ReadableInstant toDate(Object inputDate) {
    return dateTimeFormatter.parseLocalDate(inputDate.toString()).toDateTimeAtStartOfDay();

  }

  private ReadableInstant toTimeStamp(Object inputTimestamp) {
    return dateTimeFormatter.parseDateTime(inputTimestamp.toString()).secondOfMinute()
        .roundCeilingCopy();
  }
}
