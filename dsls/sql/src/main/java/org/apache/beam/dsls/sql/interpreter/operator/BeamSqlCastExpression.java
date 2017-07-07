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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

/**
 * Base class to support 'CAST' operations for all {@link SqlTypeName}.
 */
public class BeamSqlCastExpression extends BeamSqlExpression {

  private static final int index = 0;
  private static final String outputTimestampFormat = "yyyy-MM-dd HH:mm:ss";
  private static final String outputDateFormat = "yyyy-MM-dd";
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

  @Override public boolean accept() {
    return numberOfOperands() == 1;
  }

  @Override public BeamSqlPrimitive evaluate(BeamSqlRow inputRecord) {
    SqlTypeName castOutputType = getOutputType();
    switch (castOutputType) {
      case INTEGER:
        return BeamSqlPrimitive
            .of(castOutputType, SqlFunctions.toInt(opValueEvaluated(index, inputRecord)));
      case DOUBLE:
        return BeamSqlPrimitive
            .of(castOutputType, SqlFunctions.toDouble(opValueEvaluated(index, inputRecord)));
      case SMALLINT:
        return BeamSqlPrimitive
            .of(castOutputType, SqlFunctions.toShort(opValueEvaluated(index, inputRecord)));
      case TINYINT:
        return BeamSqlPrimitive
            .of(castOutputType, SqlFunctions.toByte(opValueEvaluated(index, inputRecord)));
      case BIGINT:
        return BeamSqlPrimitive
            .of(castOutputType, SqlFunctions.toLong(opValueEvaluated(index, inputRecord)));
      case DECIMAL:
        return BeamSqlPrimitive
            .of(castOutputType, SqlFunctions.toBigDecimal(opValueEvaluated(index, inputRecord)));
      case FLOAT:
        return BeamSqlPrimitive
            .of(castOutputType, SqlFunctions.toFloat(opValueEvaluated(index, inputRecord)));
      case CHAR:
      case VARCHAR:
        return BeamSqlPrimitive.of(castOutputType, opValueEvaluated(index, inputRecord).toString());
      case DATE:
        return BeamSqlPrimitive
            .of(castOutputType, toDate(opValueEvaluated(index, inputRecord), outputDateFormat));
      case TIMESTAMP:
        return BeamSqlPrimitive.of(castOutputType,
            toTimeStamp(opValueEvaluated(index, inputRecord), outputTimestampFormat));
    }
    throw new RuntimeException(String.format("Cast to type %s not supported", castOutputType));
  }

  private Date toDate(Object inputDate, String outputFormat) {
    try {
      return Date
          .valueOf(dateTimeFormatter.parseLocalDate(inputDate.toString()).toString(outputFormat));
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      return null;
    }
  }

  private Timestamp toTimeStamp(Object inputTimestamp, String outputFormat) {
    try {
      return Timestamp.valueOf(
          dateTimeFormatter.parseDateTime(inputTimestamp.toString()).secondOfMinute()
              .roundCeilingCopy().toString(outputFormat));
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      return null;
    }
  }
}
