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
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;

/** String operator implementations. */
public class StringOperators {

  /** A {@link BeamSqlOperator} that returns a string. */
  public interface StringOperator extends BeamSqlOperator {
    @Override
    default SqlTypeName getOutputType() {
      return SqlTypeName.VARCHAR;
    }
  }

  @FunctionalInterface
  private interface StringUnaryOperator extends BeamSqlUnaryOperator {

    @Override
    default boolean accept(BeamSqlExpression arg) {
      return SqlTypeName.CHAR_TYPES.contains(arg.getOutputType());
    }

    @Override
    default SqlTypeName getOutputType() {
      return SqlTypeName.VARCHAR;
    }
  }

  public static final BeamSqlOperator CHAR_LENGTH =
      (StringUnaryOperator)
          (BeamSqlPrimitive arg) ->
              BeamSqlPrimitive.of(SqlTypeName.INTEGER, SqlFunctions.charLength(arg.getString()));

  public static final BeamSqlOperator UPPER =
      (StringUnaryOperator)
          (BeamSqlPrimitive arg) ->
              BeamSqlPrimitive.of(SqlTypeName.VARCHAR, SqlFunctions.upper(arg.getString()));

  public static final BeamSqlOperator LOWER =
      (StringUnaryOperator)
          (BeamSqlPrimitive arg) ->
              BeamSqlPrimitive.of(SqlTypeName.VARCHAR, SqlFunctions.lower(arg.getString()));

  /** {@code INITCAP}. */
  public static final BeamSqlOperator INIT_CAP =
      (StringUnaryOperator)
          (BeamSqlPrimitive arg) ->
              BeamSqlPrimitive.of(SqlTypeName.VARCHAR, SqlFunctions.initcap(arg.getString()));

  public static final BeamSqlBinaryOperator CONCAT =
      new BeamSqlBinaryOperator() {
        @Override
        public SqlTypeName getOutputType() {
          return SqlTypeName.VARCHAR;
        }

        @Override
        public boolean accept(BeamSqlExpression left, BeamSqlExpression right) {
          return SqlTypeName.CHAR_TYPES.contains(left.getOutputType())
              && SqlTypeName.CHAR_TYPES.contains(right.getOutputType());
        }

        @Override
        public BeamSqlPrimitive apply(BeamSqlPrimitive left, BeamSqlPrimitive right) {
          return BeamSqlPrimitive.of(
              SqlTypeName.VARCHAR, SqlFunctions.concat(left.getString(), right.getString()));
        }
      };

  public static final BeamSqlOperator POSITION =
      new StringOperator() {
        @Override
        public boolean accept(List<BeamSqlExpression> operands) {
          if (operands.size() < 2 || operands.size() > 3) {
            return false;
          }

          return SqlTypeName.CHAR_TYPES.contains(operands.get(0).getOutputType())
              && SqlTypeName.CHAR_TYPES.contains(operands.get(1).getOutputType())
              && ((operands.size() < 3)
                  || SqlTypeName.INT_TYPES.contains(operands.get(2).getOutputType()));
        }

        @Override
        public BeamSqlPrimitive apply(List<BeamSqlPrimitive> arguments) {
          String targetStr = arguments.get(0).getString();
          String containingStr = arguments.get(1).getString();
          int from = arguments.size() < 3 ? 1 : arguments.get(2).getInteger();
          return BeamSqlPrimitive.of(
              SqlTypeName.INTEGER, SqlFunctions.position(targetStr, containingStr, from));
        }
      };

  public static final BeamSqlOperator TRIM =
      new StringOperator() {
        @Override
        public boolean accept(List<BeamSqlExpression> subexpressions) {
          return (subexpressions.size() == 1
                  && SqlTypeName.CHAR_TYPES.contains(subexpressions.get(0).getOutputType()))
              || (subexpressions.size() == 3
                  && SqlTypeName.SYMBOL.equals(subexpressions.get(0).getOutputType())
                  && SqlTypeName.CHAR_TYPES.contains(subexpressions.get(1).getOutputType())
                  && SqlTypeName.CHAR_TYPES.contains(subexpressions.get(2).getOutputType()));
        }

        @Override
        public BeamSqlPrimitive apply(List<BeamSqlPrimitive> operands) {
          SqlTrimFunction.Flag type;
          String removeChars;
          String fromStr;

          if (operands.size() == 1) {
            type = SqlTrimFunction.Flag.BOTH;
            removeChars = " ";
            fromStr = operands.get(0).getString();
          } else {
            type = (SqlTrimFunction.Flag) operands.get(0).getValue();
            removeChars = operands.get(1).getString();
            fromStr = operands.get(2).getString();
          }

          return BeamSqlPrimitive.of(
              SqlTypeName.VARCHAR,
              trim(
                  type == SqlTrimFunction.Flag.BOTH || type == SqlTrimFunction.Flag.LEADING,
                  type == SqlTrimFunction.Flag.BOTH || type == SqlTrimFunction.Flag.TRAILING,
                  removeChars,
                  fromStr));
        }
      };

  /**
   * Calcite's implementation of TRIM is incorrect and only trims the first character in removeStr.
   *
   * <p>This implementation deliberately kept compatible with eventual upstreaming.
   */
  private static String trim(
      boolean leading, boolean trailing, String removeChars, String fromStr) {
    int j = fromStr.length();
    if (trailing) {
      for (; ; ) {
        if (j == 0) {
          return "";
        }
        if (removeChars.indexOf(fromStr.charAt(j - 1)) < 0) {
          break;
        }
        --j;
      }
    }
    int i = 0;
    if (leading) {
      for (; ; ) {
        if (i == j) {
          return "";
        }
        if (removeChars.indexOf(fromStr.charAt(i)) < 0) {
          break;
        }
        ++i;
      }
    }
    return fromStr.substring(i, j);
  }

  public static final BeamSqlOperator OVERLAY =
      new StringOperator() {
        @Override
        public boolean accept(List<BeamSqlExpression> operands) {
          return (operands.size() == 3
                  && SqlTypeName.CHAR_TYPES.contains(operands.get(0).getOutputType())
                  && SqlTypeName.CHAR_TYPES.contains(operands.get(1).getOutputType())
                  && SqlTypeName.INT_TYPES.contains(operands.get(2).getOutputType()))
              || (operands.size() == 4
                  && SqlTypeName.CHAR_TYPES.contains(operands.get(0).getOutputType())
                  && SqlTypeName.CHAR_TYPES.contains(operands.get(1).getOutputType())
                  && SqlTypeName.INT_TYPES.contains(operands.get(2).getOutputType())
                  && SqlTypeName.INT_TYPES.contains(operands.get(3).getOutputType()));
        }

        @Override
        public BeamSqlPrimitive apply(List<BeamSqlPrimitive> operands) {
          String str = operands.get(0).getString();
          String replaceStr = operands.get(1).getString();
          int idx = operands.get(2).getInteger();
          int length = operands.size() == 4 ? operands.get(3).getInteger() : replaceStr.length();
          return BeamSqlPrimitive.of(
              SqlTypeName.VARCHAR, SqlFunctions.overlay(str, replaceStr, idx, length));
        }
      };

  public static final BeamSqlOperator SUBSTRING =
      new StringOperator() {

        @Override
        public boolean accept(List<BeamSqlExpression> operands) {
          return (operands.size() == 2
                  && SqlTypeName.CHAR_TYPES.contains(operands.get(0).getOutputType())
                  && SqlTypeName.INT_TYPES.contains(operands.get(1).getOutputType()))
              || (operands.size() == 3
                  && SqlTypeName.CHAR_TYPES.contains(operands.get(0).getOutputType())
                  && SqlTypeName.INT_TYPES.contains(operands.get(1).getOutputType())
                  && SqlTypeName.INT_TYPES.contains(operands.get(2).getOutputType()));
        }

        @Override
        public BeamSqlPrimitive apply(List<BeamSqlPrimitive> operands) {
          String str = operands.get(0).getString();
          int from = operands.get(1).getInteger();
          if (from < 0) {
            from = from + str.length() + 1;
          }

          if (operands.size() == 3) {
            int length = operands.get(2).getInteger();

            return BeamSqlPrimitive.of(
                SqlTypeName.VARCHAR, SqlFunctions.substring(str, from, length));
          } else {
            return BeamSqlPrimitive.of(SqlTypeName.VARCHAR, SqlFunctions.substring(str, from));
          }
        }
      };
}
