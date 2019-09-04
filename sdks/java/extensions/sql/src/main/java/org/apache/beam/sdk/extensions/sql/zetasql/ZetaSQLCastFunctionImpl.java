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
package org.apache.beam.sdk.extensions.sql.zetasql;

import static org.apache.calcite.adapter.enumerable.RexImpTable.createImplementor;

import java.util.List;
import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/** ZetaSQLCastFunctionImpl. */
public class ZetaSQLCastFunctionImpl implements Function, ImplementableFunction {
  public static final SqlUserDefinedFunction ZETASQL_CAST_OP =
      new SqlUserDefinedFunction(
          new SqlIdentifier("CAST", SqlParserPos.ZERO),
          null,
          null,
          null,
          null,
          new ZetaSQLCastFunctionImpl());

  @Override
  public CallImplementor getImplementor() {
    return createImplementor(new ZetaSQLCastCallNotNullImplementor(), NullPolicy.STRICT, false);
  }

  @Override
  public List<FunctionParameter> getParameters() {
    return null;
  }

  private static class ZetaSQLCastCallNotNullImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      if (rexCall.getOperands().size() != 1 || list.size() != 1) {
        throw new RuntimeException("CAST should have one operand.");
      }
      SqlTypeName toType = rexCall.getType().getSqlTypeName();
      SqlTypeName fromType = rexCall.getOperands().get(0).getType().getSqlTypeName();

      Expression translatedOperand = list.get(0);
      Expression convertedOperand;
      // CAST(BYTES AS STRING) - BINARY to VARCHAR in Calcite
      if (fromType == SqlTypeName.BINARY && toType == SqlTypeName.VARCHAR) {
        // operand is literal, which is bytes wrapped in ByteString.
        // this piece of code is same as
        // BeamCodegenUtils.toStringUTF8(ByeString.getBytes());
        convertedOperand =
            Expressions.call(
                BeamCodegenUtils.class,
                "toStringUTF8",
                Expressions.call(translatedOperand, "getBytes"));
      } else if (fromType == SqlTypeName.VARBINARY && toType == SqlTypeName.VARCHAR) {
        // translatedOperand is a byte[]
        // this piece of code is same as
        // BeamCodegenUtils.toStringUTF8(byte[]);
        convertedOperand =
            Expressions.call(BeamCodegenUtils.class, "toStringUTF8", translatedOperand);
      } else if (fromType == SqlTypeName.BOOLEAN && toType == SqlTypeName.BIGINT) {
        convertedOperand =
            Expressions.condition(
                translatedOperand,
                Expressions.constant(1L, Long.class),
                Expressions.constant(0L, Long.class));
      } else if (fromType == SqlTypeName.BIGINT && toType == SqlTypeName.BOOLEAN) {
        convertedOperand = Expressions.notEqual(translatedOperand, Expressions.constant(0));
      } else if (fromType == SqlTypeName.TIMESTAMP && toType == SqlTypeName.VARCHAR) {
        convertedOperand =
            Expressions.call(BeamCodegenUtils.class, "toStringTimestamp", translatedOperand);
      } else {
        throw new RuntimeException("Unsupported CAST: " + fromType.name() + " to " + toType.name());
      }

      // If operand is nullable, wrap in a null check
      if (rexCall.getOperands().get(0).getType().isNullable()) {
        convertedOperand =
            Expressions.condition(
                Expressions.equal(translatedOperand, RexImpTable.NULL_EXPR),
                RexImpTable.NULL_EXPR,
                convertedOperand);
      }

      return convertedOperand;
    }
  }
}
