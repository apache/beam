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
package org.apache.beam.dsls.sql.interpreter;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.dsls.sql.exception.BeamSqlUnsupportedException;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlAndExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlEqualExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlInputRefExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlIsNotNullExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlIsNullExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlLargerThanEqualExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlLargerThanExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlLessThanEqualExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlLessThanExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlNotEqualExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlOrExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.dsls.sql.rel.BeamFilterRel;
import org.apache.beam.dsls.sql.rel.BeamProjectRel;
import org.apache.beam.dsls.sql.rel.BeamRelNode;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

/**
 * Executor based on {@link BeamSqlExpression} and {@link BeamSqlPrimitive}.
 * {@code BeamSQLFnExecutor} converts a {@link BeamRelNode} to a {@link BeamSqlExpression},
 * which can be evaluated against the {@link BeamSQLRow}.
 *
 */
public class BeamSQLFnExecutor implements BeamSQLExpressionExecutor {
  protected List<BeamSqlExpression> exps;

  public BeamSQLFnExecutor(BeamRelNode relNode) {
    this.exps = new ArrayList<>();
    if (relNode instanceof BeamFilterRel) {
      BeamFilterRel filterNode = (BeamFilterRel) relNode;
      RexNode condition = filterNode.getCondition();
      exps.add(buildExpression(condition));
    } else if (relNode instanceof BeamProjectRel) {
      BeamProjectRel projectNode = (BeamProjectRel) relNode;
      List<RexNode> projects = projectNode.getProjects();
      for (RexNode rexNode : projects) {
        exps.add(buildExpression(rexNode));
      }
    } else {
      throw new BeamSqlUnsupportedException(
          String.format("%s is not supported yet", relNode.getClass().toString()));
    }
  }

  /**
   * {@link #buildExpression(RexNode)} visits the operands of {@link RexNode} recursively,
   * and represent each {@link SqlOperator} with a corresponding {@link BeamSqlExpression}.
   */
  private BeamSqlExpression buildExpression(RexNode rexNode) {
    if (rexNode instanceof RexLiteral) {
      RexLiteral node = (RexLiteral) rexNode;
      return BeamSqlPrimitive.of(node.getTypeName(), node.getValue());
    } else if (rexNode instanceof RexInputRef) {
      RexInputRef node = (RexInputRef) rexNode;
      return new BeamSqlInputRefExpression(node.getType().getSqlTypeName(), node.getIndex());
    } else if (rexNode instanceof RexCall) {
      RexCall node = (RexCall) rexNode;
      String opName = node.op.getName();
      List<BeamSqlExpression> subExps = new ArrayList<>();
      for (RexNode subNode : node.operands) {
        subExps.add(buildExpression(subNode));
      }
      switch (opName) {
        case "AND":
        return new BeamSqlAndExpression(subExps);
        case "OR":
          return new BeamSqlOrExpression(subExps);

        case "=":
          return new BeamSqlEqualExpression(subExps);
        case "<>=":
          return new BeamSqlNotEqualExpression(subExps);
        case ">":
          return new BeamSqlLargerThanExpression(subExps);
        case ">=":
          return new BeamSqlLargerThanEqualExpression(subExps);
        case "<":
          return new BeamSqlLessThanExpression(subExps);
        case "<=":
          return new BeamSqlLessThanEqualExpression(subExps);

        case "IS NULL":
          return new BeamSqlIsNullExpression(subExps.get(0));
        case "IS NOT NULL":
          return new BeamSqlIsNotNullExpression(subExps.get(0));
      default:
        throw new BeamSqlUnsupportedException();
      }
    } else {
      throw new BeamSqlUnsupportedException(
          String.format("%s is not supported yet", rexNode.getClass().toString()));
    }
  }

  @Override
  public void prepare() {
  }

  @Override
  public List<Object> execute(BeamSQLRow inputRecord) {
    List<Object> results = new ArrayList<>();
    for (BeamSqlExpression exp : exps) {
      results.add(exp.evaluate(inputRecord).getValue());
    }
    return results;
  }

  @Override
  public void close() {
  }

}
