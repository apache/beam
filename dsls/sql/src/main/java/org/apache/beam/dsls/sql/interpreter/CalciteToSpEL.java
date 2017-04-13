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

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.dsls.sql.planner.BeamSqlUnsupportedException;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

/**
 * {@code CalciteToSpEL} is used in {@link BeamSQLSpELExecutor}, to convert a
 * relational expression {@link RexCall} to SpEL expression.
 *
 */
public class CalciteToSpEL {

  public static String rexcall2SpEL(RexCall cdn) {
    List<String> parts = new ArrayList<>();
    for (RexNode subcdn : cdn.operands) {
      if (subcdn instanceof RexCall) {
        parts.add(rexcall2SpEL((RexCall) subcdn));
      } else {
        parts.add(subcdn instanceof RexInputRef
            ? "#in.getFieldValue(" + ((RexInputRef) subcdn).getIndex() + ")" : subcdn.toString());
      }
    }

    String opName = cdn.op.getName();
    switch (cdn.op.getClass().getSimpleName()) {
    case "SqlMonotonicBinaryOperator": // +-*
    case "SqlBinaryOperator": // > < = >= <= <> OR AND || / .
      switch (cdn.op.getName().toUpperCase()) {
      case "AND":
        return String.format(" ( %s ) ", Joiner.on("&&").join(parts));
      case "OR":
        return String.format(" ( %s ) ", Joiner.on("||").join(parts));
      case "=":
        return String.format(" ( %s ) ", Joiner.on("==").join(parts));
      case "<>":
        return String.format(" ( %s ) ", Joiner.on("!=").join(parts));
      default:
        return String.format(" ( %s ) ", Joiner.on(cdn.op.getName().toUpperCase()).join(parts));
      }
    case "SqlCaseOperator": // CASE
      return String.format(" (%s ? %s : %s)", parts.get(0), parts.get(1), parts.get(2));
    case "SqlCastFunction": // CAST
      return parts.get(0);
    case "SqlPostfixOperator":
      switch (opName.toUpperCase()) {
      case "IS NULL":
        return String.format(" null == %s ", parts.get(0));
      case "IS NOT NULL":
        return String.format(" null != %s ", parts.get(0));
      default:
        throw new BeamSqlUnsupportedException();
      }
    default:
      throw new BeamSqlUnsupportedException();
    }
  }

}
