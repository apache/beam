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
package org.apache.beam.sdk.extensions.sql.impl.cep;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;

/**
 * A {@code CEPCall} instance represents an operation (node) that contains an operator and a list of
 * operands. It has the similar functionality as Calcite's {@code RexCall}.
 */
public class CEPCall extends CEPOperation {

  private final CEPOperator operator;
  private final List<CEPOperation> operands;

  private CEPCall(CEPOperator operator, List<CEPOperation> operands) {
    this.operator = operator;
    this.operands = operands;
  }

  public CEPOperator getOperator() {
    return operator;
  }

  public List<CEPOperation> getOperands() {
    return operands;
  }

  public static CEPCall of(RexCall operation) {
    SqlOperator call = operation.getOperator();
    CEPOperator myOp = CEPOperator.of(call);

    ArrayList<CEPOperation> operandsList = new ArrayList<>();
    for (RexNode i : operation.getOperands()) {
      if (i.getClass() == RexCall.class) {
        CEPCall callToAdd = CEPCall.of((RexCall) i);
        operandsList.add(callToAdd);
      } else if (i.getClass() == RexLiteral.class) {
        RexLiteral lit = (RexLiteral) i;
        CEPLiteral litToAdd = CEPLiteral.of(lit);
        operandsList.add(litToAdd);
      } else if (i.getClass() == RexPatternFieldRef.class) {
        RexPatternFieldRef fieldRef = (RexPatternFieldRef) i;
        CEPFieldRef fieldRefToAdd = CEPFieldRef.of(fieldRef);
        operandsList.add(fieldRefToAdd);
      }
    }

    return new CEPCall(myOp, operandsList);
  }

  @Override
  public String toString() {
    ArrayList<String> operandStrings = new ArrayList<>();
    for (CEPOperation i : operands) {
      operandStrings.add(i.toString());
    }
    return operator.toString() + "(" + String.join(", ", operandStrings) + ")";
  }
}
