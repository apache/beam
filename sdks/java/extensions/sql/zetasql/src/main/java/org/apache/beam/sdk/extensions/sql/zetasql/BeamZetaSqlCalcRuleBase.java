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

import java.util.List;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTrait;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.convert.ConverterRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.validate.SqlUserDefinedFunction;

public abstract class BeamZetaSqlCalcRuleBase extends ConverterRule {

  public BeamZetaSqlCalcRuleBase(
      Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String description) {
    super(clazz, in, out, description);
  }

  protected boolean hasUdfInProjects(RelOptRuleCall x) {
    List<RelNode> resList = x.getRelList();
    for (RelNode relNode : resList) {
      if (relNode instanceof LogicalCalc) {
        LogicalCalc logicalCalc = (LogicalCalc) relNode;
        for (RexNode rexNode : logicalCalc.getProgram().getExprList()) {
          if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            if (call.getOperator() instanceof SqlUserDefinedFunction) return true;
          }
        }
      }
    }

    return false;
  }
}
