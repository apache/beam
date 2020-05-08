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
package org.apache.beam.sdk.extensions.sql.impl.rule;

import static org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamTableFunctionScanRel;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.convert.ConverterRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalTableFunctionScan;

/**
 * This is the conveter rule that converts a Calcite {@code TableFunctionScan} to Beam {@code
 * TableFunctionScanRel}.
 */
public class BeamTableFunctionScanRule extends ConverterRule {
  public static final BeamTableFunctionScanRule INSTANCE = new BeamTableFunctionScanRule();

  private BeamTableFunctionScanRule() {
    super(
        LogicalTableFunctionScan.class,
        Convention.NONE,
        BeamLogicalConvention.INSTANCE,
        "BeamTableFunctionScanRule");
  }

  @Override
  public RelNode convert(RelNode relNode) {
    TableFunctionScan tableFunctionScan = (TableFunctionScan) relNode;
    // only support one input for table function scan.
    List<RelNode> inputs = new ArrayList<>();
    checkArgument(
        relNode.getInputs().size() == 1,
        "Wrong number of inputs for %s, expected 1 input but received: %s",
        BeamTableFunctionScanRel.class.getSimpleName(),
        relNode.getInputs().size());
    inputs.add(
        convert(
            relNode.getInput(0),
            relNode.getInput(0).getTraitSet().replace(BeamLogicalConvention.INSTANCE)));
    return new BeamTableFunctionScanRel(
        tableFunctionScan.getCluster(),
        tableFunctionScan.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        inputs,
        tableFunctionScan.getCall(),
        null,
        tableFunctionScan.getCall().getType(),
        Collections.EMPTY_SET);
  }
}
