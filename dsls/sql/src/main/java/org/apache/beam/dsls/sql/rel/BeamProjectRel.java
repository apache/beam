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
package org.apache.beam.dsls.sql.rel;

import java.util.List;

import org.apache.beam.dsls.sql.interpreter.BeamSQLExpressionExecutor;
import org.apache.beam.dsls.sql.interpreter.BeamSQLFnExecutor;
import org.apache.beam.dsls.sql.planner.BeamPipelineCreator;
import org.apache.beam.dsls.sql.planner.BeamSQLRelUtils;
import org.apache.beam.dsls.sql.schema.BeamSQLRecordType;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.dsls.sql.transform.BeamSQLProjectFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

/**
 * BeamRelNode to replace a {@code Project} node.
 *
 */
public class BeamProjectRel extends Project implements BeamRelNode {

  /**
   * projects: {@link RexLiteral}, {@link RexInputRef}, {@link RexCall}.
   *
   */
  public BeamProjectRel(RelOptCluster cluster, RelTraitSet traits, RelNode input,
      List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traits, input, projects, rowType);
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects,
      RelDataType rowType) {
    return new BeamProjectRel(getCluster(), traitSet, input, projects, rowType);
  }

  @Override
  public Pipeline buildBeamPipeline(BeamPipelineCreator planCreator) throws Exception {
    RelNode input = getInput();
    BeamSQLRelUtils.getBeamRelInput(input).buildBeamPipeline(planCreator);

    String stageName = BeamSQLRelUtils.getStageName(this);

    PCollection<BeamSQLRow> upstream = planCreator.popUpstream();

    BeamSQLExpressionExecutor executor = new BeamSQLFnExecutor(this);

    PCollection<BeamSQLRow> projectStream = upstream.apply(stageName, ParDo
        .of(new BeamSQLProjectFn(getRelTypeName(), executor, BeamSQLRecordType.from(rowType))));

    planCreator.pushUpstream(projectStream);

    return planCreator.getPipeline();

  }

}
