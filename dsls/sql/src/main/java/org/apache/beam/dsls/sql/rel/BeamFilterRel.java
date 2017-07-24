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

import org.apache.beam.dsls.sql.BeamSqlEnv;
import org.apache.beam.dsls.sql.interpreter.BeamSqlExpressionExecutor;
import org.apache.beam.dsls.sql.interpreter.BeamSqlFnExecutor;
import org.apache.beam.dsls.sql.transform.BeamSqlFilterFn;
import org.apache.beam.dsls.sql.utils.CalciteUtils;
import org.apache.beam.sdk.sd.BeamRow;
import org.apache.beam.sdk.sd.BeamRowCoder;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

/**
 * BeamRelNode to replace a {@code Filter} node.
 *
 */
public class BeamFilterRel extends Filter implements BeamRelNode {

  public BeamFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child,
      RexNode condition) {
    super(cluster, traits, child, condition);
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new BeamFilterRel(getCluster(), traitSet, input, condition);
  }

  @Override
  public PCollection<BeamRow> buildBeamPipeline(PCollectionTuple inputPCollections
      , BeamSqlEnv sqlEnv) throws Exception {
    RelNode input = getInput();
    String stageName = BeamSqlRelUtils.getStageName(this);

    PCollection<BeamRow> upstream =
        BeamSqlRelUtils.getBeamRelInput(input).buildBeamPipeline(inputPCollections, sqlEnv);

    BeamSqlExpressionExecutor executor = new BeamSqlFnExecutor(this);

    PCollection<BeamRow> filterStream = upstream.apply(stageName,
        ParDo.of(new BeamSqlFilterFn(getRelTypeName(), executor)));
    filterStream.setCoder(new BeamRowCoder(CalciteUtils.toBeamRowType(getRowType())));

    return filterStream;
  }

}
