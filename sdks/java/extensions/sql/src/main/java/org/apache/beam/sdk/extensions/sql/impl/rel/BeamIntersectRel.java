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

package org.apache.beam.sdk.extensions.sql.impl.rel;

import java.util.List;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.SetOp;

/**
 * {@code BeamRelNode} to replace a {@code Intersect} node.
 *
 * <p>This is used to combine two SELECT statements, but returns rows only from the first SELECT
 * statement that are identical to a row in the second SELECT statement.
 */
public class BeamIntersectRel extends Intersect implements BeamRelNode {
  private BeamSetOperatorRelBase delegate;

  public BeamIntersectRel(
      RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
    super(cluster, traits, inputs, all);
    delegate =
        new BeamSetOperatorRelBase(this, BeamSetOperatorRelBase.OpType.INTERSECT, inputs, all);
  }

  @Override
  public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new BeamIntersectRel(getCluster(), traitSet, inputs, all);
  }

  @Override
  public PTransform<PCollectionTuple, PCollection<Row>> toPTransform() {
    return new Transform();
  }

  private class Transform extends PTransform<PCollectionTuple, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionTuple inputPCollections) {
      return delegate.buildBeamPipeline(inputPCollections);
    }
  }
}
