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
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;

/**
 * {@link BeamRelNode} to replace a {@link Union}.
 *
 * <p>{@code BeamUnionRel} needs the input of it have the same {@link WindowFn}. From the SQL
 * perspective, two cases are supported:
 *
 * <p>1) Do not use {@code grouped window function}:
 *
 * <pre>{@code
 *   select * from person UNION select * from person
 * }</pre>
 *
 * <p>2) Use the same {@code grouped window function}, with the same param:
 * <pre>{@code
 *   select id, count(*) from person
 *   group by id, TUMBLE(order_time, INTERVAL '1' HOUR)
 *   UNION
 *   select * from person
 *   group by id, TUMBLE(order_time, INTERVAL '1' HOUR)
 * }</pre>
 *
 * <p>Inputs with different group functions are NOT supported:
 * <pre>{@code
 *   select id, count(*) from person
 *   group by id, TUMBLE(order_time, INTERVAL '1' HOUR)
 *   UNION
 *   select * from person
 *   group by id, TUMBLE(order_time, INTERVAL '2' HOUR)
 * }</pre>
 */
public class BeamUnionRel extends Union implements BeamRelNode {
  private BeamSetOperatorRelBase delegate;
  public BeamUnionRel(RelOptCluster cluster,
      RelTraitSet traits,
      List<RelNode> inputs,
      boolean all) {
    super(cluster, traits, inputs, all);
    this.delegate = new BeamSetOperatorRelBase(this,
        BeamSetOperatorRelBase.OpType.UNION,
        inputs, all);
  }

  @Override public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new BeamUnionRel(getCluster(), traitSet, inputs, all);
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
