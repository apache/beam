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

import java.util.*;
import org.apache.beam.sdk.extensions.sql.impl.planner.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.*;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.*;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.*;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.*;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.*;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.*;

public class BeamWindowRel extends Window implements BeamRelNode {
  public BeamWindowRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<RexLiteral> constants,
      RelDataType rowType,
      List<Group> groups) {
    super(cluster, traitSet, input, constants, rowType, groups);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    // runs Beam transfoms for the window
    return null;
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {
    return null;
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return null;
  }
}
