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

import java.io.Serializable;
import java.util.List;

import org.apache.beam.dsls.sql.planner.BeamSQLRelUtils;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.dsls.sql.transform.BeamSetOperatorsTransforms;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.calcite.rel.RelNode;

/**
 * Delegate for Set operators: {@code BeamUnionRel}, {@code BeamIntersectRel}
 * and {@code BeamMinusRel}.
 */
public class BeamSetOperatorRelBase {
  /**
   * Set operator type.
   */
  public enum OpType implements Serializable {
    UNION,
    INTERSECT,
    MINUS
  }

  private BeamRelNode beamRelNode;
  private List<RelNode> inputs;
  private boolean all;
  private OpType opType;

  public BeamSetOperatorRelBase(BeamRelNode beamRelNode, OpType opType,
      List<RelNode> inputs, boolean all) {
    this.beamRelNode = beamRelNode;
    this.opType = opType;
    this.inputs = inputs;
    this.all = all;
  }

  public PCollection<BeamSQLRow> buildBeamPipeline(
      PCollectionTuple inputPCollections) throws Exception {
    PCollection<BeamSQLRow> leftRows = BeamSQLRelUtils.getBeamRelInput(inputs.get(0))
        .buildBeamPipeline(inputPCollections);
    PCollection<BeamSQLRow> rightRows = BeamSQLRelUtils.getBeamRelInput(inputs.get(1))
        .buildBeamPipeline(inputPCollections);

    WindowFn leftWindow = leftRows.getWindowingStrategy().getWindowFn();
    WindowFn rightWindow = rightRows.getWindowingStrategy().getWindowFn();
    if (!leftWindow.isCompatible(rightWindow)) {
      throw new IllegalArgumentException(
          "inputs of " + opType + " have different window strategy: "
          + leftWindow + " VS " + rightWindow);
    }

    final TupleTag<BeamSQLRow> leftTag = new TupleTag<>();
    final TupleTag<BeamSQLRow> rightTag = new TupleTag<>();

    // co-group
    String stageName = BeamSQLRelUtils.getStageName(beamRelNode);
    PCollection<KV<BeamSQLRow, CoGbkResult>> coGbkResultCollection = KeyedPCollectionTuple
        .of(leftTag, leftRows.apply(
            stageName + "_CreateLeftIndex", MapElements.via(
                new BeamSetOperatorsTransforms.BeamSQLRow2KvFn())))
        .and(rightTag, rightRows.apply(
            stageName + "_CreateRightIndex", MapElements.via(
                new BeamSetOperatorsTransforms.BeamSQLRow2KvFn())))
        .apply(CoGroupByKey.<BeamSQLRow>create());
    PCollection<BeamSQLRow> ret = coGbkResultCollection
        .apply(ParDo.of(new BeamSetOperatorsTransforms.SetOperatorFilteringDoFn(leftTag, rightTag,
            opType, all)));
    return ret;
  }
}
