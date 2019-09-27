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

import static org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamSetOperatorsTransforms;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Delegate for Set operators: {@code BeamUnionRel}, {@code BeamIntersectRel} and {@code
 * BeamMinusRel}.
 */
public class BeamSetOperatorRelBase extends PTransform<PCollectionList<Row>, PCollection<Row>> {
  /** Set operator type. */
  public enum OpType implements Serializable {
    UNION,
    INTERSECT,
    MINUS
  }

  private BeamRelNode beamRelNode;
  private boolean all;
  private OpType opType;

  public BeamSetOperatorRelBase(BeamRelNode beamRelNode, OpType opType, boolean all) {
    this.beamRelNode = beamRelNode;
    this.opType = opType;
    this.all = all;
  }

  @Override
  public PCollection<Row> expand(PCollectionList<Row> inputs) {
    checkArgument(
        inputs.size() == 2,
        "Wrong number of arguments to %s: %s",
        beamRelNode.getClass().getSimpleName(),
        inputs);
    PCollection<Row> leftRows = inputs.get(0);
    PCollection<Row> rightRows = inputs.get(1);

    WindowFn leftWindow = leftRows.getWindowingStrategy().getWindowFn();
    WindowFn rightWindow = rightRows.getWindowingStrategy().getWindowFn();
    if (!leftWindow.isCompatible(rightWindow)) {
      throw new IllegalArgumentException(
          "inputs of "
              + opType
              + " have different window strategy: "
              + leftWindow
              + " VS "
              + rightWindow);
    }

    final TupleTag<Row> leftTag = new TupleTag<>();
    final TupleTag<Row> rightTag = new TupleTag<>();

    // co-group
    PCollection<KV<Row, CoGbkResult>> coGbkResultCollection =
        KeyedPCollectionTuple.of(
                leftTag,
                leftRows.apply(
                    "CreateLeftIndex",
                    MapElements.via(new BeamSetOperatorsTransforms.BeamSqlRow2KvFn())))
            .and(
                rightTag,
                rightRows.apply(
                    "CreateRightIndex",
                    MapElements.via(new BeamSetOperatorsTransforms.BeamSqlRow2KvFn())))
            .apply(CoGroupByKey.create());
    return coGbkResultCollection.apply(
        ParDo.of(
            new BeamSetOperatorsTransforms.SetOperatorFilteringDoFn(
                leftTag, rightTag, opType, all)));
  }
}
