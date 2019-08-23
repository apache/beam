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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
        inputs.size() >= 2,
        "Wrong number of arguments to %s: %s",
        beamRelNode.getClass().getSimpleName(),
        inputs);

    if(!areWinCompatible(inputs)){
      throw new IllegalArgumentException(
              "inputs of "
                      + opType
                      + " have different window strategy: " );
    }

      // co-group
    List<TupleTag<Row>> tagList = new ArrayList<>();
    tagList.add(new TupleTag());

    KeyedPCollectionTuple kPCollection = KeyedPCollectionTuple.of( tagList.get(0),
            inputs.get(0).apply(
                    "CreateIndexNo_"+0,
                    MapElements.via(new BeamSetOperatorsTransforms.BeamSqlRow2KvFn())));

    for(int i=1;i< inputs.size();i++){
      tagList.add(new TupleTag());
      kPCollection = kPCollection.and(
              tagList.get(i),
              inputs.get(i).apply(
                      "CreateIndexNo_"+i,
                      MapElements.via(new BeamSetOperatorsTransforms.BeamSqlRow2KvFn())));
    }

    PCollection<KV<Row, CoGbkResult>> coGbkResultCollection =
            (PCollection<KV<Row, CoGbkResult>> )kPCollection.apply(CoGroupByKey.create());

    return coGbkResultCollection.apply(
            ParDo.of(
                    new BeamSetOperatorsTransforms.SetOperatorFilteringDoFn(
                            tagList, opType, all)));
  }

  private boolean areWinCompatible(PCollectionList<Row> inputs){

    for(int i= 0;  i < inputs.size();i++){

      if(i == inputs.size()-1) {
        WindowFn leftWindow = inputs.get(i).getWindowingStrategy().getWindowFn();
        WindowFn rightWindow = inputs.get(0).getWindowingStrategy().getWindowFn();

        if (!leftWindow.isCompatible(rightWindow) || !rightWindow.isCompatible(leftWindow))
          return false;
      }
      else
      {
        WindowFn leftWindow = inputs.get(i).getWindowingStrategy().getWindowFn();
        WindowFn rightWindow = inputs.get(i+1).getWindowingStrategy().getWindowFn();

        if (!leftWindow.isCompatible(rightWindow) || !rightWindow.isCompatible(leftWindow))
          return false;
      }


    }

    return true;
  }
}
