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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelMetadataQuery;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.volcano.RelSubset;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utilities for {@code BeamRelNode}. */
@SuppressWarnings({
  "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
public class BeamSqlRelUtils {

  public static PCollection<Row> toPCollection(Pipeline pipeline, BeamRelNode node) {
    return toPCollection(pipeline, node, null, new HashMap());
  }

  public static PCollection<Row> toPCollection(
      Pipeline pipeline,
      BeamRelNode node,
      @Nullable PTransform<PCollection<BeamCalcRelError>, POutput> errorTransformer) {
    return toPCollection(pipeline, node, errorTransformer, new HashMap());
  }

  /** Transforms the inputs into a PInput. */
  private static PCollectionList<Row> buildPCollectionList(
      List<RelNode> inputRels,
      Pipeline pipeline,
      @Nullable PTransform<PCollection<BeamCalcRelError>, POutput> errorTransformer,
      Map<Integer, PCollection<Row>> cache) {
    if (inputRels.isEmpty()) {
      return PCollectionList.empty(pipeline);
    } else {
      return PCollectionList.of(
          inputRels.stream()
              .map(
                  input -> {
                    final BeamRelNode beamRel;
                    if (input instanceof RelSubset) {
                      beamRel =
                          Preconditions.checkArgumentNotNull(
                              (BeamRelNode) ((RelSubset) input).getBest(),
                              "Attempted to build PCollection from unoptimized RelSubset (best is null).");
                    } else {
                      beamRel = (BeamRelNode) input;
                    }
                    return BeamSqlRelUtils.toPCollection(
                        pipeline, beamRel, errorTransformer, cache);
                  })
              .collect(Collectors.toList()));
    }
  }

  /**
   * A {@link BeamRelNode} is a recursive structure, the {@code BeamQueryPlanner} visits it with a
   * DFS(Depth-First-Search) algorithm.
   */
  static PCollection<Row> toPCollection(
      Pipeline pipeline,
      BeamRelNode node,
      @Nullable PTransform<PCollection<BeamCalcRelError>, POutput> errorTransformer,
      Map<Integer, PCollection<Row>> cache) {
    PCollection<Row> output = cache.get(node.getId());
    if (output != null) {
      return output;
    }

    String name = node.getClass().getSimpleName() + "_" + node.getId();
    PCollectionList<Row> input =
        buildPCollectionList(node.getPCollectionInputs(), pipeline, errorTransformer, cache);
    node.withErrorsTransformer(errorTransformer);
    PTransform<PCollectionList<Row>, PCollection<Row>> transform = node.buildPTransform();
    output = Pipeline.applyTransform(name, input, transform);

    cache.put(node.getId(), output);
    return output;
  }

  public static BeamRelNode getBeamRelInput(RelNode input) {
    if (input instanceof RelSubset) {
      // go with known best input
      input =
          Preconditions.checkArgumentNotNull(
              ((RelSubset) input).getBest(), "input RelSubset has no best.");
    }
    return (BeamRelNode) input;
  }

  public static RelNode getInput(RelNode input) {
    RelNode result = input;
    if (input instanceof RelSubset) {
      // prefer known best input
      result = ((RelSubset) input).getBest();
      if (result == null) {
        result =
            Preconditions.checkArgumentNotNull(
                ((RelSubset) input).getOriginal(),
                "best and original nodes are both null for input RelSubset.");
      }
    }

    return result;
  }

  public static NodeStats getNodeStats(RelNode input, BeamRelMetadataQuery mq) {
    input = getInput(input);
    return mq.getNodeStats(input);
  }
}
