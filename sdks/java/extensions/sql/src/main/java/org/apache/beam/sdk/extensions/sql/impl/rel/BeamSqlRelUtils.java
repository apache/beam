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
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.volcano.RelSubset;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utilities for {@code BeamRelNode}. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class BeamSqlRelUtils {

  public static final String ROW = "row";
  public static final String ERROR = "error";

  public static PCollection<Row> toPCollection(Pipeline pipeline, BeamRelNode node) {
    return toPCollection(pipeline, node, null, new HashMap(), new HashMap<>());
  }

  public static PCollection<Row> toPCollection(
      Pipeline pipeline,
      BeamRelNode node,
      @Nullable PTransform<PCollection<Row>, ? extends POutput> errorTransformer) {
    return toPCollection(pipeline, node, errorTransformer, new HashMap(), new HashMap<>());
  }

  /** Transforms the inputs into a PInput. */
  private static PCollectionList<Row> buildPCollectionList(
      List<RelNode> inputRels,
      Pipeline pipeline,
      @Nullable PTransform<PCollection<Row>, ? extends POutput> errorTransformer,
      Map<Integer, PCollection<Row>> cache,
      Map<String, Integer> usedNames) {
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
                        pipeline, beamRel, errorTransformer, cache, usedNames);
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
      @Nullable PTransform<PCollection<Row>, ? extends POutput> errorTransformer,
      Map<Integer, PCollection<Row>> cache,
      Map<String, Integer> usedNames) {
    PCollection<Row> output = cache.get(node.getId());
    if (output != null) {
      return output;
    }

    String name = uniqueName(usedNames, transformName(pipeline, node));
    PCollectionList<Row> input =
        buildPCollectionList(
            node.getPCollectionInputs(), pipeline, errorTransformer, cache, usedNames);
    PTransform<PCollectionList<Row>, PCollection<Row>> transform =
        node.buildPTransform(errorTransformer);
    output = Pipeline.applyTransform(name, input, transform);

    cache.put(node.getId(), output);
    return output;
  }

  /**
   * Names the composite that {@code node} expands into after the stage it was composed from, or
   * after the node's own type when nothing composed it.
   */
  private static String transformName(Pipeline pipeline, BeamRelNode node) {
    if (ExperimentalOptions.hasExperiment(pipeline.getOptions(), StageName.LEGACY_EXPERIMENT)) {
      return node.getClass().getSimpleName() + "_" + node.getId();
    }
    String label = StageName.renderedName(node);
    return label == null || label.isEmpty() ? node.getClass().getSimpleName() : label;
  }

  /**
   * Disambiguates repeated names in DFS order, within the plan being expanded.
   *
   * <p>Names no longer carry a rel id, so a plan can legitimately contain two stages with the same
   * provenance. {@link Pipeline} would uniquify them itself, but then reports the pipeline as not
   * having stable unique names, which is fatal under {@code --stableUniqueNames=ERROR}.
   *
   * <p>Counting per plan rather than per pipeline is what makes the numbering stable: every caller
   * expands a plan either into a fresh pipeline or inside {@code SqlTransform}'s own composite, so
   * names only have to be unique among the stages of one query. A counter shared across a pipeline
   * would let an unrelated query added elsewhere renumber stages that did not themselves change.
   */
  private static String uniqueName(Map<String, Integer> usedNames, String name) {
    int occurrence = usedNames.merge(name, 1, Integer::sum);
    return occurrence == 1 ? name : name + " #" + occurrence;
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

  public static Schema getErrorRowSchema(Schema upstreamSchema) {
    return Schema.of(
        Schema.Field.of(ROW, Schema.FieldType.row(upstreamSchema)),
        Schema.Field.of(ERROR, Schema.FieldType.STRING));
  }

  /** A lazy explain via {@link #toString()} for logging purposes. */
  public static Object explainLazily(final RelNode node) {
    return new Object() {
      @Override
      public String toString() {
        return RelOptUtil.toString(node);
      }
    };
  }
}
