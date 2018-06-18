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
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;

/** Utilities for {@code BeamRelNode}. */
public class BeamSqlRelUtils {

  public static PCollection<Row> toPCollection(Pipeline pipeline, BeamRelNode node) {
    return toPCollection(pipeline, node, new HashMap());
  }

  /**
   * A {@link BeamRelNode} is a recursive structure, the {@code BeamQueryPlanner} visits it with a
   * DFS(Depth-First-Search) algorithm.
   */
  static PCollection<Row> toPCollection(
      Pipeline pipeline, BeamRelNode node, Map<Integer, PCollection<Row>> cache) {
    PCollection<Row> output = cache.get(node.getId());
    if (output != null) {
      return output;
    }

    String name = node.getClass().getSimpleName() + "_" + node.getId();
    PInput input = node.buildPInput(pipeline, cache);
    PTransform<PInput, PCollection<Row>> transform = node.buildPTransform();

    output = Pipeline.applyTransform(name, input, transform);

    cache.put(node.getId(), output);
    return output;
  }

  public static BeamRelNode getBeamRelInput(RelNode input) {
    if (input instanceof RelSubset) {
      // go with known best input
      input = ((RelSubset) input).getBest();
    }
    return (BeamRelNode) input;
  }
}
