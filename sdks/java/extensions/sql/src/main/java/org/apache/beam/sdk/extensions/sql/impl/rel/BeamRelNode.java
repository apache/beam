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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.rel.RelNode;

/** A {@link RelNode} that can also give a {@link PTransform} that implements the expression. */
public interface BeamRelNode extends RelNode {

  /**
   * Whether the collection of rows represented by this relational expression is bounded (known to
   * be finite) or unbounded (may or may not be finite).
   *
   * @return bounded if and only if all PCollection inputs are bounded
   */
  default PCollection.IsBounded isBounded() {
    return getPCollectionInputs().stream()
            .allMatch(
                rel ->
                    BeamSqlRelUtils.getBeamRelInput(rel).isBounded()
                        == PCollection.IsBounded.BOUNDED)
        ? PCollection.IsBounded.BOUNDED
        : PCollection.IsBounded.UNBOUNDED;
  }

  default List<RelNode> getPCollectionInputs() {
    return getInputs();
  };

  PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform();

  /** Perform a DFS(Depth-First-Search) to find the PipelineOptions config. */
  default PipelineOptions getPipelineOptions() {
    PipelineOptions options = null;
    for (RelNode input : getInputs()) {
      PipelineOptions inputOptions = ((BeamRelNode) input).getPipelineOptions();
      assert inputOptions != null;
      assert options == null || options == inputOptions;
      options = inputOptions;
    }
    return options;
  }
}
