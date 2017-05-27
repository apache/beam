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

import org.apache.beam.dsls.sql.planner.BeamPipelineCreator;
import org.apache.beam.sdk.Pipeline;
import org.apache.calcite.rel.RelNode;

/**
 * A new method {@link #buildBeamPipeline(BeamPipelineCreator)} is added, it's
 * called by {@link BeamPipelineCreator}.
 *
 */
public interface BeamRelNode extends RelNode {

  /**
   * A {@link BeamRelNode} is a recursive structure, the
   * {@link BeamPipelineCreator} visits it with a DFS(Depth-First-Search)
   * algorithm.
   *
   */
  Pipeline buildBeamPipeline(BeamPipelineCreator planCreator) throws Exception;
}
