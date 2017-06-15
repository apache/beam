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
package org.apache.beam.dsls.sql.planner;

import java.util.Map;
import org.apache.beam.dsls.sql.rel.BeamRelNode;
import org.apache.beam.dsls.sql.schema.BaseBeamTable;
import org.apache.beam.sdk.Pipeline;

/**
 * {@link BeamPipelineCreator} converts a {@link BeamRelNode} tree, into a Beam
 * pipeline.
 *
 */
class BeamPipelineCreator {
  private Map<String, BaseBeamTable> sourceTables;

  private Pipeline pipeline;

  private boolean hasPersistent = false;

  public BeamPipelineCreator(Map<String, BaseBeamTable> sourceTables, Pipeline basePipeline) {
    this.sourceTables = sourceTables;
    this.pipeline = basePipeline;
  }

  public Map<String, BaseBeamTable> getSourceTables() {
    return sourceTables;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public boolean hasPersistent() {
    return hasPersistent;
  }

  public void setHasPersistent(boolean hasPersistent) {
    this.hasPersistent = hasPersistent;
  }

}
