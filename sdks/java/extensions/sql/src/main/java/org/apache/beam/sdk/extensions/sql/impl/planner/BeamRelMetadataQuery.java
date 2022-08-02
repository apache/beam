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
package org.apache.beam.sdk.extensions.sql.impl.planner;

import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.metadata.RelMetadataQuery;

public class BeamRelMetadataQuery extends RelMetadataQuery {
  private NodeStatsMetadata.Handler nodeStatsMetadataHandler;

  private BeamRelMetadataQuery() {
    nodeStatsMetadataHandler = initialHandler(NodeStatsMetadata.Handler.class);
  }

  public static BeamRelMetadataQuery instance() {
    return new BeamRelMetadataQuery();
  }

  public NodeStats getNodeStats(RelNode relNode) {
    // Note this infinite loop was duplicated from logic in Calcite's RelMetadataQuery.get* methods
    for (; ; ) {
      try {
        NodeStats result = nodeStatsMetadataHandler.getNodeStats(relNode, this);
        return result;
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        nodeStatsMetadataHandler = revise(e.relClass, NodeStatsMetadata.DEF);
      }
    }
  }
}
