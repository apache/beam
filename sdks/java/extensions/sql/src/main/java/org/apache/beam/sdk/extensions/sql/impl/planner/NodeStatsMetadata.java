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

import java.lang.reflect.Method;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.tree.Types;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.Metadata;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * This is a metadata used for row count and rate estimation. It extends Calcite's Metadata
 * interface so that we can use MetadataQuery to get our own estimates.
 */
public interface NodeStatsMetadata extends Metadata {
  Method METHOD = Types.lookupMethod(NodeStatsMetadata.class, "getNodeStats");

  MetadataDef<NodeStatsMetadata> DEF =
      MetadataDef.of(NodeStatsMetadata.class, NodeStatsMetadata.Handler.class, METHOD);

  // In order to use this we need to call it by relNode.metadata(NodeStatsMetadata.class,
  // mq).getNodeStats() where mq is the MetadataQuery (can be obtained by
  // relNode.getCluster().getMetadataQuery()). After this, Calcite looks for the implementation of
  // this metadata that we have registered in MetadataProvider (it is RelMdNodeStats.class in
  // this case and we have registered it in CalciteQueryPlanner). Then Calcite's generated Code
  // decides the type of the rel node and calls appropriate method in RelMdNodeStats.
  // For instance: Join is a subclass of RelNode and if we have both getNodeStats(RelNode rel,
  // RelMetadataQuery mq) and getNodeStats(Join rel, RelMetadataQuery mq) then if the rel is an
  // instance of Join it will call getNodeStats((Join) rel, mq).
  // Currently we only register it in SQLTransform path. JDBC does not register this and it does not
  // use it. (because it does not register the our NonCumulativeMetadata implementation either).
  NodeStats getNodeStats();

  /** Handler API. */
  interface Handler extends MetadataHandler<NodeStatsMetadata> {
    NodeStats getNodeStats(RelNode r, RelMetadataQuery mq);
  }
}
