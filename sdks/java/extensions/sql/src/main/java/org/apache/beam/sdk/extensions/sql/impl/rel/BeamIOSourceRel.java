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

import java.util.Map;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;

/** BeamRelNode to replace a {@code TableScan} node. */
public class BeamIOSourceRel extends TableScan implements BeamRelNode {

  private final BeamSqlTable sqlTable;
  private final Map<String, String> pipelineOptions;

  public BeamIOSourceRel(
      RelOptCluster cluster,
      RelOptTable table,
      BeamSqlTable sqlTable,
      Map<String, String> pipelineOptions) {
    super(cluster, cluster.traitSetOf(BeamLogicalConvention.INSTANCE), table);
    this.sqlTable = sqlTable;
    this.pipelineOptions = pipelineOptions;
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return sqlTable.isBounded();
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new Transform();
  }

  private class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionList<Row> input) {
      checkArgument(
          input.size() == 0,
          "Should not have received input for %s: %s",
          BeamIOSourceRel.class.getSimpleName(),
          input);
      return sqlTable.buildIOReader(input.getPipeline().begin());
    }
  }

  protected BeamSqlTable getBeamSqlTable() {
    return sqlTable;
  }

  @Override
  public Map<String, String> getPipelineOptions() {
    return pipelineOptions;
  }
}
