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

import com.google.common.base.Joiner;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;

/**
 * BeamRelNode to replace a {@code TableScan} node.
 *
 */
public class BeamIOSourceRel extends TableScan implements BeamRelNode {

  private BeamSqlTable sqlTable;

  public BeamIOSourceRel(
      RelOptCluster cluster, RelOptTable table, BeamSqlTable sqlTable) {
    super(cluster, cluster.traitSetOf(BeamLogicalConvention.INSTANCE), table);
    this.sqlTable = sqlTable;
  }

  @Override
  public PTransform<PCollectionTuple, PCollection<Row>> toPTransform() {
    return new Transform();
  }

  private class Transform extends PTransform<PCollectionTuple, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionTuple inputPCollections) {
      String sourceName = Joiner.on('.').join(getTable().getQualifiedName());

      TupleTag<Row> sourceTupleTag = new TupleTag<>(sourceName);
      if (inputPCollections.has(sourceTupleTag)) {
        // choose PCollection from input PCollectionTuple if exists there.
        PCollection<Row> sourceStream = inputPCollections.get(new TupleTag<Row>(sourceName));
        return sourceStream;
      }
      // If not, the source PColection is provided with BaseBeamTable.buildIOReader().
      return sqlTable
          .buildIOReader(inputPCollections.getPipeline())
          .setCoder(CalciteUtils.toBeamSchema(getRowType()).getRowCoder());
    }
  }

  protected BeamSqlTable getBeamSqlTable() {
    return sqlTable;
  }
}
