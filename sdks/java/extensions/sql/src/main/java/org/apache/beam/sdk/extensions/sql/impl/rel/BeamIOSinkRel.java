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
import java.util.List;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;

/** BeamRelNode to replace a {@code TableModify} node. */
public class BeamIOSinkRel extends TableModify implements BeamRelNode {

  private final BeamSqlEnv sqlEnv;

  public BeamIOSinkRel(
      BeamSqlEnv sqlEnv,
      RelOptCluster cluster,
      RelTraitSet traits,
      RelOptTable table,
      Prepare.CatalogReader catalogReader,
      RelNode child,
      Operation operation,
      List<String> updateColumnList,
      List<RexNode> sourceExpressionList,
      boolean flattened) {
    super(
        cluster,
        traits,
        table,
        catalogReader,
        child,
        operation,
        updateColumnList,
        sourceExpressionList,
        flattened);
    this.sqlEnv = sqlEnv;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BeamIOSinkRel(
        sqlEnv,
        getCluster(),
        traitSet,
        getTable(),
        getCatalogReader(),
        sole(inputs),
        getOperation(),
        getUpdateColumnList(),
        getSourceExpressionList(),
        isFlattened());
  }

  @Override
  public PTransform<PCollectionTuple, PCollection<Row>> toPTransform() {
    return new Transform();
  }

  private class Transform extends PTransform<PCollectionTuple, PCollection<Row>> {

    /**
     * Note that {@code BeamIOSinkRel} returns the input PCollection, which is the persisted
     * PCollection.
     */
    @Override
    public PCollection<Row> expand(PCollectionTuple inputPCollections) {
      RelNode input = getInput();
      String stageName = BeamSqlRelUtils.getStageName(BeamIOSinkRel.this);

      PCollection<Row> upstream =
          inputPCollections.apply(BeamSqlRelUtils.getBeamRelInput(input).toPTransform());

      String sourceName = Joiner.on('.').join(getTable().getQualifiedName());

      BeamSqlTable targetTable = sqlEnv.findTable(sourceName);

      upstream.apply(stageName, targetTable.buildIOWriter());

      return upstream;
    }
  }
}
