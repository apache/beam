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

import java.util.Collection;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelWriter;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Correlate;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Uncollect;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link BeamRelNode} to implement UNNEST, supporting specifically only {@link Correlate} with
 * {@link Uncollect}.
 */
public class BeamUnnestRel extends Uncollect implements BeamRelNode {

  private final RelDataType unnestType;
  private final int unnestIndex;

  public BeamUnnestRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType unnestType,
      int unnestIndex) {
    super(cluster, traitSet, input);
    this.unnestType = unnestType;
    this.unnestIndex = unnestIndex;
  }

  @Override
  public Uncollect copy(RelTraitSet traitSet, RelNode input) {
    return new BeamUnnestRel(getCluster(), traitSet, input, unnestType, unnestIndex);
  }

  @Override
  protected RelDataType deriveRowType() {
    return SqlValidatorUtil.deriveJoinRowType(
        input.getRowType(),
        unnestType,
        JoinRelType.INNER,
        getCluster().getTypeFactory(),
        null,
        ImmutableList.of());
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {
    // We estimate the average length of each array by a constant.
    // We might be able to get an estimate of the length by making a MetadataHandler for this
    // purpose, and get the estimate by reading the first couple of the rows in the source.
    return BeamSqlRelUtils.getNodeStats(this.input, mq).multiply(2);
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    NodeStats estimates = BeamSqlRelUtils.getNodeStats(this, mq);
    return BeamCostModel.FACTORY.makeCost(estimates.getRowCount(), estimates.getRate());
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("unnestIndex", Integer.toString(unnestIndex));
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new Transform();
  }

  private class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {
    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      // The set of rows where we run the correlated unnest for each row
      PCollection<Row> outer = pinput.get(0);

      Schema joinedSchema = CalciteUtils.toSchema(rowType);

      return outer
          .apply(ParDo.of(new UnnestFn(joinedSchema, unnestIndex)))
          .setRowSchema(joinedSchema);
    }
  }

  private static class UnnestFn extends DoFn<Row, Row> {

    private final Schema outputSchema;
    private final int unnestIndex;

    private UnnestFn(Schema outputSchema, int unnestIndex) {
      this.outputSchema = outputSchema;
      this.unnestIndex = unnestIndex;
    }

    @ProcessElement
    public void process(@Element Row row, OutputReceiver<Row> out) {

      @Nullable Collection<Object> rawValues = row.getArray(unnestIndex);

      if (rawValues == null) {
        return;
      }
      Schema.TypeName typeName =
          outputSchema.getField(unnestIndex).getType().getCollectionElementType().getTypeName();

      for (Object uncollectedValue : rawValues) {
        if (typeName.equals(Schema.TypeName.ROW)) {
          Row nestedRow = (Row) uncollectedValue;
          out.output(
              Row.withSchema(outputSchema)
                  .addValues(row.getBaseValues())
                  .addValues(nestedRow.getBaseValues())
                  .build());
        } else {
          out.output(
              Row.withSchema(outputSchema)
                  .addValues(row.getBaseValues())
                  .addValue(uncollectedValue)
                  .build());
        }
      }
    }
  }
}
