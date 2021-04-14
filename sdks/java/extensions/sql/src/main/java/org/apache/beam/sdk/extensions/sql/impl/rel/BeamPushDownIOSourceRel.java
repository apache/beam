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

import static org.apache.beam.vendor.calcite.v1_26_0.com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteTable;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.base.Preconditions;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptTable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelWriter;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.RelMetadataQuery;

public class BeamPushDownIOSourceRel extends BeamIOSourceRel {
  private final List<String> usedFields;
  private final BeamSqlTableFilter tableFilters;

  public BeamPushDownIOSourceRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      BeamSqlTable beamTable,
      List<String> usedFields,
      BeamSqlTableFilter tableFilters,
      Map<String, String> pipelineOptions,
      BeamCalciteTable calciteTable) {
    super(cluster, traitSet, table, beamTable, pipelineOptions, calciteTable);
    this.usedFields = usedFields;
    this.tableFilters = tableFilters;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);

    // This is done to tell Calcite planner that BeamIOSourceRel cannot be simply substituted by
    //  another BeamIOSourceRel, except for when they carry the same content.
    if (!usedFields.isEmpty()) {
      pw.item("usedFields", usedFields.toString());
    }
    if (!(tableFilters instanceof DefaultTableFilter)) {
      pw.item(tableFilters.getClass().getSimpleName(), tableFilters.toString());
    }

    return pw;
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

      final PBegin begin = input.getPipeline().begin();
      final BeamSqlTable beamSqlTable = BeamPushDownIOSourceRel.this.getBeamSqlTable();

      if (usedFields.isEmpty() && tableFilters instanceof DefaultTableFilter) {
        return beamSqlTable.buildIOReader(begin);
      }

      final Schema newBeamSchema = CalciteUtils.toSchema(getRowType());
      return beamSqlTable
          .buildIOReader(begin, tableFilters, usedFields)
          .setRowSchema(newBeamSchema);
    }
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    BeamCostModel parentCost = super.beamComputeSelfCost(planner, mq);
    Preconditions.checkArgument(parentCost.getCpu() >= 0, "Cpu cost must be zero or positive.");
    // Table schema will always contain all fields, while usedFields may contain less fields due to
    // push-down. Difference between the two will be the number of fields pushed-down. Bigger
    // difference - better.
    double projectBenefit = getBeamSqlTable().getSchema().getFieldCount() - usedFields.size();
    // Number of filters pushed-down should have a smaller impact.
    double filterBenefit = tableFilters.numSupported();
    // Normalize filterBenefit to be between 0.00 and (10% of projectBenefit).
    filterBenefit = (1.0 - (1.0 / (filterBenefit + 1))) * ((projectBenefit + 1) * 0.1);
    // Total totalBenefit of having push-down.
    double totalBenefit = projectBenefit + filterBenefit;
    // Normalize totalBenefit to be between 0.00 and 1.00.
    double normalizedBenefit = totalBenefit / (Math.max(totalBenefit, parentCost.getCpu()) + 1);
    BeamCostModel minus = BeamCostModel.FACTORY.makeTinyCost().multiplyBy(normalizedBenefit);
    return parentCost.minus(minus);
  }
}
