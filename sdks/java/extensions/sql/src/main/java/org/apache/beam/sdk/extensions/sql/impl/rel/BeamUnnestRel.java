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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelMetadataQuery;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelWriter;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.core.Correlate;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.core.Uncollect;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link BeamRelNode} to implement UNNEST, supporting specifically only {@link Correlate} with
 * {@link Uncollect}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BeamUnnestRel extends Uncollect implements BeamRelNode {

  private final RelDataType unnestType;
  private final List<Integer> unnestIndices;

  public BeamUnnestRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelDataType unnestType,
      List<Integer> unnestIndices) {
    super(cluster, traitSet, input);
    this.unnestType = unnestType;
    this.unnestIndices = unnestIndices;
  }

  @Override
  public Uncollect copy(RelTraitSet traitSet, RelNode input) {
    return new BeamUnnestRel(getCluster(), traitSet, input, unnestType, unnestIndices);
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
  public NodeStats estimateNodeStats(BeamRelMetadataQuery mq) {
    // We estimate the average length of each array by a constant.
    // We might be able to get an estimate of the length by making a MetadataHandler for this
    // purpose, and get the estimate by reading the first couple of the rows in the source.
    return BeamSqlRelUtils.getNodeStats(this.input, mq).multiply(2);
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, BeamRelMetadataQuery mq) {
    NodeStats estimates = BeamSqlRelUtils.getNodeStats(this, mq);
    return BeamCostModel.FACTORY.makeCost(estimates.getRowCount(), estimates.getRate());
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("unnestIndices", unnestIndices);
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

      Schema joinedSchema = CalciteUtils.toSchema(getRowType());

      return outer
          .apply(ParDo.of(new UnnestFn(joinedSchema, unnestIndices)))
          .setRowSchema(joinedSchema);
    }
  }

  private static class UnnestFn extends DoFn<Row, Row> {

    private final Schema outputSchema;
    private final List<Integer> unnestIndices;

    private UnnestFn(Schema outputSchema, List<Integer> unnestIndices) {
      this.outputSchema = outputSchema;
      this.unnestIndices = unnestIndices;
    }
    /**
     * This is recursive call to get all the values of the nested rows. The recursion is bounded by
     * the amount of nesting with in the data. This mirrors the unnest behavior of calcite towards
     * schema. *
     */
    private List<Object> getNestedRowBaseValues(Row nestedRow) {
      return IntStream.range(0, nestedRow.getFieldCount())
          .mapToObj(
              (i) -> {
                List<Object> values = new ArrayList<>();
                Schema.FieldType fieldType = nestedRow.getSchema().getField(i).getType();
                if (fieldType.getTypeName().equals(Schema.TypeName.ROW)) {
                  @Nullable Row row = nestedRow.getBaseValue(i, Row.class);
                  if (row == null) {
                    return Stream.builder().build();
                  }
                  List<Object> rowValues = getNestedRowBaseValues(row);
                  if (null != rowValues) {
                    values.addAll(rowValues);
                  }
                } else {
                  values.add(nestedRow.getBaseValue(i));
                }
                return values.stream();
              })
          .flatMap(Function.identity())
          .collect(Collectors.toList());
    }

    @ProcessElement
    public void process(@Element Row row, OutputReceiver<Row> out) {
      Row rowWithArrayField = row;
      Schema schemaWithArrayField = outputSchema;
      for (int i = unnestIndices.size() - 1; i > 0; i--) {
        rowWithArrayField = rowWithArrayField.getRow(unnestIndices.get(i));
        schemaWithArrayField =
            schemaWithArrayField.getField(unnestIndices.get(i)).getType().getRowSchema();
      }
      @Nullable Collection<Object> rawValues = rowWithArrayField.getArray(unnestIndices.get(0));

      if (rawValues == null) {
        return;
      }
      Schema.TypeName typeName =
          schemaWithArrayField
              .getField(unnestIndices.get(0))
              .getType()
              .getCollectionElementType()
              .getTypeName();

      for (Object uncollectedValue : rawValues) {
        if (typeName.equals(Schema.TypeName.ROW)) {
          Row nestedRow = (Row) uncollectedValue;
          out.output(
              Row.withSchema(outputSchema)
                  .addValues(row.getBaseValues())
                  .addValues(getNestedRowBaseValues(nestedRow))
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
