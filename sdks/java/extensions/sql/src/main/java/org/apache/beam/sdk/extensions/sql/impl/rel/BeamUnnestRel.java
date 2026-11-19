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
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelWriter;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.Correlate;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.Uncollect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlValidatorUtil;
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
     * Appends the fields of an unnested array element to {@code values}, laid out the way {@code
     * outputSchema} expects them. The recursion is bounded by the nesting depth of the element
     * schema.
     *
     * <p>Whether a nested row field of the element arrives as one ROW column or as one column per
     * leaf field depends on which Calcite planner produced the plan. For an element {@code ROW<id,
     * author ROW<name, email>>}, the planner behind {@code SqlTransform} flattens the element into
     * {@code id, name, email}, while the planner behind the JDBC driver and the SQL shell keeps
     * {@code id, author}. Building the values from the element schema alone matches only one of
     * them, and the other then fails in {@link Row.Builder#build()} with a field count mismatch. So
     * each nested row is checked against the output column at the same position: a ROW column takes
     * the row as is, any other column means the row was flattened.
     */
    private void addElementValues(
        Schema elementSchema, @Nullable Row element, List<Object> values) {
      for (int i = 0; i < elementSchema.getFieldCount(); i++) {
        Schema.FieldType fieldType = elementSchema.getField(i).getType();
        Object value = null;
        if (element != null) {
          value = element.getBaseValue(i);
        }

        boolean isRow = fieldType.getTypeName().equals(Schema.TypeName.ROW);
        if (!isRow || isRowColumn(values.size())) {
          values.add(value);
          continue;
        }

        // a null nested row still fills one null per flattened column
        addElementValues(fieldType.getRowSchema(), (Row) value, values);
      }
    }

    private boolean isRowColumn(int index) {
      // past the last output column the element does not fit the planner's layout. returning
      // false lets the values run long so Row.Builder#build() reports the field count mismatch,
      // instead of an IndexOutOfBoundsException from getField() that names neither schema
      if (index >= outputSchema.getFieldCount()) {
        return false;
      }

      return outputSchema.getField(index).getType().getTypeName().equals(Schema.TypeName.ROW);
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
      Schema.FieldType elementType =
          schemaWithArrayField.getField(unnestIndices.get(0)).getType().getCollectionElementType();
      Schema.TypeName typeName = elementType.getTypeName();

      for (Object uncollectedValue : rawValues) {
        if (typeName.equals(Schema.TypeName.ROW)) {
          List<Object> values = new ArrayList<>(row.getBaseValues());
          addElementValues(elementType.getRowSchema(), (Row) uncollectedValue, values);
          out.output(Row.withSchema(outputSchema).addValues(values).build());
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
