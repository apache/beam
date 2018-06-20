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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironment;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironments;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionExecutor;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutor;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * {@link BeamRelNode} to implement UNNEST, supporting specifically only {@link Correlate} with
 * {@link Uncollect}.
 */
public class BeamUnnestRel extends Correlate implements BeamRelNode {

  public BeamUnnestRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      CorrelationId correlationId,
      ImmutableBitSet requiredColumns,
      SemiJoinType joinType) {
    super(cluster, traits, left, right, correlationId, requiredColumns, joinType);
  }

  @Override
  public Correlate copy(
      RelTraitSet relTraitSet,
      RelNode left,
      RelNode right,
      CorrelationId correlationId,
      ImmutableBitSet requireColumns,
      SemiJoinType joinType) {
    return new BeamUnnestRel(
        getCluster(), relTraitSet, left, right, correlationId, requiredColumns, joinType);
  }

  @Override
  public List<RelNode> getPCollectionInputs() {
    return ImmutableList.of(BeamSqlRelUtils.getBeamRelInput(left));
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

      // The correlated subquery
      BeamUncollectRel uncollect = (BeamUncollectRel) BeamSqlRelUtils.getBeamRelInput(right);
      Schema innerSchema = CalciteUtils.toBeamSchema(uncollect.getRowType());
      checkArgument(
          innerSchema.getFieldCount() == 1, "Can only UNNEST a single column", getClass());

      BeamSqlExpressionExecutor expr =
          new BeamSqlFnExecutor(
              ((BeamCalcRel) BeamSqlRelUtils.getBeamRelInput(uncollect.getInput())).getProgram());

      Schema joinedSchema = CalciteUtils.toBeamSchema(rowType);

      return outer
          .apply(
              ParDo.of(
                  new UnnestFn(correlationId.getId(), expr, joinedSchema, innerSchema.getField(0))))
          .setCoder(joinedSchema.getRowCoder());
    }
  }

  private static class UnnestFn extends DoFn<Row, Row> {

    /** The expression that should return an iterable to be uncollected. */
    private final BeamSqlExpressionExecutor expr;

    private final int correlationId;
    private final Schema outputSchema;
    private final Schema.Field innerField;

    private UnnestFn(
        int correlationId,
        BeamSqlExpressionExecutor expr,
        Schema outputSchema,
        Schema.Field innerField) {
      this.correlationId = correlationId;
      this.expr = expr;
      this.outputSchema = outputSchema;
      this.innerField = innerField;
    }

    @ProcessElement
    public void process(@Element Row row, BoundedWindow window, OutputReceiver<Row> out) {

      checkState(correlationId == 0, "Only one level of correlation nesting is supported");
      BeamSqlExpressionEnvironment env =
          BeamSqlExpressionEnvironments.forRowAndCorrelVariables(
              row, window, ImmutableList.of(row));

      @Nullable List<Object> rawValues = expr.execute(row, window, env);

      if (rawValues == null) {
        return;
      }

      checkState(
          rawValues.size() == 1,
          "%s expression to unnest %s resulted in more than one column",
          getClass(),
          expr);

      checkState(
          rawValues.get(0) instanceof Iterable,
          "%s expression to unnest %s not iterable",
          getClass(),
          expr);

      for (Object uncollectedValue : (Iterable) rawValues.get(0)) {
        Object coercedValue = BeamTableUtils.autoCastField(innerField, uncollectedValue);
        out.output(
            Row.withSchema(outputSchema).addValues(row.getValues()).addValue(coercedValue).build());
      }
    }

    @Teardown
    public void close() {
      expr.close();
    }
  }
}
