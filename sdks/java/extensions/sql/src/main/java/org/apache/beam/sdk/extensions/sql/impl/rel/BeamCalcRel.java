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
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
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
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexProgram;

/** BeamRelNode to replace a {@code Project} node. */
public class BeamCalcRel extends Calc implements BeamRelNode {

  public BeamCalcRel(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexProgram program) {
    super(cluster, traits, input, program);
  }

  @Override
  public Calc copy(RelTraitSet traitSet, RelNode input, RexProgram program) {
    return new BeamCalcRel(getCluster(), traitSet, input, program);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new Transform();
  }

  private class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      checkArgument(
          pinput.size() == 1,
          "Wrong number of inputs for %s: %s",
          BeamCalcRel.class.getSimpleName(),
          pinput);
      PCollection<Row> upstream = pinput.get(0);

      BeamSqlExpressionExecutor executor = new BeamSqlFnExecutor(BeamCalcRel.this.getProgram());

      Schema schema = CalciteUtils.toSchema(rowType);
      PCollection<Row> projectStream =
          upstream
              .apply(ParDo.of(new CalcFn(executor, CalciteUtils.toSchema(rowType))))
              .setRowSchema(schema);
      projectStream.setRowSchema(CalciteUtils.toSchema(getRowType()));

      return projectStream;
    }
  }

  public int getLimitCountOfSortRel() {
    if (input instanceof BeamSortRel) {
      return ((BeamSortRel) input).getCount();
    }

    throw new RuntimeException("Could not get the limit count from a non BeamSortRel input.");
  }

  public boolean isInputSortRelAndLimitOnly() {
    return (input instanceof BeamSortRel) && ((BeamSortRel) input).isLimitOnly();
  }

  /** {@code CalcFn} is the executor for a {@link BeamCalcRel} step. */
  private static class CalcFn extends DoFn<Row, Row> {
    private BeamSqlExpressionExecutor executor;
    private Schema outputSchema;

    public CalcFn(BeamSqlExpressionExecutor executor, Schema outputSchema) {
      super();
      this.executor = executor;
      this.outputSchema = outputSchema;
    }

    @Setup
    public void setup() {
      executor.prepare();
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window, PaneInfo paneInfo) {
      Row inputRow = c.element();
      @Nullable
      List<Object> rawResultValues =
          executor.execute(
              inputRow, window, paneInfo, BeamSqlExpressionEnvironments.forRow(inputRow, null));

      if (rawResultValues != null) {
        List<Object> castResultValues =
            IntStream.range(0, outputSchema.getFieldCount())
                .mapToObj(i -> castField(rawResultValues, i))
                .collect(toList());
        c.output(Row.withSchema(outputSchema).addValues(castResultValues).build());
      }
    }

    private Object castField(List<Object> resultValues, int i) {
      return BeamTableUtils.autoCastField(outputSchema.getField(i), resultValues.get(i));
    }

    @Teardown
    public void close() {
      executor.close();
    }
  }
}
