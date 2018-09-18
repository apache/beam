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

import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

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

      @Nullable List<Object> rawValues = row.getArray(unnestIndex);

      if (rawValues == null) {
        return;
      }

      for (Object uncollectedValue : rawValues) {
        out.output(
            Row.withSchema(outputSchema)
                .addValues(row.getValues())
                .addValue(uncollectedValue)
                .build());
      }
    }
  }
}
