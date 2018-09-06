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
import org.apache.calcite.rel.core.Uncollect;

/** {@link BeamRelNode} to implement an uncorrelated {@link Uncollect}, aka UNNEST. */
public class BeamUncollectRel extends Uncollect implements BeamRelNode {

  public BeamUncollectRel(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, boolean withOrdinality) {
    super(cluster, traitSet, input, withOrdinality);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, RelNode input) {
    return new BeamUncollectRel(getCluster(), traitSet, input, withOrdinality);
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
          BeamUncollectRel.class.getSimpleName(),
          pinput);
      PCollection<Row> upstream = pinput.get(0);

      // Each row of the input contains a single array of things to be emitted; Calcite knows
      // what the row looks like
      Schema outputSchema = CalciteUtils.toSchema(getRowType());

      PCollection<Row> uncollected =
          upstream.apply(ParDo.of(new UncollectDoFn(outputSchema))).setRowSchema(outputSchema);

      return uncollected;
    }
  }

  private static class UncollectDoFn extends DoFn<Row, Row> {

    private final Schema schema;

    private UncollectDoFn(Schema schema) {
      this.schema = schema;
    }

    @ProcessElement
    public void process(@Element Row inputRow, OutputReceiver<Row> output) {
      for (Object element : inputRow.getArray(0)) {
        output.output(Row.withSchema(schema).addValue(element).build());
      }
    }
  }
}
