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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.autoCastField;
import static org.apache.beam.sdk.values.Row.toRow;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelMetadataQuery;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.Values;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * {@code BeamRelNode} to replace a {@code Values} node.
 *
 * <p>{@code BeamValuesRel} will be used in the following SQLs:
 *
 * <ul>
 *   <li>{@code insert into t (name, desc) values ('hello', 'world')}
 *   <li>{@code select 1, '1', LOCALTIME}
 * </ul>
 */
public class BeamValuesRel extends Values implements BeamRelNode {

  public BeamValuesRel(
      RelOptCluster cluster,
      RelDataType rowType,
      ImmutableList<ImmutableList<RexLiteral>> tuples,
      RelTraitSet traits) {
    super(cluster, rowType, tuples, traits);
  }

  @Override
  public Map<String, String> getPipelineOptions() {
    return ImmutableMap.of();
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new Transform();
  }

  private class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      checkArgument(
          pinput.size() == 0,
          "Should not have received input for %s: %s",
          BeamValuesRel.class.getSimpleName(),
          pinput);

      Schema inferredSchema = CalciteUtils.toSchema(getRowType());
      Schema.Builder schemaBuilder = Schema.builder();
      for (int i = 0; i < inferredSchema.getFieldCount(); i++) {
        Schema.Field field = inferredSchema.getField(i);
        boolean hasNull = false;
        for (ImmutableList<RexLiteral> tuple : tuples) {
          if (tuple.get(i).getValue() == null) {
            hasNull = true;
            break;
          }
        }
        if (hasNull && !field.getType().getNullable()) {
          schemaBuilder.addField(field.getName(), field.getType().withNullable(true));
        } else {
          schemaBuilder.addField(field);
        }
      }
      Schema schema = schemaBuilder.build();
      List<Row> rows = tuples.stream().map(tuple -> tupleToRow(schema, tuple)).collect(toList());
      return pinput
          .getPipeline()
          .begin()
          .apply(Impulse.create())
          .apply(ParDo.of(new EmitRowsFn(rows)))
          .setRowSchema(schema);
    }
  }

  private Row tupleToRow(Schema schema, ImmutableList<RexLiteral> tuple) {
    return IntStream.range(0, tuple.size())
        .mapToObj(
            i -> {
              Object val = tuple.get(i).getValue();
              return autoCastField(schema.getField(i), val);
            })
        .collect(toRow(schema));
  }

  @Override
  public NodeStats estimateNodeStats(BeamRelMetadataQuery mq) {
    return NodeStats.create(tuples.size(), 0, tuples.size());
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, BeamRelMetadataQuery mq) {
    NodeStats estimates = BeamSqlRelUtils.getNodeStats(this, mq);
    return BeamCostModel.FACTORY.makeCost(estimates.getRowCount(), estimates.getRate());
  }

  private static class EmitRowsFn extends DoFn<byte[], Row> {
    private final List<Row> rows;

    public EmitRowsFn(List<Row> rows) {
      this.rows = rows;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      for (Row row : rows) {
        c.output(row);
      }
    }
  }
}
