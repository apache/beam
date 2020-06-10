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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamBuiltinAggregations;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelFieldCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Window;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

public class BeamWindowRel extends Window implements BeamRelNode {
  public BeamWindowRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<RexLiteral> constants,
      RelDataType rowType,
      List<Group> groups) {
    super(cluster, traitSet, input, constants, rowType, groups);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    Schema outputSchema = CalciteUtils.toSchema(getRowType());
    final List<FieldAggregation> analyticFields =
        this.groups.stream()
            .map(
                anAnalyticGroup -> {
                  List<Integer> partitionKeysDef = anAnalyticGroup.keys.toList();
                  List<Integer> orderByKeys = Lists.newArrayList();
                  List<Boolean> orderByDirections = Lists.newArrayList();
                  List<Boolean> orderByNullDirections = Lists.newArrayList();
                  anAnalyticGroup.orderKeys.getFieldCollations().stream()
                      .forEach(
                          fc -> {
                            orderByKeys.add(fc.getFieldIndex());
                            orderByDirections.add(
                                fc.direction == RelFieldCollation.Direction.ASCENDING);
                            orderByNullDirections.add(
                                fc.nullDirection == RelFieldCollation.NullDirection.FIRST);
                          });
                  int lowerB = Integer.MAX_VALUE; // Unbounded by default
                  int upperB = Integer.MAX_VALUE; // Unbounded by default
                  if (anAnalyticGroup.lowerBound.isCurrentRow()) {
                    lowerB = 0;
                  } else if (anAnalyticGroup.lowerBound.isPreceding()) {
                    // pending
                  } else if (anAnalyticGroup.lowerBound.isFollowing()) {
                    // pending
                  }
                  if (anAnalyticGroup.upperBound.isCurrentRow()) {
                    upperB = 0;
                  } else if (anAnalyticGroup.upperBound.isPreceding()) {
                    // pending
                  } else if (anAnalyticGroup.upperBound.isFollowing()) {
                    // pending
                  }
                  // Assume a single input for now
                  final List<Integer> aggregationFields = Lists.newArrayList();
                  anAnalyticGroup.aggCalls.stream()
                      .forEach(
                          anAggCall -> {
                            anAggCall.operands.stream()
                                .forEach(
                                    anAggCallInput -> {
                                      aggregationFields.add(
                                          ((RexInputRef) anAggCallInput).getIndex());
                                    });
                          });
                  return new FieldAggregation(
                      partitionKeysDef,
                      orderByKeys,
                      orderByDirections,
                      orderByNullDirections,
                      lowerB,
                      upperB,
                      anAnalyticGroup.isRows,
                      aggregationFields);
                })
            .collect(toList());
    return new Transform(outputSchema, analyticFields);
  }

  private static class FieldAggregation implements Serializable {

    private List<Integer> partitionKeys;
    private List<Integer> orderKeys;
    private List<Boolean> orderOrientations;
    private List<Boolean> orderNulls;
    private int lowerLimit = Integer.MAX_VALUE;
    private int upperLimit = Integer.MAX_VALUE;
    private boolean rows = true;
    private List<Integer> inputFields;
    // private AggFunction  ... pending

    public FieldAggregation(
        List<Integer> partitionKeys,
        List<Integer> orderKeys,
        List<Boolean> orderOrientations,
        List<Boolean> orderNulls,
        int lowerLimit,
        int upperLimit,
        boolean rows,
        List<Integer> fields) {
      this.partitionKeys = partitionKeys;
      this.orderKeys = orderKeys;
      this.orderOrientations = orderOrientations;
      this.orderNulls = orderNulls;
      this.lowerLimit = lowerLimit;
      this.upperLimit = upperLimit;
      this.rows = rows;
      this.inputFields = fields;
    }
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {
    NodeStats inputStat = BeamSqlRelUtils.getNodeStats(this.input, mq);
    return inputStat;
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    NodeStats inputStat = BeamSqlRelUtils.getNodeStats(this.input, mq);
    float multiplier = 1f + 0.125f;
    return BeamCostModel.FACTORY.makeCost(
        inputStat.getRowCount() * multiplier, inputStat.getRate() * multiplier);
  }

  private static class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    private Schema outputSchema;
    private List<FieldAggregation> aggFields;

    public Transform(Schema s, List<FieldAggregation> af) {
      this.outputSchema = s;
      this.aggFields = af;
    }

    @Override
    public PCollection<Row> expand(PCollectionList<Row> input) {
      PCollection<Row> r = input.get(0);
      for (FieldAggregation af : aggFields) {
        org.apache.beam.sdk.schemas.transforms.Group.ByFields<Row> myg =
            org.apache.beam.sdk.schemas.transforms.Group.byFieldIds(af.partitionKeys);
        r = r.apply("partitionBy", myg);
        r = r.apply("orderBy", ParDo.of(sortPartition(af))).setRowSchema(r.getSchema());
        r = r.apply("aggCall", ParDo.of(aggField(outputSchema, af))).setRowSchema(outputSchema);
      }
      return r;
    }
  }

  private static DoFn<Row, Row> aggField(
      final Schema outputSchema, final FieldAggregation fieldAgg) {
    return new DoFn<Row, Row>() {
      @ProcessElement
      public void processElement(
          @Element Row inputPartition, OutputReceiver<Row> out, ProcessContext c) {
        Collection<Row> inputPartitions = inputPartition.getArray(1); // 1 -> value
        List<Row> sortedRowsAsList = new ArrayList<Row>(inputPartitions);
        for (int idx = 0; idx < sortedRowsAsList.size(); idx++) {
          int lowerIndex = idx - fieldAgg.lowerLimit;
          int upperIndex = idx + fieldAgg.upperLimit + 1;
          lowerIndex = lowerIndex < 0 ? 0 : lowerIndex;
          upperIndex = upperIndex > sortedRowsAsList.size() ? sortedRowsAsList.size() : upperIndex;
          List<Row> aggRange = sortedRowsAsList.subList(lowerIndex, upperIndex);

          // Just concept-proof
          // Assume that aggFun = SUM
          // Assume dataType = INTEGER
          final Combine.CombineFn<Integer, int[], Integer> aggFunction =
              (Combine.CombineFn<Integer, int[], Integer>)
                  BeamBuiltinAggregations.create("SUM", Schema.FieldType.INT32);
          int[] aggAccumulator = aggFunction.createAccumulator();

          // Assume a simple expression within SUM($aUniqueDirectField)
          final int aggFieldIndex = fieldAgg.inputFields.get(0);

          for (Row aggRow : aggRange) {
            Integer valueToAgg = aggRow.getInt32(aggFieldIndex);
            aggFunction.addInput(aggAccumulator, valueToAgg);
          }
          Integer aggOutput = aggFunction.extractOutput(aggAccumulator);
          List<Object> fieldValues =
              Lists.newArrayListWithCapacity(sortedRowsAsList.get(idx).getFieldCount());
          fieldValues.addAll(sortedRowsAsList.get(idx).getValues());
          fieldValues.add(aggOutput);
          Row ou = Row.withSchema(outputSchema).addValues(fieldValues).build();
          out.output(ou);
        }
      }
    };
  }

  private static DoFn<Row, Row> sortPartition(final FieldAggregation fieldAgg) {
    return new DoFn<Row, Row>() {
      @ProcessElement
      public void processElement(
          @Element Row inputPartition, OutputReceiver<Row> out, ProcessContext c) {
        Collection<Row> value =
            inputPartition.getArray(1); // 1 -> value , inputPartition (key, value)
        List<Row> partitionRows = new ArrayList<Row>(value);
        BeamSortRel.BeamSqlRowComparator beamSqlRowComparator =
            new BeamSortRel.BeamSqlRowComparator(
                fieldAgg.orderKeys, fieldAgg.orderOrientations, fieldAgg.orderNulls);
        Collections.sort(partitionRows, beamSqlRowComparator);
        List<Object> fieldValues = Lists.newArrayListWithCapacity(inputPartition.getFieldCount());
        fieldValues.addAll(inputPartition.getValues());
        fieldValues.set(1, partitionRows);
        Row build = Row.withSchema(inputPartition.getSchema()).addValues(fieldValues).build();
        out.output(build);
      }
    };
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return this.copy(traitSet, sole(inputs), this.constants, this.rowType, this.groups);
  }

  public BeamWindowRel copy(
      RelTraitSet traitSet,
      RelNode input,
      List<RexLiteral> constants,
      RelDataType rowType,
      List<Group> groups) {
    return new BeamWindowRel(getCluster(), traitSet, input, constants, rowType, groups);
  }
}
