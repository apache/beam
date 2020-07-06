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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.transform.agg.AggregationCombineFnAdapter;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelFieldCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.AggregateCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Window;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

/**
 * {@code BeamRelNode} to replace a {@code Window} node.
 *
 * <p>The following types of Analytic Functions are supported:
 *
 * <pre>{@code
 * SELECT agg(c) over () FROM t
 * SELECT agg(c1) over (PARTITION BY c2 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t
 * }</pre>
 *
 * <p>The following types of Analytic Functions are not supported:
 *
 * <pre>{@code
 * SELECT agg(c1) over (PARTITION BY c2 ORDER BY c3 ROWS BETWEEN 15 PRECEDING AND CURRENT ROW) FROM t
 * SELECT agg(c1) over (PARTITION BY c2 ORDER BY c3 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t
 * }</pre>
 *
 * <h3>Constraints</h3>
 *
 * <ul>
 *   <li>Only Aggregate Analytic Functions are available.
 *   <li>Sliding windows with RANGE are not supported.
 *   <li>Bounded windows with fixed limits are not supported.
 * </ul>
 */
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
    final List<FieldAggregation> analyticFields = Lists.newArrayList();
    this.groups.stream()
        .forEach(
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
                if (!anAnalyticGroup.lowerBound.isUnbounded())
                  throw new UnsupportedOperationException("Bounded windows not supported!");
              } else if (anAnalyticGroup.lowerBound.isFollowing()) {
                if (!anAnalyticGroup.lowerBound.isUnbounded())
                  throw new UnsupportedOperationException("Bounded windows not supported!");
              }
              if (anAnalyticGroup.upperBound.isCurrentRow()) {
                upperB = 0;
              } else if (anAnalyticGroup.upperBound.isPreceding()) {
                if (!anAnalyticGroup.upperBound.isUnbounded())
                  throw new UnsupportedOperationException("Bounded windows not supported!");
              } else if (anAnalyticGroup.upperBound.isFollowing()) {
                if (!anAnalyticGroup.upperBound.isUnbounded())
                  throw new UnsupportedOperationException("Bounded windows not supported!");
              }
              if (!anAnalyticGroup.isRows) {
                throw new UnsupportedOperationException("RANGE windows not supported!");
              }
              final int lowerBFinal = lowerB;
              final int upperBFinal = upperB;
              List<AggregateCall> aggregateCalls = anAnalyticGroup.getAggregateCalls(this);
              aggregateCalls.stream()
                  .forEach(
                      anAggCall -> {
                        List<Integer> argList = anAggCall.getArgList();
                        Schema.Field field =
                            CalciteUtils.toField(anAggCall.getName(), anAggCall.getType());
                        Combine.CombineFn combineFn =
                            AggregationCombineFnAdapter.createCombineFnAnalyticsFunctions(
                                anAggCall, field, anAggCall.getAggregation().getName());
                        FieldAggregation fieldAggregation =
                            new FieldAggregation(
                                partitionKeysDef,
                                orderByKeys,
                                orderByDirections,
                                orderByNullDirections,
                                lowerBFinal,
                                upperBFinal,
                                anAnalyticGroup.isRows,
                                argList,
                                combineFn,
                                field);
                        analyticFields.add(fieldAggregation);
                      });
            });

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
    private Combine.CombineFn combineFn;
    private Schema.Field outputField;

    public FieldAggregation(
        List<Integer> partitionKeys,
        List<Integer> orderKeys,
        List<Boolean> orderOrientations,
        List<Boolean> orderNulls,
        int lowerLimit,
        int upperLimit,
        boolean rows,
        List<Integer> inputFields,
        Combine.CombineFn combineFn,
        Schema.Field outputField) {
      this.partitionKeys = partitionKeys;
      this.orderKeys = orderKeys;
      this.orderOrientations = orderOrientations;
      this.orderNulls = orderNulls;
      this.lowerLimit = lowerLimit;
      this.upperLimit = upperLimit;
      this.rows = rows;
      this.inputFields = inputFields;
      this.combineFn = combineFn;
      this.outputField = outputField;
    }
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {
    NodeStats inputStat = BeamSqlRelUtils.getNodeStats(this.input, mq);
    return inputStat;
  }

  /**
   * A dummy cost computation based on a fixed multiplier.
   *
   * <p>Since, there are not additional LogicalWindow to BeamWindowRel strategies, the result of
   * this cost computation has no impact in the query execution plan.
   */
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

    public Transform(Schema schema, List<FieldAggregation> fieldAgg) {
      this.outputSchema = schema;
      this.aggFields = fieldAgg;
    }

    @Override
    public PCollection<Row> expand(PCollectionList<Row> input) {
      PCollection<Row> inputData = input.get(0);
      Schema inputSchema = inputData.getSchema();
      for (FieldAggregation af : aggFields) {
        Coder<Row> rowCoder = inputData.getCoder();
        PCollection<Iterable<Row>> partitioned = null;
        if (af.partitionKeys.isEmpty()) {
          partitioned = inputData.apply(org.apache.beam.sdk.schemas.transforms.Group.globally());
        } else {
          org.apache.beam.sdk.schemas.transforms.Group.ByFields<Row> myg =
              org.apache.beam.sdk.schemas.transforms.Group.byFieldIds(af.partitionKeys);
          PCollection<KV<Row, Iterable<Row>>> partitionBy =
              inputData.apply("partitionBy", myg.getToKvs());
          partitioned =
              partitionBy
                  .apply(ParDo.of(new selectOnlyValues()))
                  .setCoder(IterableCoder.of(rowCoder));
        }
        // Migrate to a SortedValues transform.
        PCollection<List<Row>> sortedPartition =
            partitioned
                .apply("orderBy", ParDo.of(sortPartition(af)))
                .setCoder(ListCoder.of(rowCoder));

        inputSchema =
            Schema.builder().addFields(inputSchema.getFields()).addFields(af.outputField).build();
        inputData =
            sortedPartition
                .apply("aggCall", ParDo.of(aggField(inputSchema, af)))
                .setRowSchema(inputSchema);
      }
      return inputData.setRowSchema(this.outputSchema);
    }
  }

  private static DoFn<List<Row>, Row> aggField(
      final Schema expectedSchema, final FieldAggregation fieldAgg) {
    return new DoFn<List<Row>, Row>() {
      @ProcessElement
      public void processElement(
          @Element List<Row> inputPartition, OutputReceiver<Row> out, ProcessContext c) {
        List<Row> sortedRowsAsList = inputPartition;
        for (int idx = 0; idx < sortedRowsAsList.size(); idx++) {
          int lowerIndex =
              fieldAgg.lowerLimit == Integer.MAX_VALUE
                  ? Integer.MIN_VALUE
                  : idx - fieldAgg.lowerLimit;
          int upperIndex =
              fieldAgg.upperLimit == Integer.MAX_VALUE
                  ? Integer.MAX_VALUE
                  : idx + fieldAgg.upperLimit + 1;
          lowerIndex = lowerIndex < 0 ? 0 : lowerIndex;
          upperIndex = upperIndex > sortedRowsAsList.size() ? sortedRowsAsList.size() : upperIndex;
          List<Row> aggRange = sortedRowsAsList.subList(lowerIndex, upperIndex);
          Object accumulator = fieldAgg.combineFn.createAccumulator();
          final int aggFieldIndex = fieldAgg.inputFields.get(0);
          for (Row aggRow : aggRange) {
            fieldAgg.combineFn.addInput(accumulator, aggRow.getBaseValue(aggFieldIndex));
          }
          Object result = fieldAgg.combineFn.extractOutput(accumulator);
          Row processingRow = sortedRowsAsList.get(idx);
          List<Object> fieldValues = Lists.newArrayListWithCapacity(processingRow.getFieldCount());
          fieldValues.addAll(processingRow.getValues());
          fieldValues.add(result);
          Row build = Row.withSchema(expectedSchema).addValues(fieldValues).build();
          out.output(build);
        }
      }
    };
  }

  static class selectOnlyValues extends DoFn<KV<Row, Iterable<Row>>, Iterable<Row>> {
    @ProcessElement
    public void processElement(
        @Element KV<Row, Iterable<Row>> inputPartition,
        OutputReceiver<Iterable<Row>> out,
        ProcessContext c) {
      out.output(inputPartition.getValue());
    }
  }

  private static DoFn<Iterable<Row>, List<Row>> sortPartition(final FieldAggregation fieldAgg) {
    return new DoFn<Iterable<Row>, List<Row>>() {
      @ProcessElement
      public void processElement(
          @Element Iterable<Row> inputPartition, OutputReceiver<List<Row>> out, ProcessContext c) {
        List<Row> partitionRows = Lists.newArrayList(inputPartition);
        BeamSortRel.BeamSqlRowComparator beamSqlRowComparator =
            new BeamSortRel.BeamSqlRowComparator(
                fieldAgg.orderKeys, fieldAgg.orderOrientations, fieldAgg.orderNulls);
        Collections.sort(partitionRows, beamSqlRowComparator);
        out.output(partitionRows);
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
