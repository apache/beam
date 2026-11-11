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
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRelMetadataQuery;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamBuiltinAnalyticFunctions;
import org.apache.beam.sdk.extensions.sql.impl.transform.agg.AggregationCombineFnAdapter;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelFieldCollation;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.AggregateCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.Window;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.ReadableInstant;

/**
 * {@code BeamRelNode} to replace a {@code Window} node.
 *
 * <p>The following types of Analytic Functions are supported:
 *
 * <pre>{@code
 * SELECT agg(c) over () FROM t
 * SELECT agg(c1) over (PARTITION BY c2 ORDER BY c3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t
 * SELECT agg(c1) over (PARTITION BY c2 ORDER BY c3 RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t
 * }</pre>
 *
 * <h3>Constraints</h3>
 *
 * <ul>
 *   <li>Only Aggregate Analytic Functions are available.
 * </ul>
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
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
              BigDecimal lowerB = null; // Unbounded by default
              BigDecimal upperB = null; // Unbounded by default
              if (anAnalyticGroup.lowerBound.isCurrentRow()) {
                lowerB = BigDecimal.ZERO;
              } else if (anAnalyticGroup.lowerBound.isPreceding()) {
                if (!anAnalyticGroup.lowerBound.isUnbounded()) {
                  lowerB = getLiteralValueConstants(anAnalyticGroup.lowerBound.getOffset());
                }
              } else if (anAnalyticGroup.lowerBound.isFollowing()) {
                if (!anAnalyticGroup.lowerBound.isUnbounded()) {
                  lowerB =
                      getLiteralValueConstants(anAnalyticGroup.lowerBound.getOffset()).negate();
                }
              }
              if (anAnalyticGroup.upperBound.isCurrentRow()) {
                upperB = BigDecimal.ZERO;
              } else if (anAnalyticGroup.upperBound.isPreceding()) {
                if (!anAnalyticGroup.upperBound.isUnbounded()) {
                  upperB =
                      getLiteralValueConstants(anAnalyticGroup.upperBound.getOffset()).negate();
                }
              } else if (anAnalyticGroup.upperBound.isFollowing()) {
                if (!anAnalyticGroup.upperBound.isUnbounded()) {
                  upperB = getLiteralValueConstants(anAnalyticGroup.upperBound.getOffset());
                }
              }
              final BigDecimal lowerBFinal = lowerB;
              final BigDecimal upperBFinal = upperB;
              List<AggregateCall> aggregateCalls = anAnalyticGroup.getAggregateCalls(this);
              aggregateCalls.stream()
                  .forEach(
                      anAggCall -> {
                        List<Integer> argList = anAggCall.getArgList();
                        Schema.Field field =
                            CalciteUtils.toField(anAggCall.getName(), anAggCall.getType());
                        String aggName = anAggCall.getAggregation().getName();
                        Combine.CombineFn combineFn;
                        if (anAggCall.ignoreNulls()
                            && ("FIRST_VALUE".equals(aggName) || "LAST_VALUE".equals(aggName))) {
                          combineFn =
                              "FIRST_VALUE".equals(aggName)
                                  ? BeamBuiltinAnalyticFunctions.navigationFirstValue(true)
                                  : BeamBuiltinAnalyticFunctions.navigationLastValue(true);
                        } else if ("NTH_VALUE".equals(aggName)) {
                          long nthN = readNthValueOffset(argList);
                          combineFn =
                              BeamBuiltinAnalyticFunctions.navigationNthValue(
                                  nthN, anAggCall.ignoreNulls());
                          // Keep only the value column in argList; the offset n is resolved from
                          // constants.
                          argList = argList.isEmpty() ? argList : argList.subList(0, 1);
                        } else {
                          combineFn =
                              AggregationCombineFnAdapter.createCombineFnAnalyticsFunctions(
                                  anAggCall, field, aggName);
                        }
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

  private BigDecimal getLiteralValueConstants(RexNode n) {
    int idx = ((RexInputRef) n).getIndex() - input.getRowType().getFieldCount();
    return (BigDecimal) this.constants.get(idx).getValue();
  }

  /**
   * Extracts the 1-based position {@code n} of an {@code NTH_VALUE(expr, n)} call. The value column
   * is {@code argList[0]}; {@code argList[1]} references the {@code n} literal, which the planner
   * stores in the Window {@code constants} list at index {@code arg - inputFieldCount}. Defaults to
   * 1 (== FIRST_VALUE) if the position cannot be resolved.
   */
  private long readNthValueOffset(List<Integer> argList) {
    if (argList.size() < 2) {
      return 1L;
    }
    int inputFieldCount = input.getRowType().getFieldCount();
    // Calcite stores window literal arguments in `constants` indexed past the input fields.
    int constIdx = argList.get(1) - inputFieldCount;
    if (constIdx >= 0 && constIdx < this.constants.size()) {
      Object v = this.constants.get(constIdx).getValue();
      if (v instanceof BigDecimal) {
        return ((BigDecimal) v).longValueExact();
      }
      if (v instanceof Number) {
        return ((Number) v).longValue();
      }
    }
    return 1L;
  }

  private static class FieldAggregation implements Serializable {

    private List<Integer> partitionKeys;
    private List<Integer> orderKeys;
    private List<Boolean> orderOrientations;
    private List<Boolean> orderNulls;
    private BigDecimal lowerLimit = null;
    private BigDecimal upperLimit = null;
    private boolean rows = true;
    private List<Integer> inputFields;
    private Combine.CombineFn combineFn;
    private Schema.Field outputField;

    public FieldAggregation(
        List<Integer> partitionKeys,
        List<Integer> orderKeys,
        List<Boolean> orderOrientations,
        List<Boolean> orderNulls,
        BigDecimal lowerLimit,
        BigDecimal upperLimit,
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
  public NodeStats estimateNodeStats(BeamRelMetadataQuery mq) {
    // Use mq.getRowCount so RelSubset inputs do not return UNKNOWN/infinite cost.
    double rows = mq.getRowCount(this.input);
    return NodeStats.create(rows, 0d, rows);
  }

  /**
   * A dummy cost computation based on a fixed multiplier.
   *
   * <p>Since, there are not additional LogicalWindow to BeamWindowRel strategies, the result of
   * this cost computation has no impact in the query execution plan.
   */
  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, BeamRelMetadataQuery mq) {
    // Use the planner's row-count (RelSubset-safe) rather than BeamSqlRelUtils.getNodeStats, which
    // yields UNKNOWN/infinite for a RelSubset input and prevents the Volcano cost fixpoint from
    // converging.
    double rows = mq.getRowCount(this.input);
    float multiplier = 1f + 0.125f;
    return BeamCostModel.FACTORY.makeCost(rows * multiplier, 0d);
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
      int ids = 0;
      for (FieldAggregation af : aggFields) {
        ids++;
        String prefix = "transform_" + ids;
        Coder<Row> rowCoder = inputData.getCoder();
        PCollection<Iterable<Row>> partitioned = null;
        if (af.partitionKeys.isEmpty()) {
          partitioned =
              inputData.apply(
                  prefix + "globalPartition",
                  org.apache.beam.sdk.schemas.transforms.Group.globally());
        } else {
          org.apache.beam.sdk.schemas.transforms.Group.ByFields<Row> myg =
              org.apache.beam.sdk.schemas.transforms.Group.byFieldIds(af.partitionKeys);
          PCollection<KV<Row, Iterable<Row>>> partitionBy =
              inputData
                  .apply(prefix + "partitionByKV", myg.getToKV())
                  .apply(prefix + "partitionByGK", GroupByKey.create());
          partitioned =
              partitionBy
                  .apply(prefix + "selectOnlyValues", ParDo.of(new SelectOnlyValues()))
                  .setCoder(IterableCoder.of(rowCoder));
        }
        // Migrate to a SortedValues transform.
        PCollection<List<Row>> sortedPartition =
            partitioned
                .apply(prefix + "orderBy", ParDo.of(sortPartition(af)))
                .setCoder(ListCoder.of(rowCoder));

        inputSchema =
            Schema.builder().addFields(inputSchema.getFields()).addFields(af.outputField).build();
        inputData =
            sortedPartition
                .apply(prefix + "aggCall", ParDo.of(aggField(inputSchema, af)))
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
          @Element List<Row> inputPartition,
          OutputReceiver<Row> out,
          @SuppressWarnings("unused") ProcessContext c) {
        List<Row> sortedRowsAsList = inputPartition;
        NavigableMap<BigDecimal, List<Row>> indexRange = null;
        if (!fieldAgg.rows) {
          indexRange = indexRows(sortedRowsAsList);
        }
        for (int idx = 0; idx < sortedRowsAsList.size(); idx++) {
          List<Row> aggRange = null;
          if (fieldAgg.rows) {
            aggRange = getRows(sortedRowsAsList, idx);
          } else {
            aggRange = getRange(indexRange, sortedRowsAsList.get(idx));
          }
          Object accumulator = fieldAgg.combineFn.createAccumulator();
          // if not inputs are needed, put a mock Field index
          final int aggFieldIndex =
              fieldAgg.inputFields.isEmpty() ? -1 : fieldAgg.inputFields.get(0);
          long count = 0;
          for (Row aggRow : aggRange) {
            if (fieldAgg.combineFn instanceof BeamBuiltinAnalyticFunctions.PositionAwareCombineFn) {
              BeamBuiltinAnalyticFunctions.PositionAwareCombineFn fn =
                  (BeamBuiltinAnalyticFunctions.PositionAwareCombineFn) fieldAgg.combineFn;
              Object positionInputValue =
                  aggFieldIndex >= 0 ? aggRow.getBaseValue(aggFieldIndex) : (Object) aggRow;
              accumulator =
                  fn.addInput(
                      accumulator,
                      getOrderByValue(aggRow),
                      positionInputValue,
                      count,
                      (long) idx,
                      (long) sortedRowsAsList.size());
            } else {
              // Pass the row itself for argumentless functions like COUNT(*) OVER (...).
              Object inputValue =
                  aggFieldIndex >= 0 ? aggRow.getBaseValue(aggFieldIndex) : (Object) aggRow;
              accumulator = fieldAgg.combineFn.addInput(accumulator, inputValue);
            }
            count++;
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

      private NavigableMap<BigDecimal, List<Row>> indexRows(List<Row> input) {
        NavigableMap<BigDecimal, List<Row>> map = new TreeMap<BigDecimal, List<Row>>();
        for (Row r : input) {
          BigDecimal orderByValue = getOrderByValue(r);
          if (orderByValue == null) {
            /** Special case agg(X) OVER () set dummy value. */
            orderByValue = BigDecimal.ZERO;
          }
          if (!map.containsKey(orderByValue)) {
            map.put(orderByValue, Lists.newArrayList());
          }
          map.get(orderByValue).add(r);
        }
        return map;
      }

      private List<Row> getRange(NavigableMap<BigDecimal, List<Row>> indexRanges, Row aRow) {
        NavigableMap<BigDecimal, List<Row>> subMap;
        BigDecimal currentRowValue = getOrderByValue(aRow);
        if (currentRowValue != null && fieldAgg.lowerLimit != null && fieldAgg.upperLimit != null) {
          BigDecimal ll = currentRowValue.subtract(fieldAgg.lowerLimit);
          BigDecimal ul = currentRowValue.add(fieldAgg.upperLimit);
          subMap = indexRanges.subMap(ll, true, ul, true);
        } else if (currentRowValue != null
            && fieldAgg.lowerLimit != null
            && fieldAgg.upperLimit == null) {
          BigDecimal ll = currentRowValue.subtract(fieldAgg.lowerLimit);
          subMap = indexRanges.tailMap(ll, true);
        } else if (currentRowValue != null
            && fieldAgg.lowerLimit == null
            && fieldAgg.upperLimit != null) {
          BigDecimal ul = currentRowValue.add(fieldAgg.upperLimit);
          subMap = indexRanges.headMap(ul, true);
        } else {
          subMap = indexRanges;
        }
        List<Row> result = Lists.newArrayList();
        for (List<Row> partialList : subMap.values()) {
          result.addAll(partialList);
        }
        return result;
      }

      private BigDecimal getOrderByValue(Row r) {
        /**
         * Special Case: This query is transformed by calcite as follows: agg(X) over () -> agg(X)
         * over (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) No orderKeys, so return
         * null.
         */
        if (fieldAgg.orderKeys.size() == 0) {
          return null;
        } else {
          return toComparableBigDecimal(r.getBaseValue(fieldAgg.orderKeys.get(0)));
        }
      }

      /**
       * Convert an ORDER BY key's base value into a comparable {@link BigDecimal} for RANGE
       * framing.
       *
       * <p>Numeric values keep their exact decimal representation (byte-identical to the historical
       * {@code new BigDecimal(((Number) v).toString())} path). Temporal values (DATE / TIME /
       * TIMESTAMP / TIMESTAMP_WITH_LOCAL_TIME_ZONE) all surface a base value of joda {@link
       * ReadableInstant}; their epoch-millis are used so a RANGE frame ordered by a temporal column
       * compares correctly instead of throwing a {@link ClassCastException}. A null value (NULL
       * ordering key) maps to {@code null}, leaving the surrounding callers' existing null handling
       * intact.
       */
      private BigDecimal toComparableBigDecimal(Object value) {
        if (value == null) {
          return null;
        }
        if (value instanceof Number) {
          return new BigDecimal(((Number) value).toString());
        }
        if (value instanceof ReadableInstant) {
          return BigDecimal.valueOf(((ReadableInstant) value).getMillis());
        }
        return BigDecimal.ZERO;
      }

      private List<Row> getRows(List<Row> input, int index) {
        Integer ll =
            fieldAgg.lowerLimit != null ? fieldAgg.lowerLimit.intValue() : Integer.MAX_VALUE;
        Integer ul =
            fieldAgg.upperLimit != null ? fieldAgg.upperLimit.intValue() : Integer.MAX_VALUE;
        int lowerIndex = ll == Integer.MAX_VALUE ? Integer.MIN_VALUE : index - ll;
        int upperIndex = ul == Integer.MAX_VALUE ? Integer.MAX_VALUE : index + ul + 1;
        // Clamp bounds to [0, size] so frames completely outside the partition yield an empty list.
        lowerIndex = Math.max(0, Math.min(lowerIndex, input.size()));
        upperIndex = Math.max(lowerIndex, Math.min(upperIndex, input.size()));
        List<Row> out = input.subList(lowerIndex, upperIndex);
        return out;
      }
    };
  }

  static class SelectOnlyValues extends DoFn<KV<Row, Iterable<Row>>, Iterable<Row>> {
    @ProcessElement
    public void processElement(
        @Element KV<Row, Iterable<Row>> inputPartition,
        OutputReceiver<Iterable<Row>> out,
        @SuppressWarnings("unused") ProcessContext c) {
      out.output(inputPartition.getValue());
    }
  }

  private static DoFn<Iterable<Row>, List<Row>> sortPartition(final FieldAggregation fieldAgg) {
    return new DoFn<Iterable<Row>, List<Row>>() {
      @ProcessElement
      public void processElement(
          @Element Iterable<Row> inputPartition,
          OutputReceiver<List<Row>> out,
          @SuppressWarnings("unused") ProcessContext c) {
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

  @Override
  public Window copy(List<RexLiteral> constants) {
    return this.copy(traitSet, input, constants, this.rowType, this.groups);
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
