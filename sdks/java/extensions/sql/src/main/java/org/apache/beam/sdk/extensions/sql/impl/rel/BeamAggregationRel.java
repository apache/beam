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
import static org.apache.beam.sdk.values.PCollection.IsBounded.BOUNDED;
import static org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.transform.agg.AggregationCombineFnAdapter;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelWriter;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Aggregate;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.AggregateCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.ImmutableBitSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/** {@link BeamRelNode} to replace a {@link Aggregate} node. */
public class BeamAggregationRel extends Aggregate implements BeamRelNode {
  private @Nullable WindowFn<Row, IntervalWindow> windowFn;
  private final int windowFieldIndex;

  public BeamAggregationRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls,
      @Nullable WindowFn<Row, IntervalWindow> windowFn,
      int windowFieldIndex) {

    super(cluster, traits, child, groupSet, groupSets, aggCalls);

    this.windowFn = windowFn;
    this.windowFieldIndex = windowFieldIndex;
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {

    NodeStats inputStat = BeamSqlRelUtils.getNodeStats(this.input, mq);
    inputStat = computeWindowingCostEffect(inputStat);

    // Aggregates with more aggregate functions cost a bit more
    float multiplier = 1f + (float) aggCalls.size() * 0.125f;
    for (AggregateCall aggCall : aggCalls) {
      if (aggCall.getAggregation().getName().equals("SUM")) {
        // Pretend that SUM costs a little bit more than $SUM0,
        // to make things deterministic.
        multiplier += 0.0125f;
      }
    }

    return BeamCostModel.FACTORY.makeCost(
        inputStat.getRowCount() * multiplier, inputStat.getRate() * multiplier);
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {

    NodeStats inputEstimate = BeamSqlRelUtils.getNodeStats(this.input, mq);

    inputEstimate = computeWindowingCostEffect(inputEstimate);

    NodeStats estimate;
    // groupCount shows how many columns do we have in group by. One of them might be the windowing.
    int groupCount = groupSet.cardinality() - (windowFn == null ? 0 : 1);
    // This is similar to what Calcite does.If groupCount is zero then then we have only one value
    // per window for unbounded and we have only one value for bounded. e.g select count(*) from A
    // If group count is none zero then more column we include in the group by, more rows will be
    // preserved.
    return (groupCount == 0)
        ? NodeStats.create(
            Math.min(inputEstimate.getRowCount(), 1d),
            inputEstimate.getRate() / inputEstimate.getWindow(),
            1d)
        : inputEstimate.multiply(1.0 - Math.pow(.5, groupCount));
  }

  private NodeStats computeWindowingCostEffect(NodeStats inputStat) {
    if (windowFn == null) {
      return inputStat;
    }
    WindowFn w = windowFn;
    double multiplicationFactor = 1;
    // If the window is SlidingWindow, the number of tuples will increase. (Because, some of the
    // tuples repeat in multiple windows).
    if (w instanceof SlidingWindows) {
      multiplicationFactor =
          ((double) ((SlidingWindows) w).getSize().getStandardSeconds())
              / ((SlidingWindows) w).getPeriod().getStandardSeconds();
    }

    return NodeStats.create(
        inputStat.getRowCount() * multiplicationFactor,
        inputStat.getRate() * multiplicationFactor,
        BeamIOSourceRel.CONSTANT_WINDOW_SIZE);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    if (this.windowFn != null) {
      WindowFn windowFn = this.windowFn;
      String window = windowFn.getClass().getSimpleName() + "($" + String.valueOf(windowFieldIndex);
      if (windowFn instanceof FixedWindows) {
        FixedWindows fn = (FixedWindows) windowFn;
        window = window + ", " + fn.getSize().toString() + ", " + fn.getOffset().toString();
      } else if (windowFn instanceof SlidingWindows) {
        SlidingWindows fn = (SlidingWindows) windowFn;
        window =
            window
                + ", "
                + fn.getPeriod().toString()
                + ", "
                + fn.getSize().toString()
                + ", "
                + fn.getOffset().toString();
      } else if (windowFn instanceof Sessions) {
        Sessions fn = (Sessions) windowFn;
        window = window + ", " + fn.getGapDuration().toString();
      } else {
        throw new UnsupportedOperationException(
            "Unknown window function " + windowFn.getClass().getSimpleName());
      }
      window = window + ")";
      pw.item("window", window);
    }
    return pw;
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    Schema outputSchema = CalciteUtils.toSchema(getRowType());
    List<FieldAggregation> aggregationAdapters =
        getNamedAggCalls().stream()
            .map(aggCall -> new FieldAggregation(aggCall.getKey(), aggCall.getValue()))
            .collect(toList());

    return new Transform(
        windowFn, windowFieldIndex, getGroupSet(), aggregationAdapters, outputSchema);
  }

  private static class FieldAggregation implements Serializable {
    final List<Integer> inputs;
    final CombineFn combineFn;
    final Field outputField;

    FieldAggregation(AggregateCall call, String alias) {
      inputs = call.getArgList();
      outputField = CalciteUtils.toField(alias, call.getType());
      combineFn =
          AggregationCombineFnAdapter.createCombineFn(
              call, outputField, call.getAggregation().getName());
    }
  }

  private static class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    private final List<Integer> keyFieldsIds;
    private Schema outputSchema;
    private WindowFn<Row, IntervalWindow> windowFn;
    private int windowFieldIndex;
    private List<FieldAggregation> fieldAggregations;

    private Transform(
        WindowFn<Row, IntervalWindow> windowFn,
        int windowFieldIndex,
        ImmutableBitSet groupSet,
        List<FieldAggregation> fieldAggregations,
        Schema outputSchema) {
      this.windowFn = windowFn;
      this.windowFieldIndex = windowFieldIndex;
      this.fieldAggregations = fieldAggregations;
      this.outputSchema = outputSchema;
      this.keyFieldsIds =
          groupSet.asList().stream().filter(i -> i != windowFieldIndex).collect(toList());
    }

    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      checkArgument(
          pinput.size() == 1,
          "Wrong number of inputs for %s: %s",
          BeamAggregationRel.class.getSimpleName(),
          pinput);
      PCollection<Row> upstream = pinput.get(0);
      PCollection<Row> windowedStream = upstream;
      if (windowFn != null) {
        windowedStream = assignTimestampsAndWindow(upstream);
      }

      validateWindowIsSupported(windowedStream);

      org.apache.beam.sdk.schemas.transforms.Group.ByFields<Row> byFields =
          org.apache.beam.sdk.schemas.transforms.Group.byFieldIds(keyFieldsIds);
      org.apache.beam.sdk.schemas.transforms.Group.CombineFieldsByFields<Row> combined = null;
      for (FieldAggregation fieldAggregation : fieldAggregations) {
        List<Integer> inputs = fieldAggregation.inputs;
        CombineFn combineFn = fieldAggregation.combineFn;
        if (inputs.size() > 1 || inputs.isEmpty()) {
          // In this path we extract a Row (an empty row if inputs.isEmpty).
          combined =
              (combined == null)
                  ? byFields.aggregateFieldsById(inputs, combineFn, fieldAggregation.outputField)
                  : combined.aggregateFieldsById(inputs, combineFn, fieldAggregation.outputField);
        } else {
          // Combining over a single field, so extract just that field.
          combined =
              (combined == null)
                  ? byFields.aggregateField(inputs.get(0), combineFn, fieldAggregation.outputField)
                  : combined.aggregateField(inputs.get(0), combineFn, fieldAggregation.outputField);
        }
      }

      PTransform<PCollection<Row>, PCollection<Row>> combiner = combined;
      boolean ignoreValues = false;
      if (combiner == null) {
        // If no field aggregations were specified, we run a constant combiner that always returns
        // a single empty row for each key. This is used by the SELECT DISTINCT query plan - in this
        // case a group by is generated to determine unique keys, and a constant null combiner is
        // used.
        combiner =
            byFields.aggregateField(
                "*",
                AggregationCombineFnAdapter.createConstantCombineFn(),
                Field.of(
                    "e",
                    FieldType.row(AggregationCombineFnAdapter.EMPTY_SCHEMA).withNullable(true)));
        ignoreValues = true;
      }

      boolean verifyRowValues =
          pinput.getPipeline().getOptions().as(BeamSqlPipelineOptions.class).getVerifyRowValues();
      return windowedStream
          .apply(combiner)
          .apply(
              "mergeRecord",
              ParDo.of(mergeRecord(outputSchema, windowFieldIndex, ignoreValues, verifyRowValues)))
          .setRowSchema(outputSchema);
    }

    /** Extract timestamps from the windowFieldIndex, then window into windowFns. */
    private PCollection<Row> assignTimestampsAndWindow(PCollection<Row> upstream) {
      PCollection<Row> windowedStream;
      windowedStream =
          upstream
              .apply(
                  "assignEventTimestamp",
                  WithTimestamps.<Row>of(row -> row.getDateTime(windowFieldIndex).toInstant())
                      .withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)))
              .setCoder(upstream.getCoder())
              .apply(Window.into(windowFn));
      return windowedStream;
    }

    /**
     * Performs the same check as {@link GroupByKey}, provides more context in exception.
     *
     * <p>Verifies that the input PCollection is bounded, or that there is windowing/triggering
     * being used. Without this, the watermark (at end of global window) will never be reached.
     *
     * <p>Throws {@link UnsupportedOperationException} if validation fails.
     */
    private void validateWindowIsSupported(PCollection<Row> upstream) {
      WindowingStrategy<?, ?> windowingStrategy = upstream.getWindowingStrategy();
      if (windowingStrategy.getWindowFn() instanceof GlobalWindows
          && windowingStrategy.getTrigger() instanceof DefaultTrigger
          && upstream.isBounded() != BOUNDED) {

        throw new UnsupportedOperationException(
            "Please explicitly specify windowing in SQL query using HOP/TUMBLE/SESSION functions "
                + "(default trigger will be used in this case). "
                + "Unbounded input with global windowing and default trigger is not supported "
                + "in Beam SQL aggregations. "
                + "See GroupByKey section in Beam Programming Guide");
      }
    }

    static DoFn<Row, Row> mergeRecord(
        Schema outputSchema,
        int windowStartFieldIndex,
        boolean ignoreValues,
        boolean verifyRowValues) {
      return new DoFn<Row, Row>() {
        @ProcessElement
        public void processElement(
            @Element Row kvRow, BoundedWindow window, OutputReceiver<Row> o) {
          int capacity =
              kvRow.getRow(0).getFieldCount()
                  + (!ignoreValues ? kvRow.getRow(1).getFieldCount() : 0);
          List<Object> fieldValues = Lists.newArrayListWithCapacity(capacity);

          fieldValues.addAll(kvRow.getRow(0).getValues());
          if (!ignoreValues) {
            fieldValues.addAll(kvRow.getRow(1).getValues());
          }

          if (windowStartFieldIndex != -1) {
            fieldValues.add(windowStartFieldIndex, ((IntervalWindow) window).start());
          }

          Row row =
              verifyRowValues
                  ? Row.withSchema(outputSchema).addValues(fieldValues).build()
                  : Row.withSchema(outputSchema).attachValues(fieldValues);
          o.output(row);
        }
      };
    }
  }

  @Override
  public Aggregate copy(
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return new BeamAggregationRel(
        getCluster(), traitSet, input, groupSet, groupSets, aggCalls, windowFn, windowFieldIndex);
  }
}
