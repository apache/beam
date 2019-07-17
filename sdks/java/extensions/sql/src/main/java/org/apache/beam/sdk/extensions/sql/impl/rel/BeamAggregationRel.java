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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.impl.transform.agg.AggregationCombineFnAdapter;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.joda.time.Duration;

/** {@link BeamRelNode} to replace a {@link Aggregate} node. */
public class BeamAggregationRel extends Aggregate implements BeamRelNode {
  private @Nullable WindowFn<Row, IntervalWindow> windowFn;
  private final int windowFieldIndex;

  public BeamAggregationRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls,
      @Nullable WindowFn<Row, IntervalWindow> windowFn,
      int windowFieldIndex) {

    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);

    this.windowFn = windowFn;
    this.windowFieldIndex = windowFieldIndex;
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
        throw new RuntimeException(
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

      PTransform<PCollection<Row>, PCollection<KV<Row, Row>>> combiner = combined;
      if (combiner == null) {
        // If no field aggregations were specified, we run a constant combiner that always returns
        // a single empty row for each key. This is used by the SELECT DISTINCT query plan - in this
        // case a group by is generated to determine unique keys, and a constant null combiner is
        // used.
        combiner = byFields.aggregate(AggregationCombineFnAdapter.createConstantCombineFn());
      }

      return windowedStream
          .apply(combiner)
          .apply("mergeRecord", ParDo.of(mergeRecord(outputSchema, windowFieldIndex)))
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

    static DoFn<KV<Row, Row>, Row> mergeRecord(Schema outputSchema, int windowStartFieldIndex) {
      return new DoFn<KV<Row, Row>, Row>() {
        @ProcessElement
        public void processElement(
            @Element KV<Row, Row> kvRow, BoundedWindow window, OutputReceiver<Row> o) {
          List<Object> fieldValues =
              Lists.newArrayListWithCapacity(
                  kvRow.getKey().getValues().size() + kvRow.getValue().getValues().size());

          fieldValues.addAll(kvRow.getKey().getValues());
          fieldValues.addAll(kvRow.getValue().getValues());

          if (windowStartFieldIndex != -1) {
            fieldValues.add(windowStartFieldIndex, ((IntervalWindow) window).start());
          }

          o.output(Row.withSchema(outputSchema).addValues(fieldValues).build());
        }
      };
    }
  }

  @Override
  public Aggregate copy(
      RelTraitSet traitSet,
      RelNode input,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return new BeamAggregationRel(
        getCluster(),
        traitSet,
        input,
        indicator,
        groupSet,
        groupSets,
        aggCalls,
        windowFn,
        windowFieldIndex);
  }
}
