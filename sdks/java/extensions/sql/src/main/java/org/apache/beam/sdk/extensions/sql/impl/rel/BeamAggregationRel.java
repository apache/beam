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
import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.apache.beam.sdk.values.PCollection.IsBounded.BOUNDED;
import static org.apache.beam.sdk.values.Row.toRow;

import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamAggregationTransforms;
import org.apache.beam.sdk.extensions.sql.impl.transform.agg.AggregationCombineFnAdapter;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
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
  private WindowFn<Row, IntervalWindow> windowFn;
  private final int windowFieldIndex;

  public BeamAggregationRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls,
      WindowFn<Row, IntervalWindow> windowFn,
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
    Schema inputSchema = CalciteUtils.toSchema(getInput().getRowType());
    Schema outputSchema = CalciteUtils.toSchema(getRowType());
    List<AggregationCombineFnAdapter> aggregationAdapters =
        getNamedAggCalls()
            .stream()
            .map(aggCall -> AggregationCombineFnAdapter.of(aggCall, inputSchema))
            .collect(toList());

    return new Transform(
        windowFn, windowFieldIndex, inputSchema, getGroupSet(), aggregationAdapters, outputSchema);
  }

  private static class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    private final List<Integer> keyFieldsIds;
    private Schema outputSchema;
    private Schema keySchema;
    private SchemaCoder<Row> keyCoder;
    private WindowFn<Row, IntervalWindow> windowFn;
    private int windowFieldIndex;
    private List<AggregationCombineFnAdapter> aggregationCalls;
    private SchemaCoder<Row> aggCoder;

    private Transform(
        WindowFn<Row, IntervalWindow> windowFn,
        int windowFieldIndex,
        Schema inputSchema,
        ImmutableBitSet groupSet,
        List<AggregationCombineFnAdapter> aggregationCalls,
        Schema outputSchema) {

      this.windowFn = windowFn;
      this.windowFieldIndex = windowFieldIndex;
      this.aggregationCalls = aggregationCalls;

      this.outputSchema = outputSchema;

      this.keySchema =
          groupSet
              .asList()
              .stream()
              .filter(i -> i != windowFieldIndex)
              .map(inputSchema::getField)
              .collect(toSchema());

      this.keyFieldsIds =
          groupSet.asList().stream().filter(i -> i != windowFieldIndex).collect(toList());

      this.keyCoder = SchemaCoder.of(keySchema);
      this.aggCoder =
          SchemaCoder.of(
              aggregationCalls.stream().map(aggCall -> aggCall.field()).collect(toSchema()));
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
        upstream =
            upstream
                .apply(
                    "assignEventTimestamp",
                    WithTimestamps.<Row>of(row -> row.getDateTime(windowFieldIndex).toInstant())
                        .withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)))
                .setCoder(upstream.getCoder());
        windowedStream = upstream.apply(Window.into(windowFn));
      }

      validateWindowIsSupported(windowedStream);

      PCollection<KV<Row, Row>> exCombineByStream =
          windowedStream
              .apply(
                  "exCombineBy",
                  WithKeys.of(
                      row -> keyFieldsIds.stream().map(row::getValue).collect(toRow(keySchema))))
              .setCoder(KvCoder.of(keyCoder, upstream.getCoder()));

      PCollection<KV<Row, Row>> aggregatedStream =
          exCombineByStream
              .apply(
                  "combineBy",
                  Combine.perKey(
                      new BeamAggregationTransforms.MultiAggregationCombineFn(aggregationCalls)))
              .setCoder(KvCoder.of(keyCoder, aggCoder));

      PCollection<Row> mergedStream =
          aggregatedStream.apply(
              "mergeRecord",
              ParDo.of(
                  new BeamAggregationTransforms.MergeAggregationRecord(
                      outputSchema, windowFieldIndex)));
      mergedStream.setRowSchema(outputSchema);

      return mergedStream;
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
