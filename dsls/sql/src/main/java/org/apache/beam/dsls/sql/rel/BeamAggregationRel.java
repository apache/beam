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
package org.apache.beam.dsls.sql.rel;

import java.util.List;
import org.apache.beam.dsls.sql.planner.BeamPipelineCreator;
import org.apache.beam.dsls.sql.planner.BeamSQLRelUtils;
import org.apache.beam.dsls.sql.schema.BeamSQLRecordType;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.dsls.sql.transform.BeamAggregationTransforms;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.joda.time.Duration;

/**
 * {@link BeamRelNode} to replace a {@link Aggregate} node.
 *
 */
public class BeamAggregationRel extends Aggregate implements BeamRelNode {
  private int windowFieldIdx = -1;
  private WindowFn<BeamSQLRow, BoundedWindow> windowFn;
  private Trigger trigger;
  private Duration allowedLatence = Duration.ZERO;

  public BeamAggregationRel(RelOptCluster cluster, RelTraitSet traits
      , RelNode child, boolean indicator,
      ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls
      , WindowFn windowFn, Trigger trigger, int windowFieldIdx, Duration allowedLatence) {
    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
    this.windowFn = windowFn;
    this.trigger = trigger;
    this.windowFieldIdx = windowFieldIdx;
    this.allowedLatence = allowedLatence;
  }

  @Override
  public Pipeline buildBeamPipeline(BeamPipelineCreator planCreator) throws Exception {
    RelNode input = getInput();
    BeamSQLRelUtils.getBeamRelInput(input).buildBeamPipeline(planCreator);

    String stageName = BeamSQLRelUtils.getStageName(this);

    PCollection<BeamSQLRow> upstream = planCreator.popUpstream();
    if (windowFieldIdx != -1) {
      upstream = upstream.apply("assignEventTimestamp", WithTimestamps
          .<BeamSQLRow>of(new BeamAggregationTransforms.WindowTimestampFn(windowFieldIdx)));
    }

    PCollection<BeamSQLRow> windowStream = upstream.apply("window",
        Window.<BeamSQLRow>into(windowFn)
        .triggering(trigger)
        .withAllowedLateness(allowedLatence)
        .accumulatingFiredPanes());

    //1. extract fields in group-by key part
    PCollection<KV<BeamSQLRow, BeamSQLRow>> exGroupByStream = windowStream.apply("exGroupBy",
        WithKeys
            .of(new BeamAggregationTransforms.AggregationGroupByKeyFn(windowFieldIdx, groupSet)));

    //2. apply a GroupByKey.
    PCollection<KV<BeamSQLRow, Iterable<BeamSQLRow>>> groupedStream = exGroupByStream
        .apply("groupBy", GroupByKey.<BeamSQLRow, BeamSQLRow>create());

    //3. run aggregation functions
    PCollection<KV<BeamSQLRow, BeamSQLRow>> aggregatedStream = groupedStream.apply("aggregation",
        Combine.<BeamSQLRow, BeamSQLRow, BeamSQLRow>groupedValues(
            new BeamAggregationTransforms.AggregationCombineFn(getAggCallList(),
                BeamSQLRecordType.from(input.getRowType()))));

    //4. flat KV to a single record
    PCollection<BeamSQLRow> mergedStream = aggregatedStream.apply("mergeRecord",
        ParDo.of(new BeamAggregationTransforms.MergeAggregationRecord(
            BeamSQLRecordType.from(getRowType()), getAggCallList())));
    planCreator.pushUpstream(mergedStream);

    return planCreator.getPipeline();
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator
      , ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    return new BeamAggregationRel(getCluster(), traitSet, input, indicator
        , groupSet, groupSets, aggCalls, windowFn, trigger, windowFieldIdx, allowedLatence);
  }

  public void setWindowFn(WindowFn windowFn) {
    this.windowFn = windowFn;
  }

  public void setTrigger(Trigger trigger) {
    this.trigger = trigger;
  }

  public RelWriter explainTerms(RelWriter pw) {
    // We skip the "groups" element if it is a singleton of "group".
    pw.item("group", groupSet)
        .itemIf("window", windowFn, windowFn != null)
        .itemIf("trigger", trigger, trigger != null)
        .itemIf("event_time", windowFieldIdx, windowFieldIdx != -1)
        .itemIf("groups", groupSets, getGroupType() != Group.SIMPLE)
        .itemIf("indicator", indicator, indicator)
        .itemIf("aggs", aggCalls, pw.nest());
    if (!pw.nest()) {
      for (Ord<AggregateCall> ord : Ord.zip(aggCalls)) {
        pw.item(Util.first(ord.e.name, "agg#" + ord.i), ord.e);
      }
    }
    return pw;
  }

}
