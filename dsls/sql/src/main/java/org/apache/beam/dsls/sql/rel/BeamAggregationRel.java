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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.dsls.sql.BeamSqlEnv;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowCoder;
import org.apache.beam.dsls.sql.transform.BeamAggregationTransforms;
import org.apache.beam.dsls.sql.utils.CalciteUtils;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
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
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.joda.time.Duration;

/**
 * {@link BeamRelNode} to replace a {@link Aggregate} node.
 *
 */
public class BeamAggregationRel extends Aggregate implements BeamRelNode {
  private int windowFieldIdx = -1;
  private WindowFn<BeamSqlRow, BoundedWindow> windowFn;
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
  public PCollection<BeamSqlRow> buildBeamPipeline(PCollectionTuple inputPCollections
      , BeamSqlEnv sqlEnv) throws Exception {
    RelNode input = getInput();
    String stageName = BeamSqlRelUtils.getStageName(this);

    PCollection<BeamSqlRow> upstream =
        BeamSqlRelUtils.getBeamRelInput(input).buildBeamPipeline(inputPCollections, sqlEnv);
    if (windowFieldIdx != -1) {
      upstream = upstream.apply(stageName + "_assignEventTimestamp", WithTimestamps
          .<BeamSqlRow>of(new BeamAggregationTransforms.WindowTimestampFn(windowFieldIdx)))
          .setCoder(upstream.getCoder());
    }

    PCollection<BeamSqlRow> windowStream = upstream.apply(stageName + "_window",
        Window.<BeamSqlRow>into(windowFn)
        .triggering(trigger)
        .withAllowedLateness(allowedLatence)
        .accumulatingFiredPanes());

    BeamSqlRowCoder keyCoder = new BeamSqlRowCoder(exKeyFieldsSchema(input.getRowType()));
    PCollection<KV<BeamSqlRow, BeamSqlRow>> exGroupByStream = windowStream.apply(
        stageName + "_exGroupBy",
        WithKeys
            .of(new BeamAggregationTransforms.AggregationGroupByKeyFn(
                windowFieldIdx, groupSet)))
        .setCoder(KvCoder.<BeamSqlRow, BeamSqlRow>of(keyCoder, upstream.getCoder()));

    PCollection<KV<BeamSqlRow, Iterable<BeamSqlRow>>> groupedStream = exGroupByStream
        .apply(stageName + "_groupBy", GroupByKey.<BeamSqlRow, BeamSqlRow>create())
        .setCoder(KvCoder.<BeamSqlRow, Iterable<BeamSqlRow>>of(keyCoder,
            IterableCoder.<BeamSqlRow>of(upstream.getCoder())));

    BeamSqlRowCoder aggCoder = new BeamSqlRowCoder(exAggFieldsSchema());
    PCollection<KV<BeamSqlRow, BeamSqlRow>> aggregatedStream = groupedStream.apply(
        stageName + "_aggregation",
        Combine.<BeamSqlRow, BeamSqlRow, BeamSqlRow>groupedValues(
            new BeamAggregationTransforms.AggregationCombineFn(getAggCallList(),
                CalciteUtils.toBeamRecordType(input.getRowType()))))
        .setCoder(KvCoder.<BeamSqlRow, BeamSqlRow>of(keyCoder, aggCoder));

    PCollection<BeamSqlRow> mergedStream = aggregatedStream.apply(stageName + "_mergeRecord",
        ParDo.of(new BeamAggregationTransforms.MergeAggregationRecord(
            CalciteUtils.toBeamRecordType(getRowType()), getAggCallList())));
    mergedStream.setCoder(new BeamSqlRowCoder(CalciteUtils.toBeamRecordType(getRowType())));

    return mergedStream;
  }

  /**
   * Type of sub-rowrecord used as Group-By keys.
   */
  private BeamSqlRecordType exKeyFieldsSchema(RelDataType relDataType) {
    BeamSqlRecordType inputRecordType = CalciteUtils.toBeamRecordType(relDataType);
    List<String> fieldNames = new ArrayList<>();
    List<Integer> fieldTypes = new ArrayList<>();
    for (int i : groupSet.asList()) {
      if (i != windowFieldIdx) {
        fieldNames.add(inputRecordType.getFieldsName().get(i));
        fieldTypes.add(inputRecordType.getFieldsType().get(i));
      }
    }
    return BeamSqlRecordType.create(fieldNames, fieldTypes);
  }

  /**
   * Type of sub-rowrecord, that represents the list of aggregation fields.
   */
  private BeamSqlRecordType exAggFieldsSchema() {
    List<String> fieldNames = new ArrayList<>();
    List<Integer> fieldTypes = new ArrayList<>();
    for (AggregateCall ac : getAggCallList()) {
      fieldNames.add(ac.name);
      fieldTypes.add(CalciteUtils.toJavaType(ac.type.getSqlTypeName()));
    }

    return BeamSqlRecordType.create(fieldNames, fieldTypes);
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
