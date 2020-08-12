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

import static org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.impl.TVFSlidingWindowFn;
import org.apache.beam.sdk.extensions.sql.impl.ZetaSqlUserDefinedSQLNativeTableValuedFunction;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.impl.utils.TVFStreamingUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
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
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * BeamRelNode to replace {@code TableFunctionScan}. Currently this class limits to support
 * table-valued function for streaming windowing.
 */
public class BeamTableFunctionScanRel extends TableFunctionScan implements BeamRelNode {
  public BeamTableFunctionScanRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelNode> inputs,
      RexNode rexCall,
      Type elementType,
      RelDataType rowType,
      Set<RelColumnMapping> columnMappings) {
    super(cluster, traitSet, inputs, rexCall, elementType, rowType, columnMappings);
  }

  @Override
  public TableFunctionScan copy(
      RelTraitSet traitSet,
      List<RelNode> list,
      RexNode rexNode,
      Type type,
      RelDataType relDataType,
      Set<RelColumnMapping> set) {
    return new BeamTableFunctionScanRel(
        getCluster(), traitSet, list, rexNode, type, relDataType, set);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new Transform();
  }

  /** Provides a function that produces a PCollection based on TVF and upstream PCollection. */
  private interface TVFToPTransform {
    PCollection<Row> toPTransform(RexCall call, PCollection<Row> upstream);
  }

  private class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {
    private TVFToPTransform tumbleToPTransform =
        (call, upstream) -> {
          RexInputRef wmCol = (RexInputRef) call.getOperands().get(1);
          Schema outputSchema = CalciteUtils.toSchema(getRowType());
          FixedWindows windowFn = FixedWindows.of(durationParameter(call.getOperands().get(2)));
          PCollection<Row> streamWithWindowMetadata =
              upstream
                  .apply(ParDo.of(new FixedWindowDoFn(windowFn, wmCol.getIndex(), outputSchema)))
                  .setRowSchema(outputSchema);

          PCollection<Row> windowedStream =
              assignTimestampsAndWindow(
                  streamWithWindowMetadata, wmCol.getIndex(), (WindowFn) windowFn);

          return windowedStream;
        };

    private TVFToPTransform hopToPTransform =
        (call, upstream) -> {
          RexInputRef wmCol = (RexInputRef) call.getOperands().get(1);
          Schema outputSchema = CalciteUtils.toSchema(getRowType());

          Duration period = durationParameter(call.getOperands().get(2));
          Duration size = durationParameter(call.getOperands().get(3));
          SlidingWindows windowFn = SlidingWindows.of(size).every(period);
          PCollection<Row> streamWithWindowMetadata =
              upstream
                  .apply(ParDo.of(new SlidingWindowDoFn(windowFn, wmCol.getIndex(), outputSchema)))
                  .setRowSchema(outputSchema);

          // Sliding window needs this special WindowFn to assign windows based on window_start,
          // window_end metadata.
          WindowFn specialWindowFn = TVFSlidingWindowFn.of(size, period);

          PCollection<Row> windowedStream =
              assignTimestampsAndWindow(
                  streamWithWindowMetadata, wmCol.getIndex(), specialWindowFn);

          return windowedStream;
        };

    private TVFToPTransform sessionToPTransform =
        (call, upstream) -> {
          RexInputRef wmCol = (RexInputRef) call.getOperands().get(1);
          Duration gap = durationParameter(call.getOperands().get(2));
          List<Integer> keyIndex = new ArrayList<>();
          for (RexNode node : call.getOperands().subList(3, call.getOperands().size())) {
            keyIndex.add(((RexInputRef) node).getIndex());
          }

          Sessions sessions = Sessions.withGapDuration(gap);

          PCollection<Row> windowedStream =
              assignTimestampsAndWindow(upstream, wmCol.getIndex(), (WindowFn) sessions);
          Schema inputSchema = upstream.getSchema();
          Schema keySchema = getKeySchema(inputSchema, keyIndex);
          Schema outputSchema = CalciteUtils.toSchema(getRowType());
          // To extract session's window metadata, we apply a GroupByKey because session is merging
          // window. After GBK, SessionWindowDoFn will help extract window_start, window_end
          // metadata.
          PCollection<Row> streamWithWindowMetadata =
              windowedStream
                  .apply("assign_session_key", ParDo.of(new SessionKeyDoFn(keySchema, keyIndex)))
                  .setCoder(KvCoder.of(RowCoder.of(keySchema), upstream.getCoder()))
                  .apply(GroupByKey.create())
                  .apply(ParDo.of(new SessionWindowDoFn(outputSchema)))
                  .setRowSchema(outputSchema);

          // Because the GBK above has consumed session window, we can re-window into global window.
          // Future aggregations or join can apply based on window metadata.
          PCollection<Row> reWindowedStream =
              streamWithWindowMetadata.apply(
                  "reWindowIntoGlobalWindow", Window.into(new GlobalWindows()));
          return reWindowedStream;
        };

    private final ImmutableMap<String, TVFToPTransform> tvfToPTransformMap =
        ImmutableMap.of(
            TVFStreamingUtils.FIXED_WINDOW_TVF,
            tumbleToPTransform,
            TVFStreamingUtils.SLIDING_WINDOW_TVF,
            hopToPTransform,
            TVFStreamingUtils.SESSION_WINDOW_TVF,
            sessionToPTransform);

    @Override
    public PCollection<Row> expand(PCollectionList<Row> input) {
      checkArgument(
          input.size() == 1,
          "Wrong number of inputs for %s, expected 1 input but received: %s",
          BeamTableFunctionScanRel.class.getSimpleName(),
          input);
      String operatorName = ((RexCall) getCall()).getOperator().getName();

      // builtin TVF uses existing PTransform implementations.
      if (tvfToPTransformMap.keySet().contains(operatorName)) {
        return tvfToPTransformMap
            .get(operatorName)
            .toPTransform(((RexCall) getCall()), input.get(0));
      }

      // ZetaSQL pure SQL TVF should pass through input to output.
      if (((RexCall) getCall()).getOperator()
          instanceof ZetaSqlUserDefinedSQLNativeTableValuedFunction) {
        return input.get(0);
      }

      throw new IllegalArgumentException(
          String.format("Does not support table_valued function: %s", operatorName));
    }

    private Schema getKeySchema(Schema inputSchema, List<Integer> keys) {
      List<Field> fields = new ArrayList<>();
      for (Integer i : keys) {
        fields.add(inputSchema.getField(i));
      }
      return Schema.builder().addFields(fields).build();
    }

    /** Extract timestamps from the windowFieldIndex, then window into windowFns. */
    private PCollection<Row> assignTimestampsAndWindow(
        PCollection<Row> upstream, int windowFieldIndex, WindowFn<Row, IntervalWindow> windowFn) {
      PCollection<Row> windowedStream;
      windowedStream =
          upstream
              .apply(
                  "assignEventTimestamp",
                  WithTimestamps.<Row>of(
                          row ->
                              Instant.ofEpochMilli(
                                  row.getLogicalTypeValue(windowFieldIndex, java.time.Instant.class)
                                      .toEpochMilli()))
                      .withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)))
              .setCoder(upstream.getCoder())
              .apply(Window.into(windowFn));
      return windowedStream;
    }
  }

  private Duration durationParameter(RexNode node) {
    return Duration.millis(longValue(node));
  }

  private long longValue(RexNode operand) {
    if (operand instanceof RexLiteral) {
      return ((Number) RexLiteral.value(operand)).longValue();
    } else {
      throw new IllegalArgumentException(String.format("[%s] is not valid.", operand));
    }
  }

  private static class SessionKeyDoFn extends DoFn<Row, KV<Row, Row>> {
    private final Schema keySchema;
    private final List<Integer> keyIndex;

    public SessionKeyDoFn(Schema keySchema, List<Integer> keyIndex) {
      this.keySchema = keySchema;
      this.keyIndex = keyIndex;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Row row = c.element();
      Row.Builder builder = Row.withSchema(keySchema);
      for (Integer i : keyIndex) {
        builder.addValue(row.getValue(i));
      }
      Row keyRow = builder.build();
      c.output(KV.of(keyRow, row));
    }
  }

  private static class FixedWindowDoFn extends DoFn<Row, Row> {
    private final int windowFieldIndex;
    private final FixedWindows windowFn;
    private final Schema outputSchema;

    public FixedWindowDoFn(FixedWindows windowFn, int windowFieldIndex, Schema schema) {
      this.windowFn = windowFn;
      this.windowFieldIndex = windowFieldIndex;
      this.outputSchema = schema;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Row row = c.element();
      IntervalWindow window =
          windowFn.assignWindow(
              Instant.ofEpochMilli(
                  row.getLogicalTypeValue(windowFieldIndex, java.time.Instant.class)
                      .toEpochMilli()));
      Row.Builder builder = Row.withSchema(outputSchema);
      builder.addValues(row.getValues());
      builder.addValue(java.time.Instant.ofEpochMilli(window.start().getMillis()));
      builder.addValue(java.time.Instant.ofEpochMilli(window.end().getMillis()));
      c.output(builder.build());
    }
  }

  private static class SlidingWindowDoFn extends DoFn<Row, Row> {
    private final int windowFieldIndex;
    private final SlidingWindows windowFn;
    private final Schema outputSchema;

    public SlidingWindowDoFn(SlidingWindows windowFn, int windowFieldIndex, Schema schema) {
      this.windowFn = windowFn;
      this.windowFieldIndex = windowFieldIndex;
      this.outputSchema = schema;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Row row = c.element();
      Collection<IntervalWindow> windows =
          windowFn.assignWindows(
              Instant.ofEpochMilli(
                  row.getLogicalTypeValue(windowFieldIndex, java.time.Instant.class)
                      .toEpochMilli()));
      for (IntervalWindow window : windows) {
        Row.Builder builder = Row.withSchema(outputSchema);
        builder.addValues(row.getValues());
        builder.addValue(java.time.Instant.ofEpochMilli(window.start().getMillis()));
        builder.addValue(java.time.Instant.ofEpochMilli(window.end().getMillis()));
        c.output(builder.build());
      }
    }
  }

  private static class SessionWindowDoFn extends DoFn<KV<Row, Iterable<Row>>, Row> {
    private final Schema outputSchema;

    public SessionWindowDoFn(Schema schema) {
      this.outputSchema = schema;
    }

    @ProcessElement
    public void processElement(
        @Element KV<Row, Iterable<Row>> element, BoundedWindow window, OutputReceiver<Row> out) {
      IntervalWindow intervalWindow = (IntervalWindow) window;
      for (Row cur : element.getValue()) {
        Row.Builder builder =
            Row.withSchema(outputSchema)
                .addValues(cur.getValues())
                .addValue(java.time.Instant.ofEpochMilli(intervalWindow.start().getMillis()))
                .addValue(java.time.Instant.ofEpochMilli(intervalWindow.end().getMillis()));
        out.output(builder.build());
      }
    }
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {
    return BeamSqlRelUtils.getNodeStats(getInput(0), mq);
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    NodeStats inputEstimates = BeamSqlRelUtils.getNodeStats(getInput(0), mq);

    final double rowSize = getRowType().getFieldCount();
    final double cpu = inputEstimates.getRowCount() * rowSize;
    final double cpuRate = inputEstimates.getRate() * inputEstimates.getWindow() * rowSize;
    return BeamCostModel.FACTORY.makeCost(cpu, cpuRate);
  }
}
