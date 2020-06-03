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
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
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
import org.joda.time.Duration;

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

  private class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionList<Row> input) {
      checkArgument(
          input.size() == 1,
          "Wrong number of inputs for %s, expected 1 input but received: %s",
          BeamTableFunctionScanRel.class.getSimpleName(),
          input);
      String operatorName = ((RexCall) getCall()).getOperator().getName();
      checkArgument(
          operatorName.equals("TUMBLE"),
          "Only support TUMBLE table-valued function. Current operator: %s",
          operatorName);
      RexCall call = ((RexCall) getCall());
      RexInputRef wmCol = (RexInputRef) call.getOperands().get(1);
      PCollection<Row> upstream = input.get(0);
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
    }

    /** Extract timestamps from the windowFieldIndex, then window into windowFns. */
    private PCollection<Row> assignTimestampsAndWindow(
        PCollection<Row> upstream, int windowFieldIndex, WindowFn<Row, IntervalWindow> windowFn) {
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
      IntervalWindow window = windowFn.assignWindow(row.getDateTime(windowFieldIndex).toInstant());
      Row.Builder builder = Row.withSchema(outputSchema);
      builder.addValues(row.getValues());
      builder.addValue(window.start());
      builder.addValue(window.end());
      c.output(builder.build());
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
