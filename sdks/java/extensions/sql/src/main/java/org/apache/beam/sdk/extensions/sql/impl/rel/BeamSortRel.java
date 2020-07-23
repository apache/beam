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

import static org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelCollationImpl;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelFieldCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Sort;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamRelNode} to replace a {@code Sort} node.
 *
 * <p>Since Beam does not fully support global sort, it uses {@link Top} to implement the {@code
 * Sort} algebra. The following types of ORDER BY are supported:
 *
 * <pre>{@code
 * SELECT * FROM t ORDER BY id DESC LIMIT 10;
 * SELECT * FROM t ORDER BY id DESC LIMIT 10 OFFSET 5;
 * }</pre>
 *
 * <p>but an ORDER BY without a LIMIT is NOT supported. For example, the following will throw an
 * exception:
 *
 * <pre>{@code
 * SELECT * FROM t ORDER BY id DESC;
 * }</pre>
 *
 * <h3>Constraints</h3>
 *
 * <ul>
 *   <li>Due to the constraints of {@link Top}, the result of a ORDER BY LIMIT must fit into the
 *       memory of a single machine.
 *   <li>Since WINDOW (HOP, TUMBLE, SESSION, etc.) is always associated with `GroupBy`, it does not
 *       make much sense to use ORDER BY with WINDOW.
 * </ul>
 */
public class BeamSortRel extends Sort implements BeamRelNode {
  private List<Integer> fieldIndices = new ArrayList<>();
  private List<Boolean> orientation = new ArrayList<>();
  private List<Boolean> nullsFirst = new ArrayList<>();

  private int startIndex = 0;
  private int count;

  public BeamSortRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RelCollation collation,
      RexNode offset,
      RexNode fetch) {
    super(cluster, traits, child, collation, offset, fetch);

    List<RexNode> fieldExps = getChildExps();
    RelCollationImpl collationImpl = (RelCollationImpl) collation;
    List<RelFieldCollation> collations = collationImpl.getFieldCollations();
    for (int i = 0; i < fieldExps.size(); i++) {
      RexNode fieldExp = fieldExps.get(i);
      RexInputRef inputRef = (RexInputRef) fieldExp;
      fieldIndices.add(inputRef.getIndex());
      orientation.add(collations.get(i).getDirection() == RelFieldCollation.Direction.ASCENDING);

      RelFieldCollation.NullDirection rawNullDirection = collations.get(i).nullDirection;
      if (rawNullDirection == RelFieldCollation.NullDirection.UNSPECIFIED) {
        rawNullDirection = collations.get(i).getDirection().defaultNullDirection();
      }
      nullsFirst.add(rawNullDirection == RelFieldCollation.NullDirection.FIRST);
    }

    if (fetch == null) {
      throw new UnsupportedOperationException("ORDER BY without a LIMIT is not supported!");
    }

    RexLiteral fetchLiteral = (RexLiteral) fetch;
    count = ((BigDecimal) fetchLiteral.getValue()).intValue();

    if (offset != null) {
      RexLiteral offsetLiteral = (RexLiteral) offset;
      startIndex = ((BigDecimal) offsetLiteral.getValue()).intValue();
    }
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {
    // Sorting does not change rate or row count of the input.
    return BeamSqlRelUtils.getNodeStats(this.input, mq);
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    NodeStats inputEstimates = BeamSqlRelUtils.getNodeStats(this.input, mq);

    final double rowSize = getRowType().getFieldCount();
    final double cpu = inputEstimates.getRowCount() * inputEstimates.getRowCount() * rowSize;
    final double cpuRate = inputEstimates.getRate() * inputEstimates.getWindow() * rowSize;
    return BeamCostModel.FACTORY.makeCost(cpu, cpuRate);
  }

  public boolean isLimitOnly() {
    return fieldIndices.isEmpty();
  }

  public int getCount() {
    return count;
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new Transform();
  }

  private class Transform extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      checkArgument(
          pinput.size() == 1,
          "Wrong number of inputs for %s: %s",
          BeamIOSinkRel.class.getSimpleName(),
          pinput);
      PCollection<Row> upstream = pinput.get(0);

      // There is a need to separate ORDER BY LIMIT and LIMIT:
      //  - GroupByKey (used in Top) is not allowed on unbounded data in global window so ORDER BY
      // ... LIMIT
      //    works only on bounded data.
      //  - Just LIMIT operates on unbounded data, but across windows.
      if (fieldIndices.isEmpty()) {
        // TODO(https://issues.apache.org/jira/projects/BEAM/issues/BEAM-4702)
        // Figure out which operations are per-window and which are not.

        return upstream
            .apply(Window.into(new GlobalWindows()))
            .apply(new LimitTransform<>(startIndex))
            .setRowSchema(CalciteUtils.toSchema(getRowType()));
      } else {

        WindowingStrategy<?, ?> windowingStrategy = upstream.getWindowingStrategy();
        if (!(windowingStrategy.getWindowFn() instanceof GlobalWindows)) {
          throw new UnsupportedOperationException(
              String.format(
                  "`ORDER BY` is only supported for %s, actual windowing strategy: %s",
                  GlobalWindows.class.getSimpleName(), windowingStrategy));
        }

        ReversedBeamSqlRowComparator comparator =
            new ReversedBeamSqlRowComparator(fieldIndices, orientation, nullsFirst);

        // first find the top (offset + count)
        PCollection<List<Row>> rawStream =
            upstream
                .apply(
                    "extractTopOffsetAndFetch",
                    Top.of(startIndex + count, comparator).withoutDefaults())
                .setCoder(ListCoder.of(upstream.getCoder()));

        // strip the `leading offset`
        if (startIndex > 0) {
          rawStream =
              rawStream
                  .apply(
                      "stripLeadingOffset",
                      ParDo.of(new SubListFn<>(startIndex, startIndex + count)))
                  .setCoder(ListCoder.of(upstream.getCoder()));
        }

        return rawStream
            .apply("flatten", Flatten.iterables())
            .setRowSchema(CalciteUtils.toSchema(getRowType()));
      }
    }
  }

  private class LimitTransform<T> extends PTransform<PCollection<T>, PCollection<T>> {
    private final int startIndex;

    public LimitTransform(int startIndex) {
      this.startIndex = startIndex;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      Coder<T> coder = input.getCoder();
      PCollection<KV<String, T>> keyedRow =
          input.apply(WithKeys.of("DummyKey")).setCoder(KvCoder.of(StringUtf8Coder.of(), coder));

      return keyedRow.apply(ParDo.of(new LimitFn<T>(getCount(), startIndex)));
    }
  }

  private static class LimitFn<T> extends DoFn<KV<String, T>, T> {
    private final Integer limitCount;
    private final Integer startIndex;

    public LimitFn(int c, int s) {
      limitCount = c;
      startIndex = s;
    }

    @StateId("counter")
    private final StateSpec<ValueState<Integer>> counterState = StateSpecs.value(VarIntCoder.of());

    @StateId("skipped_rows")
    private final StateSpec<ValueState<Integer>> skippedRowsState =
        StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(
        ProcessContext context,
        @StateId("counter") ValueState<Integer> counterState,
        @StateId("skipped_rows") ValueState<Integer> skippedRowsState) {
      Integer toSkipRows = firstNonNull(skippedRowsState.read(), startIndex);
      if (toSkipRows == 0) {
        int current = firstNonNull(counterState.read(), 0);
        if (current < limitCount) {
          counterState.write(current + 1);
          context.output(context.element().getValue());
        }
      } else {
        skippedRowsState.write(toSkipRows - 1);
      }
    }
  }

  private static class SubListFn<T> extends DoFn<List<T>, List<T>> {
    private int startIndex;
    private int endIndex;

    public SubListFn(int startIndex, int endIndex) {
      this.startIndex = startIndex;
      this.endIndex = endIndex;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(ctx.element().subList(startIndex, endIndex));
    }
  }

  @Override
  public Sort copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      RexNode offset,
      RexNode fetch) {
    return new BeamSortRel(getCluster(), traitSet, newInput, newCollation, offset, fetch);
  }

  public static class BeamSqlRowComparator implements Comparator<Row>, Serializable {
    private List<Integer> fieldsIndices;
    private List<Boolean> orientation;
    private List<Boolean> nullsFirst;

    public BeamSqlRowComparator(
        List<Integer> fieldsIndices, List<Boolean> orientation, List<Boolean> nullsFirst) {
      this.fieldsIndices = fieldsIndices;
      this.orientation = orientation;
      this.nullsFirst = nullsFirst;
    }

    @Override
    public int compare(Row row1, Row row2) {
      for (int i = 0; i < fieldsIndices.size(); i++) {
        int fieldIndex = fieldsIndices.get(i);
        int fieldRet = 0;

        FieldType fieldType = row1.getSchema().getField(fieldIndex).getType();
        SqlTypeName sqlTypeName = CalciteUtils.toSqlTypeName(fieldType);
        // whether NULL should be ordered first or last(compared to non-null values) depends on
        // what user specified in SQL(NULLS FIRST/NULLS LAST)
        boolean isValue1Null = (row1.getValue(fieldIndex) == null);
        boolean isValue2Null = (row2.getValue(fieldIndex) == null);
        if (isValue1Null && isValue2Null) {
          continue;
        } else if (isValue1Null && !isValue2Null) {
          fieldRet = -1 * (nullsFirst.get(i) ? -1 : 1);
        } else if (!isValue1Null && isValue2Null) {
          fieldRet = 1 * (nullsFirst.get(i) ? -1 : 1);
        } else {
          switch (sqlTypeName) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case VARCHAR:
            case DATE:
            case TIMESTAMP:
              Comparable v1 = row1.getBaseValue(fieldIndex, Comparable.class);
              Comparable v2 = row2.getBaseValue(fieldIndex, Comparable.class);
              fieldRet = v1.compareTo(v2);
              break;
            default:
              throw new UnsupportedOperationException(
                  "Data type: " + sqlTypeName + " not supported yet!");
          }
        }

        fieldRet *= (orientation.get(i) ? 1 : -1);

        if (fieldRet != 0) {
          return fieldRet;
        }
      }
      return 0;
    }
  }

  private static class ReversedBeamSqlRowComparator implements Comparator<Row>, Serializable {
    private final BeamSqlRowComparator comparator;

    public ReversedBeamSqlRowComparator(
        List<Integer> fieldsIndices, List<Boolean> orientation, List<Boolean> nullsFirst) {
      comparator = new BeamSqlRowComparator(fieldsIndices, orientation, nullsFirst);
    }

    @Override
    public int compare(Row row1, Row row2) {
      return comparator.compare(row2, row1);
    }
  }
}
