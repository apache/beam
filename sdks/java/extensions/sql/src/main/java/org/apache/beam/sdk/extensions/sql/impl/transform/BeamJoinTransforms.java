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

package org.apache.beam.sdk.extensions.sql.impl.transform;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.values.Row.toRow;
import static org.apache.beam.sdk.values.RowType.toRowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.sql.BeamSqlSeekableTable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

/**
 * Collections of {@code PTransform} and {@code DoFn} used to perform JOIN operation.
 */
public class BeamJoinTransforms {

  /**
   * A {@code SimpleFunction} to extract join fields from the specified row.
   */
  public static class ExtractJoinFields
      extends SimpleFunction<Row, KV<Row, Row>> {
    private final List<Integer> joinColumns;

    public ExtractJoinFields(boolean isLeft, List<Pair<Integer, Integer>> joinColumns) {
      this.joinColumns =
          joinColumns
              .stream()
              .map(pair -> isLeft ? pair.left : pair.right)
              .collect(toList());
    }

    @Override
    public KV<Row, Row> apply(Row input) {
      RowType rowType =
          joinColumns
              .stream()
              .map(fieldIndex -> toField(input.getRowType(), fieldIndex))
              .collect(toRowType());

      Row row =
          joinColumns
              .stream()
              .map(input::getValue)
              .collect(toRow(rowType));

      return KV.of(row, input);
    }

    private RowType.Field toField(RowType rowType, Integer fieldIndex) {
      return RowType.newField(
          "c" + fieldIndex,
          //rowType.getFieldName(fieldIndex),
          rowType.getFieldCoder(fieldIndex));
    }
  }

  /**
   * A {@code DoFn} which implement the sideInput-JOIN.
   */
  public static class SideInputJoinDoFn extends DoFn<KV<Row, Row>, Row> {
    private final PCollectionView<Map<Row, Iterable<Row>>> sideInputView;
    private final JoinRelType joinType;
    private final Row rightNullRow;
    private final boolean swap;

    public SideInputJoinDoFn(JoinRelType joinType, Row rightNullRow,
        PCollectionView<Map<Row, Iterable<Row>>> sideInputView,
        boolean swap) {
      this.joinType = joinType;
      this.rightNullRow = rightNullRow;
      this.sideInputView = sideInputView;
      this.swap = swap;
    }

    @ProcessElement public void processElement(ProcessContext context) {
      Row key = context.element().getKey();
      Row leftRow = context.element().getValue();
      Map<Row, Iterable<Row>> key2Rows = context.sideInput(sideInputView);
      Iterable<Row> rightRowsIterable = key2Rows.get(key);

      if (rightRowsIterable != null && rightRowsIterable.iterator().hasNext()) {
        for (Row aRightRowsIterable : rightRowsIterable) {
          context.output(combineTwoRowsIntoOne(leftRow, aRightRowsIterable, swap));
        }
      } else {
        if (joinType == JoinRelType.LEFT) {
          context.output(combineTwoRowsIntoOne(leftRow, rightNullRow, swap));
        }
      }
    }
  }


  /**
   * A {@code SimpleFunction} to combine two rows into one.
   */
  public static class JoinParts2WholeRow
      extends SimpleFunction<KV<Row, KV<Row, Row>>, Row> {
    @Override public Row apply(KV<Row, KV<Row, Row>> input) {
      KV<Row, Row> parts = input.getValue();
      Row leftRow = parts.getKey();
      Row rightRow = parts.getValue();
      return combineTwoRowsIntoOne(leftRow, rightRow, false);
    }
  }

  /**
   * As the method name suggests: combine two rows into one wide row.
   */
  private static Row combineTwoRowsIntoOne(Row leftRow,
                                           Row rightRow, boolean swap) {
    if (swap) {
      return combineTwoRowsIntoOneHelper(rightRow, leftRow);
    } else {
      return combineTwoRowsIntoOneHelper(leftRow, rightRow);
    }
  }

  /**
   * As the method name suggests: combine two rows into one wide row.
   */
  private static Row combineTwoRowsIntoOneHelper(Row leftRow, Row rightRow) {
    // build the type
    List<String> names = new ArrayList<>(leftRow.getFieldCount() + rightRow.getFieldCount());
    names.addAll(leftRow.getRowType().getFieldNames());
    names.addAll(rightRow.getRowType().getFieldNames());

    List<Coder> types = new ArrayList<>(leftRow.getFieldCount() + rightRow.getFieldCount());
    types.addAll(leftRow.getRowType().getRowCoder().getCoders());
    types.addAll(rightRow.getRowType().getRowCoder().getCoders());
    RowType type = RowType.fromNamesAndCoders(names, types);

    return Row
            .withRowType(type)
            .addValues(leftRow.getValues())
            .addValues(rightRow.getValues())
            .build();
  }

  /**
   * Transform to execute Join as Lookup.
   */
  public static class JoinAsLookup
      extends PTransform<PCollection<Row>, PCollection<Row>> {

    BeamSqlSeekableTable seekableTable;
    RowType lkpRowType;
    RowType joinSubsetType;
    List<Integer> factJoinIdx;

    public JoinAsLookup(RexNode joinCondition, BeamSqlSeekableTable seekableTable,
                        RowType lkpRowType, int factTableColSize) {
      this.seekableTable = seekableTable;
      this.lkpRowType = lkpRowType;
      joinFieldsMapping(joinCondition, factTableColSize);
    }

    private void joinFieldsMapping(RexNode joinCondition, int factTableColSize) {
      factJoinIdx = new ArrayList<>();
      List<String> lkpJoinFieldsName = new ArrayList<>();
      List<Coder> lkpJoinFieldsType = new ArrayList<>();

      RexCall call = (RexCall) joinCondition;
      if ("AND".equals(call.getOperator().getName())) {
        List<RexNode> operands = call.getOperands();
        for (RexNode rexNode : operands) {
          factJoinIdx.add(((RexInputRef) ((RexCall) rexNode).getOperands().get(0)).getIndex());
          int lkpJoinIdx = ((RexInputRef) ((RexCall) rexNode).getOperands().get(1)).getIndex()
              - factTableColSize;
          lkpJoinFieldsName.add(lkpRowType.getFieldName(lkpJoinIdx));
          lkpJoinFieldsType.add(lkpRowType.getFieldCoder(lkpJoinIdx));
        }
      } else if ("=".equals(call.getOperator().getName())) {
        factJoinIdx.add(((RexInputRef) call.getOperands().get(0)).getIndex());
        int lkpJoinIdx = ((RexInputRef) call.getOperands().get(1)).getIndex()
            - factTableColSize;
        lkpJoinFieldsName.add(lkpRowType.getFieldName(lkpJoinIdx));
        lkpJoinFieldsType.add(lkpRowType.getFieldCoder(lkpJoinIdx));
      } else {
        throw new UnsupportedOperationException(
            "Operator " + call.getOperator().getName() + " is not supported in join condition");
      }

      joinSubsetType = RowType.fromNamesAndCoders(lkpJoinFieldsName, lkpJoinFieldsType);
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
      return input.apply("join_as_lookup", ParDo.of(new DoFn<Row, Row>() {
        @ProcessElement
        public void processElement(ProcessContext context) {
          Row factRow = context.element();
          Row joinSubRow = extractJoinSubRow(factRow);
          List<Row> lookupRows = seekableTable.seekRow(joinSubRow);
          for (Row lr : lookupRows) {
            context.output(combineTwoRowsIntoOneHelper(factRow, lr));
          }
        }

        private Row extractJoinSubRow(Row factRow) {
          List<Object> joinSubsetValues =
              factJoinIdx
                  .stream()
                  .map(factRow::getValue)
                  .collect(toList());

          return
              Row
                  .withRowType(joinSubsetType)
                  .addValues(joinSubsetValues)
                  .build();
        }

      }));
    }
  }

}
