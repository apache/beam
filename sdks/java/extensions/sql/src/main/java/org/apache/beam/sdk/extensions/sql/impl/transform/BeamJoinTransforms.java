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
import static org.apache.beam.sdk.values.BeamRecord.toRecord;
import static org.apache.beam.sdk.values.BeamRecordType.toRecordType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.sql.BeamSqlSeekableTable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.BeamRecordType;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
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
      extends SimpleFunction<BeamRecord, KV<BeamRecord, BeamRecord>> {
    private final List<Integer> joinColumns;

    public ExtractJoinFields(boolean isLeft, List<Pair<Integer, Integer>> joinColumns) {
      this.joinColumns =
          joinColumns
              .stream()
              .map(pair -> isLeft ? pair.left : pair.right)
              .collect(toList());
    }

    @Override
    public KV<BeamRecord, BeamRecord> apply(BeamRecord input) {
      BeamRecordType recordType =
          joinColumns
              .stream()
              .map(fieldIndex -> toField(input.getRecordType(), fieldIndex))
              .collect(toRecordType());

      BeamRecord beamRecord =
          joinColumns
              .stream()
              .map(input::getValue)
              .collect(toRecord(recordType));

      return KV.of(beamRecord, input);
    }

    private BeamRecordType.Field toField(BeamRecordType recordType, Integer fieldIndex) {
      return BeamRecordType.newField(
          "c" + fieldIndex,
          //recordType.getFieldName(fieldIndex),
          recordType.getFieldCoder(fieldIndex));
    }
  }

  /**
   * A {@code DoFn} which implement the sideInput-JOIN.
   */
  public static class SideInputJoinDoFn extends DoFn<KV<BeamRecord, BeamRecord>, BeamRecord> {
    private final PCollectionView<Map<BeamRecord, Iterable<BeamRecord>>> sideInputView;
    private final JoinRelType joinType;
    private final BeamRecord rightNullRow;
    private final boolean swap;

    public SideInputJoinDoFn(JoinRelType joinType, BeamRecord rightNullRow,
        PCollectionView<Map<BeamRecord, Iterable<BeamRecord>>> sideInputView,
        boolean swap) {
      this.joinType = joinType;
      this.rightNullRow = rightNullRow;
      this.sideInputView = sideInputView;
      this.swap = swap;
    }

    @ProcessElement public void processElement(ProcessContext context) {
      BeamRecord key = context.element().getKey();
      BeamRecord leftRow = context.element().getValue();
      Map<BeamRecord, Iterable<BeamRecord>> key2Rows = context.sideInput(sideInputView);
      Iterable<BeamRecord> rightRowsIterable = key2Rows.get(key);

      if (rightRowsIterable != null && rightRowsIterable.iterator().hasNext()) {
        for (BeamRecord aRightRowsIterable : rightRowsIterable) {
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
      extends SimpleFunction<KV<BeamRecord, KV<BeamRecord, BeamRecord>>, BeamRecord> {
    @Override public BeamRecord apply(KV<BeamRecord, KV<BeamRecord, BeamRecord>> input) {
      KV<BeamRecord, BeamRecord> parts = input.getValue();
      BeamRecord leftRow = parts.getKey();
      BeamRecord rightRow = parts.getValue();
      return combineTwoRowsIntoOne(leftRow, rightRow, false);
    }
  }

  /**
   * As the method name suggests: combine two rows into one wide row.
   */
  private static BeamRecord combineTwoRowsIntoOne(BeamRecord leftRow,
      BeamRecord rightRow, boolean swap) {
    if (swap) {
      return combineTwoRowsIntoOneHelper(rightRow, leftRow);
    } else {
      return combineTwoRowsIntoOneHelper(leftRow, rightRow);
    }
  }

  /**
   * As the method name suggests: combine two rows into one wide row.
   */
  private static BeamRecord combineTwoRowsIntoOneHelper(BeamRecord leftRow, BeamRecord rightRow) {
    // build the type
    List<String> names = new ArrayList<>(leftRow.getFieldCount() + rightRow.getFieldCount());
    names.addAll(leftRow.getRecordType().getFieldNames());
    names.addAll(rightRow.getRecordType().getFieldNames());

    List<Coder> types = new ArrayList<>(leftRow.getFieldCount() + rightRow.getFieldCount());
    types.addAll(leftRow.getRecordType().getRecordCoder().getCoders());
    types.addAll(rightRow.getRecordType().getRecordCoder().getCoders());
    BeamRecordType type = BeamRecordType.fromNamesAndCoders(names, types);

    return BeamRecord
            .withRecordType(type)
            .addValues(leftRow.getValues())
            .addValues(rightRow.getValues())
            .build();
  }

  /**
   * Transform to execute Join as Lookup.
   */
  public static class JoinAsLookup
      extends PTransform<PCollection<BeamRecord>, PCollection<BeamRecord>> {

    BeamSqlSeekableTable seekableTable;
    BeamRecordType lkpRowType;
    BeamRecordType joinSubsetType;
    List<Integer> factJoinIdx;

    public JoinAsLookup(RexNode joinCondition, BeamSqlSeekableTable seekableTable,
        BeamRecordType lkpRowType, int factTableColSize) {
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

      joinSubsetType = BeamRecordType.fromNamesAndCoders(lkpJoinFieldsName, lkpJoinFieldsType);
    }

    @Override
    public PCollection<BeamRecord> expand(PCollection<BeamRecord> input) {
      return input.apply("join_as_lookup", ParDo.of(new DoFn<BeamRecord, BeamRecord>(){
        @ProcessElement
        public void processElement(ProcessContext context) {
          BeamRecord factRow = context.element();
          BeamRecord joinSubRow = extractJoinSubRow(factRow);
          List<BeamRecord> lookupRows = seekableTable.seekRecord(joinSubRow);
          for (BeamRecord lr : lookupRows) {
            context.output(combineTwoRowsIntoOneHelper(factRow, lr));
          }
        }

        private BeamRecord extractJoinSubRow(BeamRecord factRow) {
          List<Object> joinSubsetValues =
              factJoinIdx
                  .stream()
                  .map(factRow::getValue)
                  .collect(toList());

          return
              BeamRecord
                  .withRecordType(joinSubsetType)
                  .addValues(joinSubsetValues)
                  .build();
        }

      }));
    }
  }

}
