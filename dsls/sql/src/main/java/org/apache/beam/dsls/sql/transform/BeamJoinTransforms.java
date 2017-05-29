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

package org.apache.beam.dsls.sql.transform;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.Pair;

/**
 * Collections of {@code PTransform} and {@code DoFn} used to perform JOIN operation.
 */
public class BeamJoinTransforms {

  /**
   * A {@code SimpleFunction} to extract join fields from the specified row.
   */
  public static class ExtractJoinFields
      extends SimpleFunction<BeamSqlRow, KV<BeamSqlRow, BeamSqlRow>> {
    private boolean isLeft;
    private List<Pair<Integer, Integer>> joinColumns;

    public ExtractJoinFields(boolean isLeft, List<Pair<Integer, Integer>> joinColumns) {
      this.isLeft = isLeft;
      this.joinColumns = joinColumns;
    }

    @Override public KV<BeamSqlRow, BeamSqlRow> apply(BeamSqlRow input) {
      // build the type
      // the name of the join field is not important
      List<String> names = new ArrayList<>(joinColumns.size());
      List<Integer> types = new ArrayList<>(joinColumns.size());
      for (int i = 0; i < joinColumns.size(); i++) {
        names.add("c" + i);
        types.add(isLeft
            ? input.getDataType().getFieldsType().get(joinColumns.get(i).getKey()) :
            input.getDataType().getFieldsType().get(joinColumns.get(i).getValue()));
      }
      BeamSqlRecordType type = BeamSqlRecordType.create(names, types);

      // build the row
      BeamSqlRow row = new BeamSqlRow(type);
      for (int i = 0; i < joinColumns.size(); i++) {
        row.addField(i, input
            .getFieldValue(isLeft ? joinColumns.get(i).getKey() : joinColumns.get(i).getValue()));
      }
      return KV.of(row, input);
    }
  }


  /**
   * A {@code DoFn} which implement the sideInput-JOIN.
   */
  public static class SideInputJoinDoFn extends DoFn<KV<BeamSqlRow, BeamSqlRow>, BeamSqlRow> {
    private PCollectionView<Map<BeamSqlRow, Iterable<BeamSqlRow>>> sideInputView;
    private JoinRelType joinType;
    private BeamSqlRow rightNullRow;
    private boolean swap;

    public SideInputJoinDoFn(JoinRelType joinType, BeamSqlRow rightNullRow,
        PCollectionView<Map<BeamSqlRow, Iterable<BeamSqlRow>>> sideInputView,
        boolean swap) {
      this.joinType = joinType;
      this.rightNullRow = rightNullRow;
      this.sideInputView = sideInputView;
      this.swap = swap;
    }

    @ProcessElement public void processElement(ProcessContext context) {
      BeamSqlRow key = context.element().getKey();
      BeamSqlRow leftRow = context.element().getValue();
      Map<BeamSqlRow, Iterable<BeamSqlRow>> key2Rows = context.sideInput(sideInputView);
      Iterable<BeamSqlRow> rightRowsIterable = key2Rows.get(key);

      if (rightRowsIterable != null && rightRowsIterable.iterator().hasNext()) {
        Iterator<BeamSqlRow> it = rightRowsIterable.iterator();
        while (it.hasNext()) {
          context.output(combineTwoRowsIntoOne(leftRow, it.next(), swap));
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
      extends SimpleFunction<KV<BeamSqlRow, KV<BeamSqlRow, BeamSqlRow>>, BeamSqlRow> {
    @Override public BeamSqlRow apply(KV<BeamSqlRow, KV<BeamSqlRow, BeamSqlRow>> input) {
      KV<BeamSqlRow, BeamSqlRow> parts = input.getValue();
      BeamSqlRow leftRow = parts.getKey();
      BeamSqlRow rightRow = parts.getValue();
      return combineTwoRowsIntoOne(leftRow, rightRow);
    }
  }

  private static BeamSqlRow combineTwoRowsIntoOne(BeamSqlRow leftRow, BeamSqlRow rightRow) {
    return combineTwoRowsIntoOne(leftRow, rightRow, false);
  }

  /**
   * As the method name suggests: combine two rows into one wide row.
   */
  private static BeamSqlRow combineTwoRowsIntoOne(BeamSqlRow leftRow,
      BeamSqlRow rightRow, boolean swap) {
    // build the type
    List<String> names = new ArrayList<>(leftRow.size() + rightRow.size());
    names.addAll(
        swap ? rightRow.getDataType().getFieldsName() : leftRow.getDataType().getFieldsName());
    names.addAll(
        swap ? leftRow.getDataType().getFieldsName() : rightRow.getDataType().getFieldsName());

    List<Integer> types = new ArrayList<>(leftRow.size() + rightRow.size());
    types.addAll(
        swap ? rightRow.getDataType().getFieldsType() : leftRow.getDataType().getFieldsType());
    types.addAll(
        swap ? leftRow.getDataType().getFieldsType() : rightRow.getDataType().getFieldsType());

    BeamSqlRecordType type = BeamSqlRecordType.create(names, types);

    BeamSqlRow row = new BeamSqlRow(type);
    BeamSqlRow currentRow = swap ? rightRow : leftRow;
    int leftRowSize = currentRow.size();
    // build the row
    for (int i = 0; i < currentRow.size(); i++) {
      row.addField(i, currentRow.getFieldValue(i));
    }

    currentRow = swap ? leftRow : rightRow;
    for (int i = 0; i < currentRow.size(); i++) {
      row.addField(i + leftRowSize, currentRow.getFieldValue(i));
    }

    return row;
  }
}
