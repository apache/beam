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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.BeamSqlSeekableTable;
import org.apache.beam.sdk.extensions.sql.impl.utils.SerializableRexFieldAccess;
import org.apache.beam.sdk.extensions.sql.impl.utils.SerializableRexInputRef;
import org.apache.beam.sdk.extensions.sql.impl.utils.SerializableRexNode;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.Pair;

/** Collections of {@code PTransform} and {@code DoFn} used to perform JOIN operation. */
public class BeamJoinTransforms {

  /** A {@code SimpleFunction} to extract join fields from the specified row. */
  public static class ExtractJoinFields extends SimpleFunction<Row, KV<Row, Row>> {
    private final List<SerializableRexNode> joinColumns;
    private final Schema schema;
    private int leftRowColumnCount;

    public ExtractJoinFields(
        boolean isLeft,
        List<Pair<RexNode, RexNode>> joinColumns,
        Schema schema,
        int leftRowColumnCount) {
      this.joinColumns =
          joinColumns.stream()
              .map(pair -> SerializableRexNode.builder(isLeft ? pair.left : pair.right).build())
              .collect(toList());
      this.schema = schema;
      this.leftRowColumnCount = leftRowColumnCount;
    }

    @Override
    public KV<Row, Row> apply(Row input) {
      Row row =
          joinColumns.stream()
              .map(v -> getValue(v, input, leftRowColumnCount))
              .collect(toRow(schema));
      return KV.of(row, input);
    }

    @SuppressWarnings("unused")
    private Schema.Field toField(Schema schema, Integer fieldIndex) {
      Schema.Field original = schema.getField(fieldIndex);
      return original.withName("c" + fieldIndex);
    }

    private Object getValue(
        SerializableRexNode serializableRexNode, Row input, int leftRowColumnCount) {
      if (serializableRexNode instanceof SerializableRexInputRef) {
        return input.getValue(
            ((SerializableRexInputRef) serializableRexNode).getIndex() - leftRowColumnCount);
      } else { // It can only be SerializableFieldAccess.
        List<Integer> indexes = ((SerializableRexFieldAccess) serializableRexNode).getIndexes();
        // retrieve row based on the first column reference.
        Row rowField = input.getValue(indexes.get(0) - leftRowColumnCount);
        for (int i = 1; i < indexes.size() - 1; i++) {
          rowField = rowField.getRow(indexes.get(i));
        }
        return rowField.getValue(indexes.get(indexes.size() - 1));
      }
    }
  }

  /** A {@code DoFn} which implement the sideInput-JOIN. */
  public static class SideInputJoinDoFn extends DoFn<KV<Row, Row>, Row> {
    private final PCollectionView<Map<Row, Iterable<Row>>> sideInputView;
    private final JoinRelType joinType;
    private final Row rightNullRow;
    private final boolean swap;
    private final Schema schema;

    public SideInputJoinDoFn(
        JoinRelType joinType,
        Row rightNullRow,
        PCollectionView<Map<Row, Iterable<Row>>> sideInputView,
        boolean swap,
        Schema schema) {
      this.joinType = joinType;
      this.rightNullRow = rightNullRow;
      this.sideInputView = sideInputView;
      this.swap = swap;
      this.schema = schema;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      Row key = context.element().getKey();
      Row leftRow = context.element().getValue();
      Map<Row, Iterable<Row>> key2Rows = context.sideInput(sideInputView);
      Iterable<Row> rightRowsIterable = key2Rows.get(key);

      if (rightRowsIterable != null && rightRowsIterable.iterator().hasNext()) {
        for (Row aRightRowsIterable : rightRowsIterable) {
          context.output(combineTwoRowsIntoOne(leftRow, aRightRowsIterable, swap, schema));
        }
      } else {
        if (joinType == JoinRelType.LEFT) {
          context.output(combineTwoRowsIntoOne(leftRow, rightNullRow, swap, schema));
        }
      }
    }
  }

  /** A {@code SimpleFunction} to combine two rows into one. */
  public static class JoinParts2WholeRow extends SimpleFunction<KV<Row, KV<Row, Row>>, Row> {
    private final Schema schema;

    public JoinParts2WholeRow(Schema schema) {
      this.schema = schema;
    }

    @Override
    public Row apply(KV<Row, KV<Row, Row>> input) {
      KV<Row, Row> parts = input.getValue();
      Row leftRow = parts.getKey();
      Row rightRow = parts.getValue();
      return combineTwoRowsIntoOne(leftRow, rightRow, false, schema);
    }
  }

  /** As the method name suggests: combine two rows into one wide row. */
  private static Row combineTwoRowsIntoOne(
      Row leftRow, Row rightRow, boolean swap, Schema outputSchema) {
    if (swap) {
      return combineTwoRowsIntoOneHelper(rightRow, leftRow, outputSchema);
    } else {
      return combineTwoRowsIntoOneHelper(leftRow, rightRow, outputSchema);
    }
  }

  /** As the method name suggests: combine two rows into one wide row. */
  private static Row combineTwoRowsIntoOneHelper(Row leftRow, Row rightRow, Schema ouputSchema) {
    return Row.withSchema(ouputSchema)
        .addValues(leftRow.getValues())
        .addValues(rightRow.getValues())
        .build();
  }

  /** Transform to execute Join as Lookup. */
  public static class JoinAsLookup extends PTransform<PCollection<Row>, PCollection<Row>> {
    private final BeamSqlSeekableTable seekableTable;
    private final Schema lkpSchema;
    private final int factColOffset;
    private Schema joinSubsetType;
    private final Schema outputSchema;
    private List<Integer> factJoinIdx;

    public JoinAsLookup(
        RexNode joinCondition,
        BeamSqlSeekableTable seekableTable,
        Schema lkpSchema,
        Schema outputSchema,
        int factColOffset,
        int lkpColOffset) {
      this.seekableTable = seekableTable;
      this.lkpSchema = lkpSchema;
      this.outputSchema = outputSchema;
      this.factColOffset = factColOffset;
      joinFieldsMapping(joinCondition, factColOffset, lkpColOffset);
    }

    private void joinFieldsMapping(RexNode joinCondition, int factColOffset, int lkpColOffset) {
      factJoinIdx = new ArrayList<>();
      List<Schema.Field> lkpJoinFields = new ArrayList<>();

      RexCall call = (RexCall) joinCondition;
      if ("AND".equals(call.getOperator().getName())) {
        List<RexNode> operands = call.getOperands();
        for (RexNode rexNode : operands) {
          factJoinIdx.add(
              ((RexInputRef) ((RexCall) rexNode).getOperands().get(0)).getIndex() - factColOffset);
          int lkpJoinIdx =
              ((RexInputRef) ((RexCall) rexNode).getOperands().get(1)).getIndex() - lkpColOffset;
          lkpJoinFields.add(lkpSchema.getField(lkpJoinIdx));
        }
      } else if ("=".equals(call.getOperator().getName())) {
        factJoinIdx.add(((RexInputRef) call.getOperands().get(0)).getIndex() - factColOffset);
        int lkpJoinIdx = ((RexInputRef) call.getOperands().get(1)).getIndex() - lkpColOffset;
        lkpJoinFields.add(lkpSchema.getField(lkpJoinIdx));
      } else {
        throw new UnsupportedOperationException(
            "Operator " + call.getOperator().getName() + " is not supported in join condition");
      }

      joinSubsetType = Schema.builder().addFields(lkpJoinFields).build();
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
      return input
          .apply(
              "join_as_lookup",
              ParDo.of(
                  new DoFn<Row, Row>() {
                    @Setup
                    public void setup() {
                      seekableTable.setUp();
                    }

                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      Row factRow = context.element();
                      Row joinSubRow = extractJoinSubRow(factRow);
                      List<Row> lookupRows = seekableTable.seekRow(joinSubRow);
                      for (Row lr : lookupRows) {
                        context.output(
                            combineTwoRowsIntoOne(factRow, lr, factColOffset != 0, outputSchema));
                      }
                    }

                    @Teardown
                    public void teardown() {
                      seekableTable.tearDown();
                    }

                    private Row extractJoinSubRow(Row factRow) {
                      List<Object> joinSubsetValues =
                          factJoinIdx.stream().map(factRow::getValue).collect(toList());

                      return Row.withSchema(joinSubsetType).addValues(joinSubsetValues).build();
                    }
                  }))
          .setRowSchema(joinSubsetType);
    }
  }
}
