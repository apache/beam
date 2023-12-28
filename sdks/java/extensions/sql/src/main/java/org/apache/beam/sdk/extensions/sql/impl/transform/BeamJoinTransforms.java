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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.BeamSqlSeekableTable;
import org.apache.beam.sdk.extensions.sql.impl.utils.SerializableRexFieldAccess;
import org.apache.beam.sdk.extensions.sql.impl.utils.SerializableRexInputRef;
import org.apache.beam.sdk.extensions.sql.impl.utils.SerializableRexNode;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.util.Pair;

/** Collections of {@code PTransform} and {@code DoFn} used to perform JOIN operation. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BeamJoinTransforms {

  public static FieldAccessDescriptor getJoinColumns(
      boolean isLeft,
      List<Pair<RexNode, RexNode>> joinColumns,
      int leftRowColumnCount,
      Schema schema) {
    List<SerializableRexNode> joinColumnsBuilt =
        joinColumns.stream()
            .map(pair -> SerializableRexNode.builder(isLeft ? pair.left : pair.right).build())
            .collect(toList());
    return FieldAccessDescriptor.union(
        joinColumnsBuilt.stream()
            .map(v -> getJoinColumn(v, leftRowColumnCount).resolve(schema))
            .collect(Collectors.toList()));
  }

  private static FieldAccessDescriptor getJoinColumn(
      SerializableRexNode serializableRexNode, int leftRowColumnCount) {
    if (serializableRexNode instanceof SerializableRexInputRef) {
      SerializableRexInputRef inputRef = (SerializableRexInputRef) serializableRexNode;
      return FieldAccessDescriptor.withFieldIds(inputRef.getIndex() - leftRowColumnCount);
    } else { // It can only be SerializableFieldAccess.
      List<Integer> indexes = ((SerializableRexFieldAccess) serializableRexNode).getIndexes();
      FieldAccessDescriptor fieldAccessDescriptor =
          FieldAccessDescriptor.withFieldIds(indexes.get(0) - leftRowColumnCount);
      for (int i = 1; i < indexes.size(); i++) {
        fieldAccessDescriptor =
            FieldAccessDescriptor.withFieldIds(fieldAccessDescriptor, indexes.get(i));
      }
      return fieldAccessDescriptor;
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
        .addValues(leftRow.getBaseValues())
        .addValues(rightRow.getBaseValues())
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
                      seekableTable.setUp(joinSubsetType);
                    }

                    @StartBundle
                    public void startBundle(
                        DoFn<Row, Row>.StartBundleContext context,
                        PipelineOptions pipelineOptions) {
                      seekableTable.startBundle(context, pipelineOptions);
                    }

                    @FinishBundle
                    public void finishBundle(
                        DoFn<Row, Row>.FinishBundleContext context,
                        PipelineOptions pipelineOptions) {
                      seekableTable.finishBundle(context, pipelineOptions);
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
                          factJoinIdx.stream()
                              .map(i -> factRow.getBaseValue(i, Object.class))
                              .collect(toList());

                      return Row.withSchema(joinSubsetType).addValues(joinSubsetValues).build();
                    }
                  }))
          .setRowSchema(joinSubsetType);
    }
  }
}
