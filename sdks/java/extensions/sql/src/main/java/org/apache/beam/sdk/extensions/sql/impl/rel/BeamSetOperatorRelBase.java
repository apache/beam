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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamSetOperatorsTransforms;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.transforms.CoGroup;
import org.apache.beam.sdk.schemas.transforms.CoGroup.By;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Delegate for Set operators: {@code BeamUnionRel}, {@code BeamIntersectRel} and {@code
 * BeamMinusRel}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BeamSetOperatorRelBase extends PTransform<PCollectionList<Row>, PCollection<Row>> {
  /** Set operator type. */
  public enum OpType implements Serializable {
    UNION,
    INTERSECT,
    MINUS
  }

  private BeamRelNode beamRelNode;
  private boolean all;
  private OpType opType;

  public BeamSetOperatorRelBase(BeamRelNode beamRelNode, OpType opType, boolean all) {
    this.beamRelNode = beamRelNode;
    this.opType = opType;
    this.all = all;
  }

  @Override
  public PCollection<Row> expand(PCollectionList<Row> inputs) {
    Schema targetSchema = CalciteUtils.toSchema(beamRelNode.getRowType());

    if (opType == OpType.UNION && all) {
      PCollectionList<Row> coercedInputs = PCollectionList.empty(inputs.getPipeline());
      for (PCollection<Row> input : inputs.getAll()) {
        coercedInputs = coercedInputs.and(coerce(input, targetSchema));
      }
      return coercedInputs.apply("UnionAllFlatten", Flatten.pCollections());
    }

    if (inputs.size() == 2) {
      return expandPair(coerce(inputs.get(0), targetSchema), coerce(inputs.get(1), targetSchema));
    } else if (inputs.size() > 2) {
      PCollection<Row> result = coerce(inputs.get(0), targetSchema);
      for (int i = 1; i < inputs.size(); i++) {
        result = expandPair(result, coerce(inputs.get(i), targetSchema));
      }
      return result;
    } else {
      throw new IllegalArgumentException(
          "Too few arguments to " + beamRelNode.getClass().getSimpleName());
    }
  }

  private PCollection<Row> expandPair(PCollection<Row> leftRows, PCollection<Row> rightRows) {

    WindowFn leftWindow = leftRows.getWindowingStrategy().getWindowFn();
    WindowFn rightWindow = rightRows.getWindowingStrategy().getWindowFn();
    if (!leftWindow.isCompatible(rightWindow)) {
      throw new IllegalArgumentException(
          "inputs of "
              + opType
              + " have different window strategy: "
              + leftWindow
              + " VS "
              + rightWindow);
    }

    // TODO: We may want to preaggregate the counts first using Group instead of calling CoGroup and
    // measuring the
    // iterable size. If on average there are duplicates in the input, this will be faster.
    final String lhsTag = "lhs";
    final String rhsTag = "rhs";
    PCollection<Row> joined =
        PCollectionTuple.of(lhsTag, leftRows, rhsTag, rightRows)
            .apply("CoGroup", CoGroup.join(By.fieldNames("*")));
    return joined
        .apply(
            "FilterResults",
            ParDo.of(
                new BeamSetOperatorsTransforms.SetOperatorFilteringDoFn(
                    lhsTag, rhsTag, opType, all)))
        .setRowSchema(joined.getSchema().getField("key").getType().getRowSchema());
  }

  private static PCollection<Row> coerce(PCollection<Row> input, Schema targetSchema) {
    if (input.getSchema().equals(targetSchema)) {
      return input;
    }
    return input
        .apply(
            "CoerceSchema",
            MapElements.into(TypeDescriptors.rows()).via(row -> coerceRow(row, targetSchema)))
        .setRowSchema(targetSchema);
  }

  private static Row coerceRow(Row row, Schema targetSchema) {
    Row.Builder builder = Row.withSchema(targetSchema);
    for (int i = 0; i < targetSchema.getFieldCount(); i++) {
      Schema.Field targetField = targetSchema.getField(i);
      Object value = row.getValue(i);
      builder.addValue(
          coerceValue(value, row.getSchema().getField(i).getType(), targetField.getType()));
    }
    return builder.build();
  }

  private static Object coerceValue(Object value, FieldType inputType, FieldType targetType) {
    if (value == null) {
      return null;
    }
    if (inputType.equals(targetType)) {
      return value;
    }
    if (inputType.getTypeName() == TypeName.ARRAY && targetType.getTypeName() == TypeName.ARRAY) {
      List<?> list = (List<?>) value;
      if (list.isEmpty()) {
        return Collections.emptyList();
      }
      List<Object> coercedList = new ArrayList<>(list.size());
      for (Object element : list) {
        coercedList.add(
            coerceValue(
                element,
                inputType.getCollectionElementType(),
                targetType.getCollectionElementType()));
      }
      return coercedList;
    }
    if (inputType.getTypeName() == TypeName.ROW && targetType.getTypeName() == TypeName.ROW) {
      return coerceRow((Row) value, targetType.getRowSchema());
    }
    return value;
  }
}
