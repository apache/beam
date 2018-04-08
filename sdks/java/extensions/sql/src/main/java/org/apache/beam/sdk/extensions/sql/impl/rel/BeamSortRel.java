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
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamRelNode} to replace a {@code Sort} node.
 *
 * <p>Since Beam does not fully supported global sort we are using {@link Top} to implement
 * the {@code Sort} algebra. The following types of ORDER BY are supported:

 * <pre>{@code
 *     select * from t order by id desc limit 10;
 *     select * from t order by id desc limit 10 offset 5;
 * }</pre>
 *
 * <p>but Order BY without a limit is NOT supported:
 *
 * <pre>{@code
 *   select * from t order by id desc
 * }</pre>
 *
 * <h3>Constraints</h3>
 * <ul>
 *   <li>Due to the constraints of {@link Top}, the result of a `ORDER BY LIMIT`
 *   must fit into the memory of a single machine.</li>
 *   <li>Since `WINDOW`(HOP, TUMBLE, SESSION etc) is always associated with `GroupBy`,
 *   it does not make much sense to use `ORDER BY` with `WINDOW`.
 *   </li>
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
  public PTransform<PCollectionTuple, PCollection<Row>> toPTransform() {
    return new Transform();
  }

  private class Transform extends PTransform<PCollectionTuple, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionTuple inputPCollections) {
      RelNode input = getInput();
      PCollection<Row> upstream =
          inputPCollections.apply(BeamSqlRelUtils.getBeamRelInput(input).toPTransform());
      Type windowType =
          upstream.getWindowingStrategy().getWindowFn().getWindowTypeDescriptor().getType();
      if (!windowType.equals(GlobalWindow.class)) {
        throw new UnsupportedOperationException(
            "`ORDER BY` is only supported for GlobalWindow, actual window: " + windowType);
      }

      BeamSqlRowComparator comparator =
          new BeamSqlRowComparator(fieldIndices, orientation, nullsFirst);
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
                    "stripLeadingOffset", ParDo.of(new SubListFn<>(startIndex, startIndex + count)))
                .setCoder(ListCoder.of(upstream.getCoder()));
      }

      PCollection<Row> orderedStream = rawStream.apply("flatten", Flatten.iterables());
      orderedStream.setCoder(CalciteUtils.toBeamSchema(getRowType()).getRowCoder());

      return orderedStream;
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

  @Override public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation,
      RexNode offset, RexNode fetch) {
    return new BeamSortRel(getCluster(), traitSet, newInput, newCollation, offset, fetch);
  }

  private static class BeamSqlRowComparator implements Comparator<Row>, Serializable {
    private List<Integer> fieldsIndices;
    private List<Boolean> orientation;
    private List<Boolean> nullsFirst;

    public BeamSqlRowComparator(List<Integer> fieldsIndices,
        List<Boolean> orientation,
        List<Boolean> nullsFirst) {
      this.fieldsIndices = fieldsIndices;
      this.orientation = orientation;
      this.nullsFirst = nullsFirst;
    }

    @Override public int compare(Row row1, Row row2) {
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
              Comparable v1 = (Comparable) row1.getValue(fieldIndex);
              Comparable v2 = (Comparable) row2.getValue(fieldIndex);
              fieldRet = v1.compareTo(v2);
              break;
            default:
              throw new UnsupportedOperationException(
                  "Data type: " + sqlTypeName + " not supported yet!");
          }
        }

        fieldRet *= (orientation.get(i) ? -1 : 1);
        if (fieldRet != 0) {
          return fieldRet;
        }
      }
      return 0;
    }
  }

}
