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

package org.apache.beam.dsls.sql.rel;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.beam.dsls.sql.exception.BeamSqlUnsupportedException;
import org.apache.beam.dsls.sql.planner.BeamSQLRelUtils;
import org.apache.beam.dsls.sql.schema.BeamSQLRecordType;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowCoder;
import org.apache.beam.dsls.sql.schema.UnsupportedDataTypeException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
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
 *     select * from t order by id desc limit 10, 5;
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
      throw new BeamSqlUnsupportedException("ORDER BY without a LIMIT is not supported!");
    }

    RexLiteral fetchLiteral = (RexLiteral) fetch;
    count = ((BigDecimal) fetchLiteral.getValue()).intValue();

    if (offset != null) {
      RexLiteral offsetLiteral = (RexLiteral) offset;
      startIndex = ((BigDecimal) offsetLiteral.getValue()).intValue();
    }
  }

  @Override public PCollection<BeamSQLRow> buildBeamPipeline(PCollectionTuple inputPCollections)
      throws Exception {
    RelNode input = getInput();
    PCollection<BeamSQLRow> upstream = BeamSQLRelUtils.getBeamRelInput(input)
        .buildBeamPipeline(inputPCollections);
    Type windowType = upstream.getWindowingStrategy().getWindowFn()
        .getWindowTypeDescriptor().getType();
    if (!windowType.equals(GlobalWindow.class)) {
      throw new BeamSqlUnsupportedException(
          "`ORDER BY` is only supported for GlobalWindow, actual window: " + windowType);
    }

    BeamSQLRowComparator comparator = new BeamSQLRowComparator(fieldIndices, orientation,
        nullsFirst);
    // first find the top (offset + count)
    PCollection<List<BeamSQLRow>> rawStream =
        upstream.apply("extractTopOffsetAndFetch",
            Top.of(startIndex + count, comparator).withoutDefaults())
        .setCoder(ListCoder.<BeamSQLRow>of(upstream.getCoder()));

    // strip the `leading offset`
    if (startIndex > 0) {
      rawStream = rawStream.apply("stripLeadingOffset", ParDo.of(
          new SubListFn<BeamSQLRow>(startIndex, startIndex + count)))
          .setCoder(ListCoder.<BeamSQLRow>of(upstream.getCoder()));
    }

    PCollection<BeamSQLRow> orderedStream = rawStream.apply(
        "flatten", Flatten.<BeamSQLRow>iterables());
    orderedStream.setCoder(new BeamSqlRowCoder(BeamSQLRecordType.from(getRowType())));

    return orderedStream;
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

  private static class BeamSQLRowComparator implements Comparator<BeamSQLRow>, Serializable {
    private List<Integer> fieldsIndices;
    private List<Boolean> orientation;
    private List<Boolean> nullsFirst;

    public BeamSQLRowComparator(List<Integer> fieldsIndices,
        List<Boolean> orientation,
        List<Boolean> nullsFirst) {
      this.fieldsIndices = fieldsIndices;
      this.orientation = orientation;
      this.nullsFirst = nullsFirst;
    }

    @Override public int compare(BeamSQLRow row1, BeamSQLRow row2) {
      for (int i = 0; i < fieldsIndices.size(); i++) {
        int fieldIndex = fieldsIndices.get(i);
        int fieldRet = 0;
        SqlTypeName fieldType = row1.getDataType().getFieldsType().get(fieldIndex);
        // whether NULL should be ordered first or last(compared to non-null values) depends on
        // what user specified in SQL(NULLS FIRST/NULLS LAST)
        if (row1.isNull(fieldIndex) && row2.isNull(fieldIndex)) {
          continue;
        } else if (row1.isNull(fieldIndex) && !row2.isNull(fieldIndex)) {
          fieldRet = -1 * (nullsFirst.get(i) ? -1 : 1);
        } else if (!row1.isNull(fieldIndex) && row2.isNull(fieldIndex)) {
          fieldRet = 1 * (nullsFirst.get(i) ? -1 : 1);
        } else {
          switch (fieldType) {
            case TINYINT:
              fieldRet = numberCompare(row1.getByte(fieldIndex), row2.getByte(fieldIndex));
              break;
            case SMALLINT:
              fieldRet = numberCompare(row1.getShort(fieldIndex), row2.getShort(fieldIndex));
              break;
            case INTEGER:
              fieldRet = numberCompare(row1.getInteger(fieldIndex), row2.getInteger(fieldIndex));
              break;
            case BIGINT:
              fieldRet = numberCompare(row1.getLong(fieldIndex), row2.getLong(fieldIndex));
              break;
            case FLOAT:
              fieldRet = numberCompare(row1.getFloat(fieldIndex), row2.getFloat(fieldIndex));
              break;
            case DOUBLE:
              fieldRet = numberCompare(row1.getDouble(fieldIndex), row2.getDouble(fieldIndex));
              break;
            case VARCHAR:
              fieldRet = row1.getString(fieldIndex).compareTo(row2.getString(fieldIndex));
              break;
            case DATE:
              fieldRet = row1.getDate(fieldIndex).compareTo(row2.getDate(fieldIndex));
              break;
            default:
              throw new UnsupportedDataTypeException(fieldType);
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

  public static <T extends Number & Comparable> int numberCompare(T a, T b) {
    return a.compareTo(b);
  }
}
