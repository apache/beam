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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.beam.dsls.sql.exception.BeamSqlUnsupportedException;
import org.apache.beam.dsls.sql.planner.BeamPipelineCreator;
import org.apache.beam.dsls.sql.planner.BeamSQLRelUtils;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.dsls.sql.schema.UnsupportedDataTypeException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.PCollection;
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
 * <p>
 * Since :
 *   <a href="https://lists.apache.org/thread.html/bc0e65a3bb653b8fd0db96bcd4c9da5af71a
 *   71af5a5639a472167808@1464278191@%3Cdev.beam.apache.org%3E">
 *   Beam does not fully supported global sort
 *   </a>
 *
 *   we are using {@link Top} to implement the {@code Sort} algebra. The following types of
 *   ORDER BY are supported:
 *
 *   <pre>{@code
 *     select * from t order by id desc limit 10;
 *     select * from t order by id desc limit 10, 5;
 *   }</pre>
 *
 *   but Order BY without a limit is NOT supported:
 *
 *   <pre>{@code
 *     select * from t order by id desc
 *   }</pre>
 * </p>
 */
public class BeamSortRel extends Sort implements BeamRelNode {
  private List<Integer> fieldIndices = new ArrayList<>();
  private List<Boolean> orientation = new ArrayList<>();

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

  @Override public Pipeline buildBeamPipeline(BeamPipelineCreator planCreator) throws Exception {
    RelNode input = getInput();
    BeamSQLRelUtils.getBeamRelInput(input).buildBeamPipeline(planCreator);

    String stageName = BeamSQLRelUtils.getStageName(this);
    PCollection<BeamSQLRow> upstream = planCreator.popUpstream();

    BeamSQLRowComparator comparator = new BeamSQLRowComparator(fieldIndices, orientation);
    // first find the top (offset + count)
    PCollection<BeamSQLRow> orderedStream =
        upstream.apply("extractTopOffset_plus_Fetch",
            Top.of(startIndex + count, comparator))
        .apply("extractTopOffset_plus_Fetch__Flatten", Flatten.<BeamSQLRow>iterables());

    if (startIndex > 0) {
      // strip the leading `offset`
      orderedStream = orderedStream.apply("stripLeadingOffset",
          Top.of(count, new NegativeComparator(comparator)))
          .apply("stripLeadingOffset__Flatten", Flatten.<BeamSQLRow>iterables());
      // reverse it
      orderedStream = orderedStream.apply("reverseTailFetch",
          Top.of(count, comparator))
          .apply("reverseTailFetch__Flatten", Flatten.<BeamSQLRow>iterables());
    }

    planCreator.pushUpstream(orderedStream);

    return planCreator.getPipeline();
  }

  @Override public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation,
      RexNode offset, RexNode fetch) {
    return new BeamSortRel(getCluster(), traitSet, newInput, newCollation, offset, fetch);
  }

  private static class NegativeComparator implements Comparator<BeamSQLRow>, Serializable {
    private BeamSQLRowComparator delegate;
    public NegativeComparator(BeamSQLRowComparator delegate) {
      this.delegate = delegate;
    }

    @Override public int compare(BeamSQLRow row1, BeamSQLRow row2) {
      return delegate.compare(row2, row1);
    }
  }

  private static class BeamSQLRowComparator implements Comparator<BeamSQLRow>, Serializable {
    private List<Integer> fieldsIndices;
    private List<Boolean> orientation;
    public BeamSQLRowComparator(List<Integer> fieldsIndices, List<Boolean> orientation) {
      this.fieldsIndices = fieldsIndices;
      this.orientation = orientation;
    }

    @Override public int compare(BeamSQLRow row1, BeamSQLRow row2) {
      for (int i = 0; i < fieldsIndices.size(); i++) {
        int fieldIndex = fieldsIndices.get(i);
        int fieldRet = 0;
        SqlTypeName fieldType = row1.getDataType().getFieldsType().get(fieldIndex);
        if (row1.isNull(fieldIndex) && row2.isNull(fieldIndex)) {
          continue;
        } else if (row1.isNull(fieldIndex) && !row2.isNull(fieldIndex)) {
          fieldRet = -1;
        } else if (!row1.isNull(fieldIndex) && row2.isNull(fieldIndex)) {
          fieldRet = 1;
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
