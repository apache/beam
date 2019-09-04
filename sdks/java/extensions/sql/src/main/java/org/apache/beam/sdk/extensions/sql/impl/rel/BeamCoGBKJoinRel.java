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

import static org.apache.beam.sdk.values.PCollection.IsBounded.UNBOUNDED;
import static org.joda.time.Duration.ZERO;

import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamJoinTransforms;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IncompatibleWindowException;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

public class BeamCoGBKJoinRel extends BeamJoinRel {

  public BeamCoGBKJoinRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traitSet, left, right, condition, variablesSet, joinType);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new StandardJoin();
  }

  private class StandardJoin extends PTransform<PCollectionList<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollectionList<Row> pinput) {
      Schema leftSchema = CalciteUtils.toSchema(left.getRowType());
      Schema rightSchema = CalciteUtils.toSchema(right.getRowType());

      PCollectionList<KV<Row, Row>> keyedInputs = pinput.apply(new ExtractJoinKeys());

      PCollection<KV<Row, Row>> extractedLeftRows = keyedInputs.get(0);
      PCollection<KV<Row, Row>> extractedRightRows = keyedInputs.get(1);

      WindowFn leftWinFn = extractedLeftRows.getWindowingStrategy().getWindowFn();
      WindowFn rightWinFn = extractedRightRows.getWindowingStrategy().getWindowFn();

      try {
        leftWinFn.verifyCompatibility(rightWinFn);
      } catch (IncompatibleWindowException e) {
        throw new IllegalArgumentException(
            "WindowFns must match for a bounded-vs-bounded/unbounded-vs-unbounded join.", e);
      }

      verifySupportedTrigger(extractedLeftRows);
      verifySupportedTrigger(extractedRightRows);

      return standardJoin(extractedLeftRows, extractedRightRows, leftSchema, rightSchema);
    }
  }

  private <T> void verifySupportedTrigger(PCollection<T> pCollection) {
    WindowingStrategy windowingStrategy = pCollection.getWindowingStrategy();

    if (UNBOUNDED.equals(pCollection.isBounded()) && !triggersOncePerWindow(windowingStrategy)) {
      throw new UnsupportedOperationException(
          "Joining unbounded PCollections is currently only supported for "
              + "non-global windows with triggers that are known to produce output once per window,"
              + "such as the default trigger with zero allowed lateness. "
              + "In these cases Beam can guarantee it joins all input elements once per window. "
              + windowingStrategy
              + " is not supported");
    }
  }

  private boolean triggersOncePerWindow(WindowingStrategy windowingStrategy) {
    Trigger trigger = windowingStrategy.getTrigger();

    return !(windowingStrategy.getWindowFn() instanceof GlobalWindows)
        && trigger instanceof DefaultTrigger
        && ZERO.equals(windowingStrategy.getAllowedLateness());
  }

  private PCollection<Row> standardJoin(
      PCollection<KV<Row, Row>> extractedLeftRows,
      PCollection<KV<Row, Row>> extractedRightRows,
      Schema leftSchema,
      Schema rightSchema) {
    PCollection<KV<Row, KV<Row, Row>>> joinedRows = null;

    switch (joinType) {
      case LEFT:
        {
          Schema rigthNullSchema = buildNullSchema(rightSchema);
          Row rightNullRow = Row.nullRow(rigthNullSchema);

          extractedRightRows = setValueCoder(extractedRightRows, SchemaCoder.of(rigthNullSchema));

          joinedRows =
              org.apache.beam.sdk.extensions.joinlibrary.Join.leftOuterJoin(
                  extractedLeftRows, extractedRightRows, rightNullRow);

          break;
        }
      case RIGHT:
        {
          Schema leftNullSchema = buildNullSchema(leftSchema);
          Row leftNullRow = Row.nullRow(leftNullSchema);

          extractedLeftRows = setValueCoder(extractedLeftRows, SchemaCoder.of(leftNullSchema));

          joinedRows =
              org.apache.beam.sdk.extensions.joinlibrary.Join.rightOuterJoin(
                  extractedLeftRows, extractedRightRows, leftNullRow);
          break;
        }
      case FULL:
        {
          Schema leftNullSchema = buildNullSchema(leftSchema);
          Schema rightNullSchema = buildNullSchema(rightSchema);

          Row leftNullRow = Row.nullRow(leftNullSchema);
          Row rightNullRow = Row.nullRow(rightNullSchema);

          extractedLeftRows = setValueCoder(extractedLeftRows, SchemaCoder.of(leftNullSchema));
          extractedRightRows = setValueCoder(extractedRightRows, SchemaCoder.of(rightNullSchema));

          joinedRows =
              org.apache.beam.sdk.extensions.joinlibrary.Join.fullOuterJoin(
                  extractedLeftRows, extractedRightRows, leftNullRow, rightNullRow);
          break;
        }
      case INNER:
      default:
        joinedRows =
            org.apache.beam.sdk.extensions.joinlibrary.Join.innerJoin(
                extractedLeftRows, extractedRightRows);
        break;
    }

    Schema schema = CalciteUtils.toSchema(getRowType());
    return joinedRows
        .apply(
            "JoinParts2WholeRow",
            MapElements.via(new BeamJoinTransforms.JoinParts2WholeRow(schema)))
        .setRowSchema(schema);
  }

  @Override
  public Join copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new BeamCoGBKJoinRel(
        getCluster(), traitSet, left, right, conditionExpr, variablesSet, joinType);
  }
}
