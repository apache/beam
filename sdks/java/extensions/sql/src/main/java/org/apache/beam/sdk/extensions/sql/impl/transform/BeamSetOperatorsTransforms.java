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

import java.util.Iterator;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSetOperatorRelBase;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.Iterators;

/** Collections of {@code PTransform} and {@code DoFn} used to perform Set operations. */
public abstract class BeamSetOperatorsTransforms {
  /** Transform a {@code BeamSqlRow} to a {@code KV<BeamSqlRow, BeamSqlRow>}. */
  public static class BeamSqlRow2KvFn extends SimpleFunction<Row, KV<Row, Row>> {
    @Override
    public KV<Row, Row> apply(Row input) {
      return KV.of(input, input);
    }
  }

  /** Filter function used for Set operators. */
  public static class SetOperatorFilteringDoFn extends DoFn<KV<Row, CoGbkResult>, Row> {
    private TupleTag<Row> leftTag;
    private TupleTag<Row> rightTag;
    private BeamSetOperatorRelBase.OpType opType;
    // ALL?
    private boolean all;

    public SetOperatorFilteringDoFn(
        TupleTag<Row> leftTag,
        TupleTag<Row> rightTag,
        BeamSetOperatorRelBase.OpType opType,
        boolean all) {
      this.leftTag = leftTag;
      this.rightTag = rightTag;
      this.opType = opType;
      this.all = all;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      CoGbkResult coGbkResult = ctx.element().getValue();
      Iterable<Row> leftRows = coGbkResult.getAll(leftTag);
      Iterable<Row> rightRows = coGbkResult.getAll(rightTag);
      switch (opType) {
        case UNION:
          if (all) {
            // output both left & right
            Iterator<Row> iter = leftRows.iterator();
            while (iter.hasNext()) {
              ctx.output(iter.next());
            }
            iter = rightRows.iterator();
            while (iter.hasNext()) {
              ctx.output(iter.next());
            }
          } else {
            // only output the key
            ctx.output(ctx.element().getKey());
          }
          break;
        case INTERSECT:
          if (leftRows.iterator().hasNext() && rightRows.iterator().hasNext()) {
            if (all) {
              int leftCount = Iterators.size(leftRows.iterator());
              int rightCount = Iterators.size(rightRows.iterator());

              // Say for Row R, there are m instances on left and n instances on right,
              // INTERSECT ALL outputs MIN(m, n) instances of R.
              Iterator<Row> iter =
                  (leftCount <= rightCount) ? leftRows.iterator() : rightRows.iterator();
              while (iter.hasNext()) {
                ctx.output(iter.next());
              }
            } else {
              ctx.output(ctx.element().getKey());
            }
          }
          break;
        case MINUS:
          // Say for Row R, there are m instances on left and n instances on right:
          // - EXCEPT ALL outputs MAX(m - n, 0) instances of R.
          // - EXCEPT [DISTINCT] outputs a single instance of R if m > 0 and n == 0, else
          //   they output 0 instances.
          if (leftRows.iterator().hasNext() && !rightRows.iterator().hasNext()) {
            Iterator<Row> iter = leftRows.iterator();
            if (all) {
              // output all
              while (iter.hasNext()) {
                ctx.output(iter.next());
              }
            } else {
              // only output one
              ctx.output(iter.next());
            }
          } else if (leftRows.iterator().hasNext() && rightRows.iterator().hasNext()) {
            int leftCount = Iterators.size(leftRows.iterator());
            int rightCount = Iterators.size(rightRows.iterator());

            int outputCount = leftCount - rightCount;
            if (outputCount > 0) {
              if (all) {
                while (outputCount > 0) {
                  outputCount--;
                  ctx.output(ctx.element().getKey());
                }
              }
              // Dont output any in DISTINCT (if (!all)) case
            }
          }
      }
    }
  }
}
