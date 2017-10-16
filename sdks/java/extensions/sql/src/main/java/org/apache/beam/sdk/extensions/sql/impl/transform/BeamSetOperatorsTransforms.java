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
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Collections of {@code PTransform} and {@code DoFn} used to perform Set operations.
 */
public abstract class BeamSetOperatorsTransforms {
  /**
   * Transform a {@code BeamSqlRow} to a {@code KV<BeamSqlRow, BeamSqlRow>}.
   */
  public static class BeamSqlRow2KvFn extends
      SimpleFunction<BeamRecord, KV<BeamRecord, BeamRecord>> {
    @Override public KV<BeamRecord, BeamRecord> apply(BeamRecord input) {
      return KV.of(input, input);
    }
  }

  /**
   * Filter function used for Set operators.
   */
  public static class SetOperatorFilteringDoFn extends
      DoFn<KV<BeamRecord, CoGbkResult>, BeamRecord> {
    private TupleTag<BeamRecord> leftTag;
    private TupleTag<BeamRecord> rightTag;
    private BeamSetOperatorRelBase.OpType opType;
    // ALL?
    private boolean all;

    public SetOperatorFilteringDoFn(TupleTag<BeamRecord> leftTag, TupleTag<BeamRecord> rightTag,
        BeamSetOperatorRelBase.OpType opType, boolean all) {
      this.leftTag = leftTag;
      this.rightTag = rightTag;
      this.opType = opType;
      this.all = all;
    }

    @ProcessElement public void processElement(ProcessContext ctx) {
      CoGbkResult coGbkResult = ctx.element().getValue();
      Iterable<BeamRecord> leftRows = coGbkResult.getAll(leftTag);
      Iterable<BeamRecord> rightRows = coGbkResult.getAll(rightTag);
      switch (opType) {
        case UNION:
          if (all) {
            // output both left & right
            Iterator<BeamRecord> iter = leftRows.iterator();
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
              for (BeamRecord leftRow : leftRows) {
                ctx.output(leftRow);
              }
            } else {
              ctx.output(ctx.element().getKey());
            }
          }
          break;
        case MINUS:
          if (leftRows.iterator().hasNext() && !rightRows.iterator().hasNext()) {
            Iterator<BeamRecord> iter = leftRows.iterator();
            if (all) {
              // output all
              while (iter.hasNext()) {
                ctx.output(iter.next());
              }
            } else {
              // only output one
              ctx.output(iter.next());
            }
          }
      }
    }
  }
}
