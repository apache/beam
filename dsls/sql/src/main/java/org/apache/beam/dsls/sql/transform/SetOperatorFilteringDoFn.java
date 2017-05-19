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

import java.io.Serializable;
import java.util.Iterator;

import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Filter function used for Set operators.
 */
public class SetOperatorFilteringDoFn extends DoFn<KV<BeamSQLRow, CoGbkResult>, BeamSQLRow> {

  /**
   * Set operator type.
   */
  public enum OpType implements Serializable {
    INTERSECT,
    MINUS
  }

  private TupleTag<BeamSQLRow> leftTag;
  private TupleTag<BeamSQLRow> rightTag;
  private OpType opType;
  // ALL?
  private boolean all;

  public SetOperatorFilteringDoFn(TupleTag<BeamSQLRow> leftTag, TupleTag<BeamSQLRow> rightTag,
      OpType opType, boolean all) {
    this.leftTag = leftTag;
    this.rightTag = rightTag;
    this.opType = opType;
    this.all = all;
  }

  @ProcessElement public void processElement(ProcessContext ctx) {
    CoGbkResult coGbkResult = ctx.element().getValue();
    Iterable<BeamSQLRow> leftRows = coGbkResult.getAll(leftTag);
    Iterable<BeamSQLRow> rightRows = coGbkResult.getAll(rightTag);
    switch (opType) {
      case INTERSECT:
        if (leftRows.iterator().hasNext() && rightRows.iterator().hasNext()) {
          if (all) {
            Iterator<BeamSQLRow> iter = leftRows.iterator();
            while (iter.hasNext()) {
              ctx.output(iter.next());
            }
          } else {
            ctx.output(ctx.element().getKey());
          }
        }
        break;
      case MINUS:
        if (leftRows.iterator().hasNext() && !rightRows.iterator().hasNext()) {
          Iterator<BeamSQLRow> iter = leftRows.iterator();
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
