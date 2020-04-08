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

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSetOperatorRelBase;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

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
  public static class SetOperatorFilteringDoFn extends DoFn<Row, Row> {
    private final String leftTag;
    private final String rightTag;
    private final BeamSetOperatorRelBase.OpType opType;
    // ALL?
    private final boolean all;

    public SetOperatorFilteringDoFn(
        String leftTag, String rightTag, BeamSetOperatorRelBase.OpType opType, boolean all) {
      this.leftTag = leftTag;
      this.rightTag = rightTag;
      this.opType = opType;
      this.all = all;
    }

    @ProcessElement
    public void processElement(@Element Row element, OutputReceiver<Row> o) {
      Row key = element.getRow("key");
      long numLeftRows = 0;
      long numRightRows = 0;
      if (!Iterables.isEmpty(element.<Row>getIterable(leftTag))) {
        numLeftRows = Iterables.size(element.<Row>getIterable(leftTag));
      }
      if (!Iterables.isEmpty(element.<Row>getIterable(rightTag))) {
        numRightRows = Iterables.size(element.<Row>getIterable(rightTag));
      }

      switch (opType) {
        case UNION:
          if (all) {
            for (int i = 0; i < numLeftRows + numRightRows; i++) {
              o.output(key);
            }
          } else {
            // only output the key
            o.output(key);
          }
          break;
        case INTERSECT:
          if (numLeftRows > 0 && numRightRows > 0) {
            if (all) {
              // Say for Row R, there are m instances on left and n instances on right,
              // INTERSECT ALL outputs MIN(m, n) instances of R.
              for (int i = 0; i < Math.min(numLeftRows, numRightRows); i++) {
                o.output(key);
              }
            } else {
              o.output(key);
            }
          }
          break;
        case MINUS:
          // Say for Row R, there are m instances on left and n instances on right:
          // - EXCEPT ALL outputs MAX(m - n, 0) instances of R.
          // - EXCEPT [DISTINCT] outputs a single instance of R if m > 0 and n == 0, else
          //   they output 0 instances.
          if (numLeftRows > 0 && numRightRows == 0) {
            if (all) {
              // output all
              for (int i = 0; i < numLeftRows; i++) {
                o.output(key);
              }
            } else {
              // only output one
              o.output(key);
            }
          } else if (numLeftRows > 0 && numRightRows > 0) {
            long outputCount = numLeftRows - numRightRows;
            if (outputCount > 0) {
              if (all) {
                while (outputCount > 0) {
                  outputCount--;
                  o.output(key);
                }
              }
              // Dont output any in DISTINCT (if (!all)) case
            }
          }
      }
    }
  }
}
